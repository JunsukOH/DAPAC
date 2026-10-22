#!/usr/bin/env python3
"""Hourly, resumable four-policy HTTPS caching comparison. No third-party packages."""
import collections
from datetime import datetime, timezone
import fcntl
import json
import logging
import re
from pathlib import Path
import signal
import sqlite3
import time
import uuid

from poc import Cache, Fetcher, Predictor, parser, validate_receipt
from cache_transport import Origin, Clients

PLAYBACK_MODEL = 'per-user-video-resume-v2'
ALGORITHM_REVISION = 'ack-committed-v3'
DB_NAME = 'experiments-ack-v3.sqlite3'

POLICIES = ('none', 'lru', 'popularity', 'ontime')
LABELS = dict(zip(POLICIES, ('No caching', 'Demand-only LRU', 'Popularity-based prefetching', 'On-time caching')))


def utc():
    return datetime.now(timezone.utc).isoformat()


class NoCache(Cache):
    def put(self, key, data, digest, prefetch):
        return {'data': data, 'sha256': digest, 'prefetch': False, 'used': True}


class Latencies:
    """Bounded histogram: nearest millisecond, overflow bucket at 24 hours."""
    def __init__(self):
        self.bins = collections.Counter()
        self.count = 0
        self.total = self.maximum = 0.0

    def add(self, ms):
        self.count += 1
        self.total += ms
        self.maximum = max(self.maximum, ms)
        self.bins[min(86400000, max(0, round(ms)))] += 1

    def percentile(self, p):
        threshold = max(1, __import__('math').ceil(self.count*p))
        total = 0
        for value, count in sorted(self.bins.items()):
            total += count
            if total >= threshold:
                return value
        return None

    def metrics(self, prefix):
        return {prefix+'_mean_ms': round(self.total/self.count, 3) if self.count else None,
                prefix+'_p95_ms': self.percentile(.95),
                prefix+'_p99_ms': self.percentile(.99),
                prefix+'_max_ms': round(self.maximum, 3) if self.count else None}


def experiment_paths(state, experiment_id='baseline'):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}', experiment_id):
        raise ValueError('Invalid experiment identifier')
    suffix = '' if experiment_id == 'baseline' else '-'+experiment_id
    return state/('experiments-ack-v3'+suffix+'.sqlite3'), state/('experiment-hourly'+suffix+'.csv')


class Records:
    def __init__(self, state, experiment_id='baseline'):
        self.path, self.csv_path = experiment_paths(state, experiment_id)
        self.db = sqlite3.connect(self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS phases (
            id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL, policy TEXT NOT NULL,
            started_utc TEXT NOT NULL, ended_utc TEXT, status TEXT NOT NULL,
            result_json TEXT, configuration_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS samples (
            phase_id TEXT NOT NULL, at_utc TEXT NOT NULL, metrics_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS samples_phase ON samples(phase_id);
        ''')
        # A killed process leaves a durable invalid record, never a completed hour.
        self.db.execute("UPDATE phases SET status='interrupted', ended_utc=? WHERE status='running'", (utc(),))
        self.db.commit()

    def next_ordinal(self):
        return self.db.execute("SELECT COALESCE(MAX(ordinal),-1)+1 FROM phases WHERE status='complete'").fetchone()[0]

    def begin(self, ordinal, policy, configuration):
        existing = self.db.execute('SELECT configuration_json FROM phases LIMIT 1').fetchone()
        if existing and json.loads(existing[0]).get('prefetch_margin') != configuration.get('prefetch_margin'):
            raise ValueError('Safety Margin differs from this experiment database; choose a new experiment-id')
        ident = uuid.uuid4().hex
        self.db.execute('INSERT INTO phases VALUES (?,?,?,?,NULL,?,NULL,?)',
                        (ident, ordinal, policy, utc(), 'running', json.dumps(configuration)))
        self.db.commit()
        return ident

    def save(self, ident, metrics, status=None):
        payload = json.dumps(metrics, ensure_ascii=False)
        self.db.execute('INSERT INTO samples VALUES (?,?,?)', (ident, utc(), payload))
        if status:
            self.db.execute('UPDATE phases SET ended_utc=?,status=?,result_json=? WHERE id=?',
                            (utc(), status, payload, ident))
        self.db.commit()


def policy_prediction(policy, req, predictor, popularity, began):
    predicted, confidence, interval = predictor.observe(req)
    popularity[req['stream']] += 1
    if policy == 'ontime' and confidence >= .55:
        return predicted, began+interval
    if policy == 'popularity':
        # Top-1 globally popular stream, next sequence. No future trace is consulted.
        popular = max(popularity, key=lambda s: (popularity[s], -s))
        return predictor.next_key(req['user'], popular), began  # fetch immediately
    return None


def deliver_then_predict(deliver, req, item, policy, predictor, popularity, received):
    # A failed send, missing ACK, or invalid receipt must leave all predictor
    # cursors, timing/transition observations, and popularity counters unchanged.
    ack = validate_receipt(req, item, deliver(req, item))
    acknowledged = time.monotonic()
    prediction = policy_prediction(policy, req, predictor, popularity, received)
    return ack, prediction, acknowledged


def phase(a, policy, ordinal, ident, records):
    origin = clients = fetcher = None
    began = time.monotonic()
    cache = (NoCache if policy == 'none' else Cache)(a.cache_mib*1024**2, a.ttl)
    predictor = Predictor(a.streams)
    popularity = collections.Counter()
    latency, queued = Latencies(), Latencies()
    requests = hits = delivered = late_demands = prefetch_demands = 0
    stopped = False
    status, error = 'error', None
    last_sample = began
    observed = None
    logger = logging.getLogger('comparison-fetcher')
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    logger.propagate = False

    def metrics(final=False):
        now = time.monotonic()
        elapsed = max(.000001, now-began)
        with fetcher.cv if fetcher else __import__('contextlib').nullcontext():
            cache.expire()
            completed = fetcher.prefetches if fetcher else 0
            resident_unused = sum(i['prefetch'] and not i['used'] for i in cache.items.values())
            waste = cache.evicted_unused + (resident_unused if final else 0)
            row = dict(phase_id=ident, ordinal=ordinal, cycle=ordinal//4+1,
                       policy=policy, label=LABELS[policy], at_utc=utc(), playback_model=PLAYBACK_MODEL,
                       algorithm_revision=ALGORITHM_REVISION, prediction_commit="after_verified_ack",
                       experiment_id=a.experiment_id, prefetch_margin_seconds=a.prefetch_margin,
                       elapsed_seconds=round(elapsed, 3), target_seconds=a.phase_seconds,
                       requests=requests, cache_hits=hits, cache_misses=requests-hits,
                       hit_ratio=hits/requests if requests else None,
                       origin_bytes=fetcher.origin_bytes if fetcher else 0,
                       delivered_bytes=delivered, cache_bytes=cache.bytes,
                       prefetch_downloads=completed, prefetch_used=cache.used_prefetch,
                       prefetch_evicted_unused=cache.evicted_unused,
                       prefetch_resident_unused=resident_unused,
                       prefetch_wasted=waste,
                       prefetch_waste_ratio=waste/completed if completed else None,
                       prefetch_late_requests=late_demands, prefetch_demand_requests=prefetch_demands,
                       prefetch_late_ratio=late_demands/prefetch_demands if prefetch_demands else None,
                       late_request_ratio=late_demands/requests if requests else None,
                       prefetch_cancelled=fetcher.cancelled_prefetches if fetcher else 0,
                       pending=len(fetcher.jobs) if fetcher else 0,
                       origin_observed_source=observed, koren_source_verified=observed=='192.0.2.20',
                       transport='https', status=status if final else 'running', error=error,
                       final=final, users=a.users, seed=a.seed,
                       histogram_resolution_ms=1, histogram_overflow_ms=86400000)
        row['origin_payload_mbps'] = row['origin_bytes']*8/elapsed/1e6
        row['delivered_payload_mbps'] = delivered*8/elapsed/1e6
        row.update(latency.metrics('latency'))
        row.update(queued.metrics('scheduled_latency'))
        return row

    def save(final=False):
        row = metrics(final)
        records.save(ident, row, status if final else None)
        state = Path(a.state)
        temp = state/'experiment-current.tmp'
        temp.write_text(json.dumps(row, ensure_ascii=False, indent=2)+'\n')
        temp.replace(state/'experiment-current.json')
        if final:
            from experiment_report import export_hourly
            export_hourly(records.db, records.csv_path)
        print(json.dumps(row, ensure_ascii=False), flush=True)

    try:
        origin = Origin(a.origin_host,a.origin_port,a.origin_bind,a.origin_ca,a.origin_cert,a.origin_key)
        observed = origin.ready['source_address']
        if observed != '192.0.2.20' and not a.allow_campus_test:
            raise RuntimeError('Refusing non-KOREN origin source')
        if float(origin.ready['rate_limit_mbps']) != a.mbps:
            raise ValueError('Origin rate mismatch')
        clients = Clients(a.client_host,a.client_port,a.origin_bind,a.client_ca,a.client_cert,a.client_key)
        reset = clients.json('POST', '/client/reset', body=b'')
        if reset.get('playback_model') != PLAYBACK_MODEL:
            raise ValueError('Receiver requires per-video resume update before running this experiment')
        if not reset.get('reset') or not reset.get('benchmark') or any(
                reset[k] != getattr(a,k) for k in ('seed','users','streams','size','workload_mbps','switch_probability')):
            raise ValueError('Client workload mismatch; cannot compare different workloads')
        # Fixed stream/user order and RNG are reset for each policy. Slower policies
        # complete a shorter prefix of this same trace (closed-loop delivery).
        began = last_sample = time.monotonic()
        fetcher = Fetcher(a,cache,origin,logger)
        fetcher.start()
        save()
        while time.monotonic()-began < a.phase_seconds:
            req = clients.request()
            received = time.monotonic()
            key = req['stream'],req['seq']
            with fetcher.cv:
                job = fetcher.jobs.get(key)
                late = bool(job and job.get('speculative',job['prefetch']))
                if job and job['prefetch']:
                    job['speculative'] = True
            future, hit = fetcher.request(key,received,False)
            item = future.result(timeout=60)
            ack, prediction, acknowledged = deliver_then_predict(
                clients.deliver,req,item,policy,predictor,popularity,received)
            if prediction:
                predicted, due = prediction
                fetcher.request(predicted,due,True)
            elapsed_ms = (acknowledged-received)*1000
            latency.add(elapsed_ms)
            queued.add(elapsed_ms+req.get('queue_seconds',0)*1000)
            requests += 1
            hits += int(hit)
            late_demands += int(late and not hit)
            prefetch_demands += int(late or (hit and item['prefetch']))
            delivered += ack['bytes']
            if requests == 1 or time.monotonic()-last_sample >= a.sample_seconds:
                save()
                last_sample = time.monotonic()
        status = 'complete'
    except KeyboardInterrupt:
        status = 'interrupted'
        stopped = True
    except Exception as exc:
        error = '%s: %s' % (type(exc).__name__,exc)
        raise
    finally:
        if fetcher:
            # Drain the single active transfer so its bytes and boundary waste are
            # counted. Queued predictions are cancelled without downloading them.
            with fetcher.cv:
                fetcher.stopping = True
                for key in list(fetcher.jobs):
                    job = fetcher.jobs[key]
                    if not job['active']:
                        fetcher.jobs.pop(key)
                        job['future'].cancel()
                        fetcher.cancelled_prefetches += int(job['prefetch'])
                fetcher.cv.notify_all()
            fetcher.join(timeout=25)
            if fetcher.is_alive():
                status, error = 'error', 'Origin worker did not finish within 25 seconds'
        for connection in (clients,origin):
            if connection:
                connection.close()
        save(final=True)
    if status == 'error':
        raise RuntimeError(error)
    return stopped


def archive_previous_revision(state):
    """Run under run.lock, after the previous worker has finalized its own CSV."""
    current = state/'experiment-current.json'
    if not current.exists():
        return
    old = json.loads(current.read_text())
    if old.get('algorithm_revision') == ALGORITHM_REVISION:
        return
    tag = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    for name in ('experiment-current.json','experiment-hourly.csv','experiment-samples.csv'):
        path = state/name
        if path.exists():
            path.rename(state/(path.stem+'.legacy-preack-'+tag+path.suffix))


def main():
    p = parser()
    p.description = __doc__
    p.add_argument('--experiment-id',default='baseline',help='Separate DB and CSV for a controlled experiment')
    p.add_argument('--phase-seconds',type=int,default=3600)
    p.add_argument('--sample-seconds',type=int,default=60)
    p.add_argument('--max-phases',type=int,default=0,help='0 runs until stopped')
    a = p.parse_args()
    if a.mode != 'run' or a.transport != 'https' or a.phase_seconds < 1 or a.sample_seconds < 1 or a.max_phases < 0:
        p.error('Use run with HTTPS and positive phase/sample durations')
    if not (0 < a.mbps <= 10000 and 1 <= a.users <= 128 and 1 <= a.streams <= 64
            and 1 <= a.size <= 8*1024**2 and a.cache_mib*1024**2 >= a.size
            and a.ttl > 0 and 0 <= a.prefetch_margin <= 60):
        p.error('Invalid cache or workload configuration')
    if a.confidence != .55:
        p.error('Comparison baseline currently requires confidence=0.55')
    try:
        experiment_paths(Path(a.state), a.experiment_id)
    except ValueError as exc:
        p.error(str(exc))
    state = Path(a.state)
    state.mkdir(parents=True,exist_ok=True)
    with open(state/'run.lock','a+') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('Existing cache worker owns run.lock; stop only the cache service first')
        archive_previous_revision(state)
        records = Records(state,a.experiment_id)
        def stop(signum,frame):
            # A second termination must not interrupt final database writes.
            signal.signal(signal.SIGTERM,signal.SIG_IGN)
            signal.signal(signal.SIGINT,signal.SIG_IGN)
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM,stop)
        signal.signal(signal.SIGINT,stop)
        try:
            count = 0
            while not a.max_phases or count < a.max_phases:
                ordinal = records.next_ordinal()
                policy = POLICIES[ordinal%4]
                ident = records.begin(ordinal,policy,dict(vars(a),playback_model=PLAYBACK_MODEL,algorithm_revision=ALGORITHM_REVISION))
                if phase(a,policy,ordinal,ident,records):
                    break
                count += 1
        finally:
            records.db.close()


if __name__ == '__main__':
    main()

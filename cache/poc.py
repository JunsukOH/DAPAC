#!/usr/bin/env python3
"""Deadline-aware predictive streaming cache; synthetic users, real HTTPS payloads."""
import argparse
import collections
import concurrent.futures
import fcntl
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import re
import shlex
import signal
import subprocess
import sys
import threading
import time


def line(stream):
    raw = stream.readline(65537)
    if not raw or len(raw) > 65536:
        raise EOFError('SSH protocol closed or invalid header')
    return json.loads(raw)


def emit(stream, obj):
    stream.write(json.dumps(obj, separators=(',', ':')).encode() + b'\n')
    stream.flush()


def exact(stream, count):
    chunks = []
    while count:
        chunk = stream.read(min(count, 65536))
        if not chunk:
            raise EOFError('Incomplete segment')
        chunks.append(chunk)
        count -= len(chunk)
    return b''.join(chunks)


def content(stream, seq, size):
    return hashlib.shake_256(f'ontime-poc-v1:{stream}:{seq}:{size}'.encode()).digest(size)


def origin(a):
    inp, out = sys.stdin.buffer, sys.stdout.buffer
    connection = os.environ.get('SSH_CONNECTION', '').split()
    emit(out, {'kind': 'ready', 'role': 'origin',
               'source_address': connection[0] if connection else 'local'})
    next_send = time.monotonic()
    while True:
        try:
            req = line(inp)
        except EOFError:
            return
        stream, seq, size = int(req['stream']), int(req['seq']), int(req['size'])
        if not (0 <= stream < 64 and 0 <= seq < 10**12 and 1 <= size <= 8*1024**2):
            raise ValueError('Invalid segment request')
        data = content(stream, seq, size)
        emit(out, {'size': size, 'sha256': hashlib.sha256(data).hexdigest()})
        next_send = max(next_send, time.monotonic())
        for offset in range(0, size, 65536):
            chunk = data[offset:offset+65536]
            time.sleep(max(0, next_send-time.monotonic()))
            out.write(chunk)
            out.flush()
            next_send += len(chunk)/(a.mbps*125000)
        time.sleep(max(0, next_send-time.monotonic()))


def clients(a):
    rng = random.Random(a.seed)
    inp, out = sys.stdin.buffer, sys.stdout.buffer
    period = a.size*a.users/(a.workload_mbps*125000)
    began = time.monotonic()
    users = [{'due': began+i*period/a.users, 'stream': i % a.streams,
              'positions': [0]*a.streams, 'zone': i % 3} for i in range(a.users)]
    request_id = 0
    while True:
        user = min(range(a.users), key=lambda i: users[i]['due'])
        u = users[user]
        time.sleep(max(0, u['due']-time.monotonic()))
        now = time.monotonic()
        request_id += 1
        emit(out, {'kind': 'request', 'id': request_id, 'user': user,
                   'stream': u['stream'], 'seq': u['positions'][u['stream']], 'zone': u['zone'],
                   'playback_positions': list(u['positions']),
                   'when': now, 'period': period, 'size': a.size})
        try:
            response = line(inp)
        except EOFError:
            return
        if response['id'] != request_id or response['size'] != a.size:
            raise ValueError('Response mismatch')
        data = exact(inp, a.size)
        digest = hashlib.sha256(data).hexdigest()
        if digest != response['sha256']:
            raise ValueError('Client checksum mismatch')
        emit(out, {'kind': 'ack', 'id': request_id, 'bytes': len(data), 'sha256': digest})
        u['due'] = max(u['due']+period, time.monotonic())
        u['positions'][u['stream']] += 1
        if rng.random() < 0.10:
            u['zone'] = (u['zone']+1) % 3
        if rng.random() < a.switch_probability:
            u['stream'] = rng.randrange(a.streams)


class Predictor:
    """Online location-conditioned Markov model plus request interval EMA."""
    def __init__(self, streams):
        self.streams = streams
        self.counts = collections.defaultdict(collections.Counter)
        self.last = {}
        self.intervals = {}
        self.positions = {}

    def observe(self, req):
        """Observe an acknowledged request; callers must validate its receipt first."""
        user = req['user']
        old = self.last.get(user)
        if old:
            counts = self.counts[(old['zone'], old['stream'])]
            counts[req['stream']] += 1
            # Forget very old preferences to adapt and bound counts.
            if sum(counts.values()) > 1000:
                for key in counts:
                    counts[key] *= 0.5
            dt = req['when']-old['when']
            if dt > 0:
                self.intervals[user] = 0.2*dt+0.8*self.intervals.get(user, req['period'])
        self.last[user] = req
        counts = self.counts[(req['zone'], req['stream'])]
        scores = [counts.get(i, 0)+(8 if i == req['stream'] else 0.2)
                  for i in range(self.streams)]
        predicted = max(range(self.streams), key=lambda i: scores[i])
        positions = self.positions.setdefault(user, [0]*self.streams)
        if 'playback_positions' in req:
            supplied = req['playback_positions']
            if len(supplied) != self.streams or any(type(v) is not int or v < 0 for v in supplied):
                raise ValueError('Invalid per-video playback positions')
            positions[:] = supplied
        # Prediction follows a successful current delivery. Advance only this video.
        positions[req['stream']] = req['seq']+1
        return (predicted, positions[predicted]), scores[predicted]/sum(scores), self.intervals.get(user, req['period'])

    def next_key(self, user, stream):
        return stream, self.positions[user][stream]


class Cache:
    def __init__(self, capacity, ttl):
        self.capacity, self.ttl = capacity, ttl
        self.items = collections.OrderedDict()
        self.bytes = self.evicted_unused = self.used_prefetch = 0

    def get(self, key, demand=False):
        self.expire()
        item = self.items.get(key)
        if item is None:
            return None
        self.items.move_to_end(key)
        if demand and item['prefetch'] and not item['used']:
            self.used_prefetch += 1
        if demand:
            item['used'] = True
        return item

    def remove(self, key):
        item = self.items.pop(key)
        self.bytes -= len(item['data'])
        if item['prefetch'] and not item['used']:
            self.evicted_unused += 1

    def expire(self):
        now = time.monotonic()
        for key in list(self.items):
            if now-self.items[key]['created'] > self.ttl:
                self.remove(key)

    def put(self, key, data, digest, prefetch):
        if key in self.items:
            self.remove(key)
        while self.items and self.bytes+len(data) > self.capacity:
            self.remove(next(iter(self.items)))
        if len(data) > self.capacity:
            raise ValueError('Segment exceeds cache')
        item = {'data': data, 'sha256': digest, 'created': time.monotonic(),
                'prefetch': prefetch, 'used': False}
        self.items[key] = item
        self.bytes += len(data)
        return item


def connect(a, host, role):
    command = ['python3', '-u', a.remote_script, role, '--mbps', str(a.mbps),
               '--size', str(a.size), '--users', str(a.users), '--streams', str(a.streams),
               '--workload-mbps', str(a.workload_mbps), '--seed', str(a.seed),
               '--switch-probability', str(a.switch_probability)]
    bind = ['-b', a.origin_bind] if role == 'origin' and a.origin_bind else []
    return subprocess.Popen(['ssh', '-T', *bind, '-i', a.key, '-o', 'IdentitiesOnly=yes',
                             '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                             '-o', 'Compression=no', '-o', 'ConnectTimeout=8',
                             '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3',
                             host, shlex.join(command)], stdin=subprocess.PIPE, stdout=subprocess.PIPE)


class Fetcher(threading.Thread):
    def __init__(self, a, cache, proc, logger):
        super().__init__(daemon=True)
        self.a, self.cache, self.proc, self.log = a, cache, proc, logger
        self.cv = threading.Condition()
        self.jobs = {}
        self.stopping = False
        self.origin_bytes = self.fetches = self.prefetches = self.late = 0
        self.cancelled_prefetches = 0
        self.download_seconds = a.size/(a.mbps*125000)+0.05

    def request(self, key, due, prefetch):
        with self.cv:
            if self.stopping:
                raise RuntimeError('Origin fetcher is unavailable')
            now = time.monotonic()
            stale = [k for k, j in self.jobs.items() if j['prefetch'] and not j['active']
                     and j['due'] < now-self.download_seconds]
            for k in stale:
                self.jobs.pop(k)['future'].cancel()
                self.cancelled_prefetches += 1
            item = self.cache.get(key, demand=not prefetch)
            if item:
                f = concurrent.futures.Future()
                f.set_result(item)
                return f, True
            if key in self.jobs:
                job = self.jobs[key]
                if not prefetch:
                    job['prefetch'] = False
                    job['at'] = min(job['at'], time.monotonic())
                self.cv.notify_all()
                return job['future'], False
            f = concurrent.futures.Future()
            if prefetch and len(self.jobs) >= 2*self.a.users:
                f.cancel()
                self.cancelled_prefetches += 1
                return f, False
            # Schedule as late as possible, with a short safety margin.
            at = due-self.download_seconds-self.a.prefetch_margin if prefetch else time.monotonic()
            self.jobs[key] = {'future': f, 'at': at, 'due': due, 'prefetch': prefetch, 'active': False}
            self.cv.notify_all()
            return f, False

    def run(self):
        try:
            while True:
                with self.cv:
                    if self.stopping:
                        return
                    pending = [(key, job) for key, job in self.jobs.items() if not job['active']]
                    if not pending:
                        self.cv.wait(1)
                        continue
                    # Demand jobs take precedence over speculative downloads.
                    key, job = min(pending, key=lambda x: (x[1]['prefetch'], x[1]['at']))
                    delay = job['at']-time.monotonic()
                    if delay > 0:
                        self.cv.wait(min(delay, 1))
                        continue
                    job['active'] = True
                    started_prefetch = job['prefetch']
                began = time.monotonic()
                if self.a.transport == 'https':
                    data,digest=self.proc.fetch(key,self.a.size)
                else:
                    emit(self.proc.stdin, {'stream': key[0], 'seq': key[1], 'size': self.a.size})
                    header = line(self.proc.stdout)
                    if header['size'] != self.a.size:
                        raise ValueError('Origin segment size mismatch')
                    data = exact(self.proc.stdout, self.a.size)
                    digest = hashlib.sha256(data).hexdigest()
                    if digest != header['sha256']:
                        raise ValueError('Origin checksum mismatch')
                elapsed = time.monotonic()-began
                with self.cv:
                    self.download_seconds = 0.2*elapsed+0.8*self.download_seconds
                    self.origin_bytes += len(data)
                    self.fetches += 1
                    self.prefetches += int(started_prefetch)
                    self.late += int(started_prefetch and time.monotonic() > job['due'])
                    item = self.cache.put(key, data, digest, started_prefetch)
                    if not job['prefetch']:
                        item['used'] = True
                        self.cache.used_prefetch += int(started_prefetch)
                    self.jobs.pop(key)
                    job['future'].set_result(item)
                self.log.info(json.dumps({'event': 'fetch', 'key': key, 'bytes': len(data),
                                          'prefetch': job['prefetch'], 'seconds': round(elapsed, 4)}))
        except Exception as e:
            with self.cv:
                intentional_stop = self.stopping
                self.stopping = True
                for job in self.jobs.values():
                    if not job['future'].done():
                        job['future'].set_exception(e)
                self.jobs.clear()
            if not intentional_stop:
                self.log.error('fetcher failed: %s', e)


def validate_receipt(req, item, ack):
    """Only a fully verified receipt permits predictor state to advance."""
    if (ack.get('kind') != 'ack' or ack.get('id') != req['id'] or
            ack.get('sha256') != item['sha256'] or ack.get('bytes') != len(item['data'])):
        raise ValueError('Client receipt failed')
    return ack


def run(a):
    state = Path(a.state)
    state.mkdir(parents=True, exist_ok=True)
    lock = open(state/'run.lock', 'a+')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Cache PoC already running')
    logger = logging.getLogger('ontime-cache')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(state/'events.log', maxBytes=5_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(handler)
    start = time.monotonic()
    origin_proc = client_proc = fetcher = None
    hits = misses = delivered = requests = 0
    last_summary = start
    def summary(event):
        now = time.monotonic()
        with fetcher.cv:
            cache.expire()
            result = {'event': event, 'origin': a.origin, 'clients': a.clients,
                      'elapsed_seconds': round(now-start, 2), 'requests': requests,
                      'cache_hits': hits, 'cache_misses': misses,
                      'hit_ratio': round(hits/max(1, requests), 4),
                      'origin_bytes': fetcher.origin_bytes,
                      'origin_payload_mbps': round(fetcher.origin_bytes*8/(now-start)/1e6, 3),
                      'delivered_bytes': delivered, 'cache_bytes': cache.bytes,
                      'prefetch_downloads': fetcher.prefetches,
                      'prefetch_used': cache.used_prefetch, 'prefetch_evicted_unused': cache.evicted_unused,
                      'prefetch_late': fetcher.late, 'pending': len(fetcher.jobs),
                      'prefetch_cancelled': fetcher.cancelled_prefetches,
                      'origin_rate_limit_mbps': a.mbps}
        result['transport']=a.transport
        if a.transport=='https':
            result['origin']='https://%s:%s'%(a.origin_host,a.origin_port)
            result['clients']='https://%s:%s'%(a.client_host,a.client_port)
        result['origin_observed_source'] = observed_source
        result['koren_source_verified'] = observed_source == '192.0.2.20'
        tmp = state/'metrics.tmp'
        tmp.write_text(json.dumps(result, indent=2)+'\n')
        tmp.replace(state/'metrics.json')
        logger.info(json.dumps(result))
        print(json.dumps(result), flush=True)
    def interrupted(sig, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    if a.seconds:
        signal.alarm(a.seconds)
    try:
        logger.info(json.dumps({'event': 'start', 'configuration': vars(a)}))
        if a.transport=='https':
            from cache_transport import Origin,Clients
            origin_proc=Origin(a.origin_host,a.origin_port,a.origin_bind,a.origin_ca,a.origin_cert,a.origin_key)
            ready=origin_proc.ready
            if float(ready['rate_limit_mbps'])!=a.mbps:
                raise ValueError('Configured origin rate limit does not match server policy')
        else:
            origin_proc = connect(a, a.origin, 'origin')
            ready = line(origin_proc.stdout)
            if ready.get('kind') != 'ready' or ready.get('role') != 'origin':
                raise ValueError('Origin handshake failed')
        observed_source = ready['source_address']
        if not a.allow_campus_test and observed_source != '192.0.2.20':
            raise RuntimeError('Origin sees source '+observed_source+'; refusing a non-KOREN source')
        cache = Cache(a.cache_mib*1024**2, a.ttl)
        predictor = Predictor(a.streams)
        fetcher = Fetcher(a, cache, origin_proc, logger)
        fetcher.start()
        if a.transport=='https':
            client_proc=Clients(a.client_host,a.client_port,a.origin_bind,a.client_ca,a.client_cert,a.client_key)
            if client_proc.ready['size']!=a.size:
                raise ValueError('Remote client workload size mismatch')
        else:
            client_proc = connect(a, a.clients, 'clients')
        while True:
            req = client_proc.request() if a.transport=='https' else line(client_proc.stdout)
            if req.get('kind') != 'request' or req['size'] != a.size:
                raise ValueError('Client request protocol mismatch')
            began = time.monotonic()
            key = req['stream'], req['seq']
            future, hit = fetcher.request(key, began, False)
            item = future.result(timeout=60)
            if a.transport=='https':
                ack=client_proc.deliver(req,item)
            else:
                emit(client_proc.stdin, {'id': req['id'], 'size': len(item['data']), 'sha256': item['sha256']})
                client_proc.stdin.write(item['data'])
                client_proc.stdin.flush()
                ack = line(client_proc.stdout)
            validate_receipt(req, item, ack)
            latency_ms = (time.monotonic()-began)*1000
            # Commit P+ only after delivery and receipt validation have succeeded.
            predicted, confidence, interval = predictor.observe(req)
            if confidence >= a.confidence:
                fetcher.request(predicted, began+interval, True)
            hits += int(hit)
            misses += int(not hit)
            requests += 1
            delivered += ack['bytes']
            logger.info(json.dumps({'event': 'request', 'user': req['user'], 'zone': req['zone'],
                                    'key': key, 'cache_hit': hit, 'prediction': predicted,
                                    'confidence': round(confidence, 3), 'latency_ms': round(latency_ms, 2)}))
            if time.monotonic()-last_summary >= 10:
                summary('metrics')
                last_summary = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        signal.alarm(0)
        if fetcher:
            with fetcher.cv:
                fetcher.stopping = True
                fetcher.cv.notify_all()
        for proc in (client_proc, origin_proc):
            if proc is not None:
                if a.transport=='https':
                    # Unblock a concurrent HTTPS download before joining its worker.
                    if proc.conn.sock is not None:
                        import socket
                        try:proc.conn.sock.shutdown(socket.SHUT_RDWR)
                        except OSError:pass
                    proc.close()
                else:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
        if fetcher:
            fetcher.join(timeout=4)
            summary('stopped')
        lock.close()


def check(a):
    from cache_transport import Origin,Clients
    import ssl
    deadline=time.monotonic()+60
    while True:
        origin_client=peer_client=None
        try:
            origin_client=Origin(a.origin_host,a.origin_port,a.origin_bind,a.origin_ca,a.origin_cert,a.origin_key)
            peer_client=Clients(a.client_host,a.client_port,a.origin_bind,a.client_ca,a.client_cert,a.client_key)
            if peer_client.ready['size']!=a.size or float(origin_client.ready['rate_limit_mbps'])!=a.mbps:
                raise ValueError('Server workload/rate configuration mismatch')
            print(json.dumps({'origin':origin_client.ready,'clients':peer_client.ready,'transport':'https'}),flush=True)
            return
        except (ssl.SSLError,FileNotFoundError,PermissionError):
            raise
        except OSError:
            if time.monotonic()>=deadline:raise
            time.sleep(1)
        finally:
            if origin_client:origin_client.close()
            if peer_client:peer_client.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['run', 'origin', 'clients','check'])
    p.add_argument('--transport',choices=['https','ssh'],default='https')
    p.add_argument('--origin-host',default='198.51.100.10')
    p.add_argument('--origin-port',type=int,default=443)
    p.add_argument('--client-host',default='192.0.2.21')
    p.add_argument('--client-port',type=int,default=8443)
    base='/opt/ontime-cache-poc/work'
    p.add_argument('--origin-ca',default=base+'/image-relay-https/client-tls/ca.crt')
    p.add_argument('--origin-cert',default=base+'/image-relay-https/client-tls/client.crt')
    p.add_argument('--origin-key',default=base+'/image-relay-https/client-tls/client.key')
    p.add_argument('--client-ca',default=base+'/ontime-cache-https/client-tls/ca.crt')
    p.add_argument('--client-cert',default=base+'/ontime-cache-https/client-tls/client.crt')
    p.add_argument('--client-key',default=base+'/ontime-cache-https/client-tls/client.key')
    p.add_argument('--origin', default='cache@198.51.100.10')
    p.add_argument('--clients', default='cache-peer@192.0.2.21')
    p.add_argument('--key', default='/etc/ontime-cache-poc/deploy-key')
    p.add_argument('--origin-bind', default='192.0.2.20')
    p.add_argument('--allow-campus-test', action='store_true', help='Algorithm test only; permits non-KOREN source')
    p.add_argument('--remote-script', default='.cache/ontime-cache-poc/poc.py')
    p.add_argument('--state', default='/var/lib/ontime-cache-poc/.ontime-cache-poc')
    p.add_argument('--mbps', type=float, default=100)
    p.add_argument('--workload-mbps', type=float, default=110)
    p.add_argument('--size', type=int, default=2*1024**2)
    p.add_argument('--users', type=int, default=8)
    p.add_argument('--streams', type=int, default=8)
    p.add_argument('--cache-mib', type=int, default=128)
    p.add_argument('--ttl', type=float, default=30)
    p.add_argument('--confidence', type=float, default=0.55)
    p.add_argument('--prefetch-margin', type=float, default=0.5)
    p.add_argument('--switch-probability', type=float, default=0.08)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--seconds', type=int, default=0)
    return p


if __name__ == '__main__':
    p = parser()
    a = p.parse_args()
    for host in (a.origin, a.clients):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+@[A-Za-z0-9.-]+', host):
            p.error('SSH host must be user@host')
    if not (0 < a.mbps <= 10000 and 0 < a.workload_mbps <= 10000 and
            1 <= a.size <= 8*1024**2 and a.cache_mib*1024**2 >= a.size and
            1 <= a.streams <= 64 and 1 <= a.users <= 128 and a.ttl > 0 and
            0 <= a.confidence <= 1 and 0 <= a.switch_probability <= 1 and
            0 <= a.prefetch_margin <= 60 and a.seconds >= 0):
        p.error('Invalid workload, cache or rate configuration')
    {'run': run, 'origin': origin, 'clients': clients,'check':check}[a.mode](a)

import collections
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from cache_api import Users,segment_content
from experiment import Latencies,NoCache,POLICIES,Records,policy_prediction,DB_NAME,deliver_then_predict,archive_previous_revision,experiment_paths
from poc import Predictor


class UnitTests(unittest.TestCase):
    def test_no_cache_never_reuses_demand(self):
        cache=NoCache(1024,30)
        cache.put((0,0),b'abc','digest',False)
        self.assertIsNone(cache.get((0,0),True))
        self.assertEqual(cache.bytes,0)

    def test_same_trace_prefix_despite_slow_delivery(self):
        def trace(delay):
            users=Users({'users':3,'streams':3,'size':32,'workload_mbps':100,
                         'seed':42,'benchmark':True,'switch_probability':.5})
            result=[]
            import hashlib
            for i in range(30):
                req=users.next_request()
                result.append(tuple(req[k] for k in ('user','zone','stream','seq')))
                if delay:time.sleep(.001)
                data=segment_content(req['stream'],req['seq'],req['size'])
                users.delivered(req['id'],data,hashlib.sha256(data).hexdigest())
            return result
        self.assertEqual(trace(False),trace(True))

    def test_video_resume_and_duplicate_ack_do_not_advance_twice(self):
        import hashlib
        users=Users({'users':1,'streams':2,'size':32,'workload_mbps':100,
                     'benchmark':True,'switch_probability':0})
        seen=[]
        for stream in [0,0,1,1,0,1]:
            users.users[0]['stream']=stream
            req=users.next_request()
            seen.append((req['stream'],req['seq']))
            data=segment_content(req['stream'],req['seq'],req['size'])
            digest=hashlib.sha256(data).hexdigest()
            users.delivered(req['id'],data,digest)
            positions=list(users.users[0]['positions'])
            users.delivered(req['id'],data,digest)
            self.assertEqual(users.users[0]['positions'],positions)
        self.assertEqual(seen,[(0,0),(0,1),(1,0),(1,1),(0,2),(1,2)])

    def test_positions_are_private_to_user_and_predictor_resumes_video(self):
        users=Users({'users':2,'streams':2,'size':32,'workload_mbps':100})
        users.users[0]['positions'][0]=12
        self.assertEqual(users.users[1]['positions'],[0,0])
        p=Predictor(2)
        p.counts[(0,0)][1]=100
        key,_,_=p.observe(dict(user=0,zone=0,stream=0,seq=11,when=1,period=1,
                              playback_positions=[11,7]))
        self.assertEqual(key,(1,7))
        self.assertEqual(p.next_key(0,0),(0,12))
        key,_,_=p.observe(dict(user=1,zone=0,stream=0,seq=0,when=1,period=1,
                              playback_positions=[0,0]))
        self.assertEqual(key,(1,0))

    def test_latency_and_popularity_no_future_access(self):
        lat=Latencies()
        for n in range(1,101):lat.add(n)
        self.assertEqual(lat.percentile(.95),95)
        self.assertEqual(lat.metrics('latency')['latency_mean_ms'],50.5)
        p=Predictor(3);pop=collections.Counter({2:20})
        req=dict(user=0,zone=0,stream=1,seq=4,when=1,period=2)
        self.assertEqual(policy_prediction('popularity',req,p,pop,10),((2,0),10))
        self.assertIsNone(policy_prediction('lru',req,p,pop,10))
        self.assertIsNone(policy_prediction('none',req,p,pop,10))
        self.assertEqual(policy_prediction('ontime',req,p,pop,10)[0],(1,5))

    def test_delivery_failure_or_invalid_ack_cannot_advance_predictor(self):
        from unittest.mock import Mock
        req=dict(id=1,user=0,zone=0,stream=0,seq=11,when=1,period=1,
                 playback_positions=[11,7])
        item=dict(data=b'abc',sha256='digest')
        good=dict(kind='ack',id=1,bytes=3,sha256='digest')
        bad_acks=[dict(good,id=2),dict(good,bytes=0),dict(good,sha256='wrong'),
                  dict(good,kind='pending'),{}]
        callbacks=[Mock(side_effect=OSError('connection lost'))]+[
            Mock(return_value=ack) for ack in bad_acks]
        for deliver in callbacks:
            p=Predictor(2);pop=collections.Counter()
            with self.assertRaises((OSError,ValueError)):
                deliver_then_predict(deliver,req,item,'ontime',p,pop,10)
            self.assertEqual(p.positions,{})
            self.assertEqual(p.last,{})
            self.assertEqual(dict(p.counts),{})
            self.assertEqual(pop,{})

    def test_prediction_observes_acknowledged_post_delivery_cursor(self):
        req=dict(id=1,user=0,zone=0,stream=0,seq=11,when=1,period=1,
                 playback_positions=[11,7])
        item=dict(data=b'abc',sha256='digest')
        p=Predictor(2);pop=collections.Counter()
        def deliver(req,item):
            self.assertEqual(p.positions,{})
            self.assertEqual(pop,{})
            return dict(kind='ack',id=1,bytes=3,sha256='digest')
        ack,prediction,_=deliver_then_predict(deliver,req,item,'ontime',p,pop,10)
        self.assertEqual(prediction[0],(0,12))
        self.assertEqual(p.positions[0],[12,7])
        self.assertEqual(pop,{0:1})
        # On the next acknowledged switch, the untouched video's cursor resumes.
        req=dict(req,id=2,stream=1,seq=7,when=2,playback_positions=[12,7])
        p.counts[(0,1)][0]=100
        def deliver_b(req,item):
            self.assertEqual(p.positions[0],[12,7])
            return dict(kind='ack',id=2,bytes=3,sha256='digest')
        ack,prediction,_=deliver_then_predict(deliver_b,req,item,'ontime',p,pop,11)
        self.assertEqual(prediction[0],(0,12))
        self.assertEqual(p.positions[0],[12,8])

    def test_revision_archive_keeps_old_results_separate(self):
        with tempfile.TemporaryDirectory() as temp:
            state=Path(temp)
            (state/'experiment-current.json').write_text(json.dumps({'playback_model':'per-user-video-resume-v2'}))
            (state/'experiment-hourly.csv').write_text('old-results')
            (state/'experiments-resume-v2.sqlite3').write_bytes(b'old-db')
            archive_previous_revision(state)
            self.assertFalse((state/'experiment-hourly.csv').exists())
            self.assertEqual(next(state.glob('experiment-hourly.legacy-preack-*.csv')).read_text(),'old-results')
            self.assertEqual((state/'experiments-resume-v2.sqlite3').read_bytes(),b'old-db')
            (state/'experiment-current.json').write_text(json.dumps({'algorithm_revision':'ack-committed-v3'}))
            (state/'experiment-hourly.csv').write_text('new-results')
            archive_previous_revision(state)
            self.assertEqual((state/'experiment-hourly.csv').read_text(),'new-results')

    def test_margin_databases_are_isolated_and_mixing_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            state=Path(temp)
            baseline=Records(state)
            baseline.begin(0,'none',{'prefetch_margin':.5})
            with self.assertRaises(ValueError):
                baseline.begin(1,'lru',{'prefetch_margin':1})
            new=Records(state,'margin-1s')
            new.begin(0,'none',{'prefetch_margin':1})
            self.assertNotEqual(baseline.path,new.path)
            self.assertNotEqual(baseline.csv_path,new.csv_path)
            self.assertEqual(baseline.db.execute('SELECT COUNT(*) FROM phases').fetchone()[0],1)
            self.assertEqual(new.db.execute('SELECT COUNT(*) FROM phases').fetchone()[0],1)
            with self.assertRaises(ValueError):experiment_paths(state,'../outside')
            baseline.db.close();new.db.close()

    def test_resume_retries_interrupted_phase_and_advances_only_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            r=Records(Path(temp));ident=r.begin(0,'none',{})
            r.db.close()
            r=Records(Path(temp))
            self.assertEqual(r.next_ordinal(),0)
            self.assertEqual(r.db.execute('SELECT status FROM phases WHERE id=?',(ident,)).fetchone()[0],'interrupted')
            ident=r.begin(0,'none',{});r.save(ident,{'status':'complete'},'complete')
            self.assertEqual(r.next_ordinal(),1);r.db.close()


# Reuse real mTLS endpoints and fixtures without running the parent tests twice.
from test_https_cache import Tests as HTTPSFixture
class IntegrationTests(HTTPSFixture):
    def test_four_policies_real_https_and_persistent_resume(self):
        # Smaller segments make this test CPU/network light.
        flags=['--origin-host','127.0.0.1','--origin-port',str(self.origin.server_port),
               '--origin-bind','127.0.0.1','--client-host','127.0.0.1','--client-port',str(self.peer.server_port),
               '--origin-ca',self.args[3],'--origin-cert',self.args[4],'--origin-key',self.args[5],
               '--client-ca',self.args[3],'--client-cert',self.args[4],'--client-key',self.args[5],
               '--allow-campus-test','--size','262144','--users','2','--streams','2','--mbps','20',
               '--workload-mbps','10','--phase-seconds','1','--sample-seconds','1','--state',str(self.root/'comparison')]
        command=[sys.executable,str(Path(__file__).with_name('experiment.py')),'run',*flags]
        first=subprocess.run(command+['--max-phases','4'],capture_output=True,text=True,timeout=20)
        self.assertEqual(first.returncode,0,first.stderr)
        state=self.root/'comparison'
        db=sqlite3.connect(state/DB_NAME)
        rows=db.execute('SELECT policy,status,result_json FROM phases ORDER BY ordinal').fetchall()
        self.assertEqual([r[0] for r in rows],list(POLICIES))
        for policy,status,payload in rows:
            m=json.loads(payload)
            self.assertEqual(status,'complete');self.assertGreater(m['requests'],0)
            self.assertGreater(m['origin_bytes'],0);self.assertGreater(m['delivered_bytes'],0)
            self.assertGreater(m['latency_mean_ms'],0)
            self.assertLessEqual(m['cache_bytes'],128*1024**2)
            if m['prefetch_late_ratio'] is not None:self.assertTrue(0<=m['prefetch_late_ratio']<=1)
            if m['prefetch_waste_ratio'] is not None:self.assertTrue(0<=m['prefetch_waste_ratio']<=1)
            if policy=='none':self.assertEqual(m['cache_hits'],0);self.assertEqual(m['cache_bytes'],0)
            if policy in ('none','lru'):self.assertEqual(m['prefetch_downloads'],0)
        self.assertTrue((state/'experiment-hourly.csv').read_text(encoding='utf-8-sig').startswith('phase_id,'))
        again=subprocess.run(command+['--max-phases','1'],capture_output=True,text=True,timeout=10)
        self.assertEqual(again.returncode,0,again.stderr)
        self.assertEqual(db.execute('SELECT ordinal,policy FROM phases ORDER BY ordinal DESC LIMIT 1').fetchone(),(4,'none'))
        margin_run=subprocess.run(command+['--max-phases','4','--experiment-id','margin-1s','--prefetch-margin','1'],
                                  capture_output=True,text=True,timeout=20)
        self.assertEqual(margin_run.returncode,0,margin_run.stderr)
        margin_db=sqlite3.connect(experiment_paths(state,'margin-1s')[0])
        new_rows=margin_db.execute('SELECT policy,status,result_json,configuration_json FROM phases ORDER BY ordinal').fetchall()
        self.assertEqual([r[0] for r in new_rows],list(POLICIES))
        for policy,status,payload,configuration in new_rows:
            self.assertEqual(status,'complete')
            self.assertEqual(json.loads(payload)['prefetch_margin_seconds'],1)
            self.assertEqual(json.loads(payload)['experiment_id'],'margin-1s')
            self.assertEqual(json.loads(configuration)['prefetch_margin'],1)
        self.assertTrue((state/'experiment-hourly-margin-1s.csv').exists())
        self.assertEqual(db.execute('SELECT COUNT(*) FROM phases').fetchone()[0],5)
        margin_db.close();db.close()

    # Parent suite is run separately; only inherit TLS setup/teardown here.
    test_large_content_and_image_exchange_work_concurrently=None
    test_remote_client_delivery_retry_is_idempotent=None
    test_full_https_prediction_cache_remote_delivery_pipeline=None

# unittest otherwise discovers the imported fixture class as well.
del HTTPSFixture
if __name__=='__main__':unittest.main()

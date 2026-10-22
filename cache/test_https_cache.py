import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from cache_api import client_server,segment_content
from cache_transport import Origin,Clients
from image_https import Server,HTTPSChannel
from relay import Store,digest,png


def fixture(root,node):
    (root/'images').mkdir(parents=True);(root/'.image-relay').mkdir()
    name=f'{node}_cifar_00000_cat.png';data=png(bytes([10 if node=='B' else 1])*3072)
    (root/'images'/name).write_bytes(data)
    (root/'.image-relay'/f'dataset-{node}.json').write_text(json.dumps({'files':[{'name':name,'sha256':digest(data),'index':0}]}))


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name)
        generator=Path(__file__).parent.parent/'tools/generate-tls.sh'
        subprocess.run(['bash',str(generator),str(cls.base/'client'),str(cls.base/'server')],check=True,stdout=subprocess.DEVNULL)

    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()

    def setUp(self):
        self.tempdir=tempfile.TemporaryDirectory();self.root=Path(self.tempdir.name)
        fixture(self.root/'B','B');fixture(self.root/'A','A')
        config={'listen_address':'127.0.0.1','listen_port':0,'allowed_source':'127.0.0.1',
                'tls_ca':str(self.base/'server/ca.crt'),'tls_cert':str(self.base/'server/server.crt'),
                'tls_key':str(self.base/'server/server.key'),'cache_origin_mbps':20}
        self.origin=Server(config,self.root/'B')
        peer_config=dict(config,workload={'users':2,'streams':2,'size':262144,'workload_mbps':10,'seed':42})
        self.peer=client_server(peer_config)
        self.threads=[]
        for server in (self.origin,self.peer):
            def serve(s=server):
                try:s.serve_forever(poll_interval=.1)
                finally:s.server_close()
            thread=threading.Thread(target=serve,daemon=True);thread.start();self.threads.append(thread)
        self.args=('127.0.0.1',self.origin.server_port,'127.0.0.1',str(self.base/'client/ca.crt'),
                   str(self.base/'client/client.crt'),str(self.base/'client/client.key'))

    def tearDown(self):
        for server in (self.origin,self.peer):server.shutdown()
        for thread in self.threads:thread.join(timeout=3)
        self.tempdir.cleanup()

    def test_large_content_and_image_exchange_work_concurrently(self):
        origin=Origin(*self.args)
        result={}
        def download():result['segment']=origin.fetch((1,3),2*1024**2)
        thread=threading.Thread(target=download);thread.start();time.sleep(.05)
        config={'peer_host':self.args[0],'peer_port':self.args[1],'source_address':self.args[2],
                'tls_ca':self.args[3],'tls_cert':self.args[4],'tls_key':self.args[5]}
        image=HTTPSChannel(config);a=Store(self.root/'A','A')
        try:
            before=time.monotonic();row=a.pending()
            reply=image.call({'op':'exchange','id':row['id'],'image':json.loads(row['payload'])})['image']
            a.accept_reply(row,reply);image.call({'op':'ack','id':row['id'],'sha256':reply['sha256']});a.finish(row['id'])
            self.assertLess(time.monotonic()-before,.6)
            thread.join(timeout=3)
            data,sha=result['segment']
            self.assertEqual(data,segment_content(1,3,len(data)))
            self.assertEqual(hashlib.sha256(data).hexdigest(),sha)
        finally:origin.close();image.close();a.close()

    def test_remote_client_delivery_retry_is_idempotent(self):
        args=list(self.args);args[1]=self.peer.server_port
        peer=Clients(*args)
        try:
            req=peer.request();self.assertEqual(peer.request(),req)
            data=segment_content(req['stream'],req['seq'],req['size'])
            item={'data':data,'sha256':hashlib.sha256(data).hexdigest()}
            ack=peer.deliver(req,item);again=peer.deliver(req,item)
            self.assertEqual(ack,again);self.assertEqual(self.peer.users.delivered_bytes,len(data))
            self.assertNotEqual(peer.request()['id'],req['id'])
        finally:peer.close()

    def test_full_https_prediction_cache_remote_delivery_pipeline(self):
        flags=['--transport','https','--origin-host','127.0.0.1','--origin-port',str(self.origin.server_port),
               '--origin-bind','127.0.0.1','--client-host','127.0.0.1','--client-port',str(self.peer.server_port),
               '--origin-ca',self.args[3],'--origin-cert',self.args[4],'--origin-key',self.args[5],
               '--client-ca',self.args[3],'--client-cert',self.args[4],'--client-key',self.args[5],
               '--allow-campus-test','--size','262144','--users','2','--streams','2',
               '--workload-mbps','10','--mbps','20','--seconds','3','--state',str(self.root/'metrics')]
        run=subprocess.run([sys.executable,str(Path(__file__).with_name('poc.py')),'run',*flags],
                           capture_output=True,text=True,timeout=10)
        self.assertEqual(run.returncode,0,run.stderr)
        m=json.loads((self.root/'metrics/metrics.json').read_text())
        self.assertGreater(m['origin_bytes'],0);self.assertGreater(m['delivered_bytes'],0)
        self.assertGreater(m['prefetch_downloads'],0);self.assertGreater(m['cache_hits'],0)
        self.assertEqual(m['transport'],'https');self.assertLessEqual(m['cache_bytes'],128*1024**2)


if __name__=='__main__':unittest.main()

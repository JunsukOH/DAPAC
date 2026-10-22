#!/usr/bin/env python3
"""HTTPS live segments and remote synthetic users, sharing mutual TLS policy."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import random
import signal
import threading
import time
from urllib.parse import parse_qs, urlsplit
from image_https import Handler, Server


def segment_content(stream, seq, size):
    return hashlib.shake_256(f'ontime-poc-v1:{stream}:{seq}:{size}'.encode()).digest(size)


def origin_get(handler):
    parsed = urlsplit(handler.path)
    if parsed.path == '/cache/health':
        return handler.reply(200, {'role':'cache-origin','version':1,
            'source_address':handler.client_address[0],
            'destination_address':handler.server.server_address[0],
            'rate_limit_mbps':handler.server.config.get('cache_origin_mbps',100)})
    if parsed.path != '/cache/segment':
        return handler.reply(404, {'error':'Unknown cache endpoint'})
    try:
        query=parse_qs(parsed.query,strict_parsing=True)
        if set(query)!={'stream','seq','size'} or any(len(v)!=1 for v in query.values()):
            raise ValueError('Invalid segment query')
        stream,seq,size=[int(query[k][0]) for k in ('stream','seq','size')]
        if not (0<=stream<64 and 0<=seq<10**12 and 1<=size<=8*1024**2):
            raise ValueError('Invalid segment bounds')
    except (ValueError,KeyError) as error:
        return handler.reply(400, {'error':str(error)})
    data=segment_content(stream,seq,size)
    handler.send_response(200)
    handler.send_header('Content-Type','application/octet-stream')
    handler.send_header('Content-Length',str(size))
    handler.send_header('X-SHA256',hashlib.sha256(data).hexdigest())
    handler.send_header('X-Source-Address',handler.client_address[0])
    handler.end_headers()
    try:
        for offset in range(0,size,65536):
            chunk=data[offset:offset+65536]
            handler.server.pace_content(len(chunk))
            handler.wfile.write(chunk);handler.wfile.flush()
    except OSError:
        handler.close_connection=True


class Users:
    def __init__(self, config):
        self.config=config
        self.random=random.Random(config.get('seed',42))
        self.size=int(config.get('size',2*1024**2))
        self.period=self.size*config.get('users',8)/(config.get('workload_mbps',110)*125000)
        self.streams=config.get('streams',8)
        self.lock=threading.Lock()
        began=time.monotonic()
        self.users=[{'due':began+i*self.period/config.get('users',8),'stream':i%self.streams,
                     'positions':[0]*self.streams,'zone':i%3} for i in range(config.get('users',8))]
        self.began=began
        self.counter=0;self.pending=None
        self.receipts=collections.OrderedDict()
        self.delivered_bytes=0

    def next_request(self):
        with self.lock:
            if self.pending:
                return dict(self.pending)
            user=(self.counter % len(self.users) if self.config.get('benchmark',False) else
                  min(range(len(self.users)),key=lambda i:self.users[i]['due']))
            u=self.users[user]
            time.sleep(max(0,u['due']-time.monotonic()))
            self.counter+=1
            self.pending={'kind':'request','id':self.counter,'user':user,'stream':u['stream'],
                          'seq':u['positions'][u['stream']],'zone':u['zone'],
                          'playback_positions':list(u['positions']),'when':time.monotonic(),
                          'period':self.period,'size':self.size,
                          'queue_seconds':max(0,time.monotonic()-u['due'])}
            return dict(self.pending)

    def delivered(self, request_id, data, expected_hash):
        digest=hashlib.sha256(data).hexdigest()
        if digest!=expected_hash or len(data)!=self.size:
            raise ValueError('Delivered segment checksum/size mismatch')
        with self.lock:
            old=self.receipts.get(request_id)
            if old:
                if old['sha256']!=digest:
                    raise ValueError('Duplicate delivery changed its content')
                return old
            if self.pending is None or self.pending['id']!=request_id:
                raise ValueError('Unknown request ID')
            u=self.users[self.pending['user']]
            u['due']=(u['due']+self.period if self.config.get('benchmark',False) else
                      max(u['due']+self.period,time.monotonic()))
            u['positions'][self.pending['stream']]=self.pending['seq']+1
            if self.random.random()<.10:u['zone']=(u['zone']+1)%3
            if self.random.random()<self.config.get('switch_probability',.08):u['stream']=self.random.randrange(self.streams)
            ack={'kind':'ack','id':request_id,'bytes':len(data),'sha256':digest}
            self.receipts[request_id]=ack
            while len(self.receipts)>128:self.receipts.popitem(last=False)
            self.delivered_bytes+=len(data);self.pending=None
            return ack


class ClientHandler(Handler):
    def do_GET(self):
        if not self.authorized():return self.reply(403,{'error':'Client identity mismatch'})
        if self.path=='/client/health':
            return self.reply(200,{'role':'cache-clients','version':1,
                'source_address':self.client_address[0],'destination_address':self.server.server_address[0],
                'playback_model':'per-user-video-resume-v2',
                'size':self.server.users.size,'delivered_bytes':self.server.users.delivered_bytes})
        if self.path=='/client/request':return self.reply(200,self.server.users.next_request())
        return self.reply(404,{'error':'Unknown endpoint'})

    def do_POST(self):
        if self.authorized() and self.path=='/client/reset':
            # Only the mutually authenticated .53 client can reset this workload.
            # The global client lock prevents resetting while next_request/delivery is in progress.
            length=int(self.headers.get('Content-Length','0'))
            if length!=0:
                self.close_connection=True
                return self.reply(400,{'error':'Reset must have an empty body'})
            old=self.server.users
            with old.lock:
                config=dict(old.config,benchmark=True)
                self.server.users=Users(config)
            return self.reply(200,{'reset':True,'seed':config.get('seed',42),
                                   'users':len(self.server.users.users),
                                   'streams':self.server.users.streams,
                                   'size':self.server.users.size,'benchmark':True,
                                   'playback_model':'per-user-video-resume-v2',
                                   'workload_mbps':config.get('workload_mbps',110),
                                   'switch_probability':config.get('switch_probability',.08)})
        if not self.authorized() or self.path!='/client/deliver':
            self.close_connection=True
            return self.reply(403,{'error':'Unauthorized endpoint'})
        try:
            length=int(self.headers.get('Content-Length','0'))
            if length!=self.server.users.size or not 0<length<=8*1024**2:
                raise ValueError('Invalid delivered size')
            request_id=int(self.headers['X-Request-ID'])
            digest=self.headers['X-SHA256']
            data=self.rfile.read(length)
            ack=self.server.users.delivered(request_id,data,digest)
            self.reply(200,ack)
        except (ValueError,KeyError,TypeError) as error:
            self.close_connection=True;self.reply(400,{'error':str(error)})


def client_server(config):
    server=Server(config,root='.',handler=ClientHandler)
    server.users=Users(config.get('workload',{}))
    return server


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',required=True)
    a=p.parse_args();server=client_server(json.loads(Path(a.config).read_text()))
    def stop(signum,frame):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    print('HTTPS synthetic users listening on %s:%s'%server.server_address,flush=True)
    try:server.serve_forever(poll_interval=.5)
    except KeyboardInterrupt:pass
    finally:server.server_close()

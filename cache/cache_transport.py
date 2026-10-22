"""HTTPS transport for both original content and actual remote client delivery."""
import hashlib
import http.client
import json
import ssl
from urllib.parse import urlencode


class Connection:
    def __init__(self, host, port, source, ca, cert, key):
        self.host,self.source=host,source
        context=ssl.create_default_context(cafile=ca)
        context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert,key)
        self.conn=http.client.HTTPSConnection(host,port,timeout=20,context=context,source_address=(source,0))

    def json(self, method, path, body=None, headers=None):
        self.conn.request(method,path,body=body,headers=headers or {})
        response=self.conn.getresponse()
        data=response.read(65537)
        if len(data)>65536:raise ValueError('Oversized metadata response')
        result=json.loads(data)
        if response.status!=200:raise RuntimeError('HTTPS %s: %s'%(response.status,result.get('error')))
        return result

    def health(self,path,role):
        result=self.json('GET',path)
        if result.get('role')!=role or result.get('source_address')!=self.source or result.get('destination_address')!=self.host:
            raise RuntimeError('HTTPS address or role mismatch')
        return result

    def close(self):self.conn.close()


class Origin(Connection):
    def __init__(self,*args):
        super().__init__(*args)
        self.ready=self.health('/cache/health','cache-origin')

    def fetch(self,key,size):
        self.conn.request('GET','/cache/segment?'+urlencode({'stream':key[0],'seq':key[1],'size':size}))
        response=self.conn.getresponse()
        if response.status!=200:raise RuntimeError('Origin segment HTTPS status %s'%response.status)
        if response.getheader('Content-Length')!=str(size) or response.getheader('X-Source-Address')!=self.source:
            raise ValueError('Origin segment address/length mismatch')
        digest=response.getheader('X-SHA256')
        data=response.read(size+1)
        if len(data)!=size or hashlib.sha256(data).hexdigest()!=digest:
            raise ValueError('Origin content checksum mismatch')
        return data,digest


class Clients(Connection):
    def __init__(self,*args):
        super().__init__(*args)
        self.ready=self.health('/client/health','cache-clients')

    def request(self):return self.json('GET','/client/request')

    def deliver(self,request,item):
        result=self.json('POST','/client/deliver',body=item['data'],headers={
            'Content-Type':'application/octet-stream','X-Request-ID':str(request['id']),
            'X-SHA256':item['sha256']})
        if result.get('kind')!='ack' or result.get('id')!=request['id'] or result.get('sha256')!=item['sha256']:
            raise ValueError('Remote client receipt mismatch')
        return result

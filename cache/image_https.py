#!/usr/bin/env python3
"""Mutually authenticated HTTPS adapter for the existing durable image protocol."""
import argparse
import fcntl
import hmac
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import queue
import ssl
import sys
import time
import threading

from relay import Store, MAX_LINE, prepare, route_interface


class HTTPSChannel:
    def __init__(self, config):
        self.config = config
        context = ssl.create_default_context(cafile=config['tls_ca'])
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(config['tls_cert'], config['tls_key'])
        self.conn = http.client.HTTPSConnection(
            config['peer_host'], int(config.get('peer_port', 443)),
            timeout=float(config.get('timeout_seconds', 120)), context=context,
            source_address=(config['source_address'], 0))
        try:
            self.hello = self.request('GET', '/health')
            if self.hello.get('source_address') != config['source_address']:
                raise RuntimeError('HTTPS origin sees a different source address')
            if self.hello.get('destination_address') != config['peer_host']:
                raise RuntimeError('HTTPS server address mismatch')
            if self.hello.get('ready') != 'B' or self.hello.get('version') != 1:
                raise RuntimeError('HTTPS worker protocol mismatch')
        except Exception:
            self.close()
            raise

    def request(self, method, path, message=None):
        body = json.dumps(message).encode() if message is not None else None
        try:
            self.conn.request(method, path, body=body, headers={'Content-Type': 'application/json'})
            response = self.conn.getresponse()
            raw = response.read(MAX_LINE+1)
        except http.client.HTTPException as e:
            raise OSError('HTTPS protocol failure') from e
        if len(raw) > MAX_LINE:
            raise ValueError('Oversized HTTPS response')
        result = json.loads(raw)
        if response.status != 200 or 'error' in result:
            raise RuntimeError('HTTPS %s: %s' % (response.status, result.get('error', 'request failed')))
        return result

    def call(self, message):
        return self.request('POST', '/rpc', message)

    def close(self):
        self.conn.close()


def check_ready(config, wait_seconds=0, retry_interval=.5):
    deadline = time.monotonic()+wait_seconds
    while True:
        try:
            probe_config = dict(config)
            if wait_seconds > 0:
                probe_config['timeout_seconds'] = min(float(config.get('timeout_seconds', 15)),
                                                     max(.1, deadline-time.monotonic()))
            channel = HTTPSChannel(probe_config)
            try:
                return channel.hello
            finally:
                channel.close()
        except (ssl.SSLError, FileNotFoundError, PermissionError):
            # Certificate/security/configuration failures need correction, not retries.
            raise
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(retry_interval, max(0, deadline-time.monotonic())))


class Busy(Exception):
    pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, config, root, handler=None):
        self.config, self.root, self.store = config, root, None
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(config['tls_cert'], config['tls_key'])
        context.load_verify_locations(cafile=config['tls_ca'])
        context.verify_mode = ssl.CERT_REQUIRED
        self.context = context
        self.jobs = queue.Queue(maxsize=64)
        self.db_thread = None
        self.db_start_lock = threading.Lock()
        self.thread_slots = threading.BoundedSemaphore(12)
        self.rate_lock = threading.Lock()
        self.next_content_send = 0.0
        super().__init__((config['listen_address'], int(config.get('listen_port', 443))), handler or Handler)

    def process_request(self, request, client_address):
        if not self.thread_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.thread_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.thread_slots.release()

    def image_rpc(self, message):
        # All SQLite operations remain on one owning thread, separate from bulk downloads.
        with self.db_start_lock:
            if self.db_thread is None:
                self.db_thread = threading.Thread(target=self.db_loop, daemon=True)
                self.db_thread.start()
        future = concurrent.futures.Future()
        self.jobs.put((message, future), timeout=2)
        return future.result(timeout=120)

    def db_loop(self):
        try:
            while True:
                item = self.jobs.get()
                if item is None:
                    return
                message, future = item
                try:
                    store = self.get_store()
                    if message['op'] == 'exchange':
                        result = {'image': store.exchange(message['id'], message['image'])}
                    elif message['op'] == 'ack':
                        store.ack(message['id'], message['sha256'], message.get('metrics'))
                        result = {'acked': message['id']}
                    else:
                        raise ValueError('Unknown operation')
                    future.set_result(result)
                except Exception as error:
                    future.set_exception(error)
        finally:
            if self.store is not None:
                self.store.close()
                self.store = None

    def pace_content(self, count):
        with self.rate_lock:
            now = time.monotonic()
            due = max(now, self.next_content_send)
            self.next_content_send = due+count/(float(self.config.get('cache_origin_mbps',100))*125000)
        time.sleep(max(0,due-time.monotonic()))

    def get_request(self):
        connection, address = self.socket.accept()
        try:
            if address[0] != self.config['allowed_source']:
                raise PermissionError('Unexpected source IP')
            connection.settimeout(15)
            return self.context.wrap_socket(connection, server_side=True), address
        except Exception:
            connection.close()
            raise

    def get_store(self):
        if self.store is None:
            # Health checks work while old SSH worker still holds the B lock.
            probe = open(Path(self.root)/'.image-relay/B.lock', 'a')
            try:
                fcntl.flock(probe, fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                raise Busy('Old image worker is still active; retry after cutover')
            finally:
                probe.close()
            self.store = Store(self.root, 'B', route_interface(self.config['allowed_source']))
        return self.store

    def server_close(self):
        if self.db_thread is not None:
            self.jobs.put(None)
            self.db_thread.join(timeout=10)
        super().server_close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def authorized(self):
        cert = self.connection.getpeercert()
        common_names = [value for fields in cert.get('subject', ()) for key, value in fields
                        if key == 'commonName']
        return common_names == ['image-relay-A']

    def reply(self, code, result):
        data = json.dumps(result).encode()
        try:
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except OSError:
            # A committed exchange remains retryable if the peer closed before its receipt.
            self.close_connection=True

    def do_GET(self):
        if not self.authorized():
            self.reply(403, {'error': 'Client identity mismatch'})
        elif self.path.startswith('/cache/'):
            from cache_api import origin_get
            origin_get(self)
        elif self.path != '/health':
            self.reply(404, {'error': 'Unknown endpoint'})
        else:
            self.reply(200, {'ready': 'B', 'version': 1, 'transport': 'https',
                             'source_address': self.client_address[0],
                             'destination_address': self.server.server_address[0]})

    def do_POST(self):
        if not self.authorized() or self.path != '/rpc':
            self.close_connection = True
            return self.reply(403, {'error': 'Unauthorized endpoint'})
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= MAX_LINE:
                raise ValueError('Invalid request size')
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError('Incomplete body')
            message = json.loads(body)
            result = self.server.image_rpc(message)
            self.reply(200, result)
        except Busy as e:
            self.reply(409, {'error': str(e)})
        except (ValueError, KeyError, TypeError) as e:
            self.close_connection = True
            self.reply(400, {'error': str(e)})
        except OSError:
            self.close_connection = True
            self.reply(503, {'error': 'Storage failure; retry safely'})

    def log_message(self, fmt, *args):
        print('HTTPS peer=%s %s' % (self.client_address[0], fmt % args), file=sys.stderr, flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['serve', 'check'])
    p.add_argument('--root', default='/var/lib/ontime-cache-poc')
    p.add_argument('--config', required=True)
    p.add_argument('--wait', type=float, default=0, help='Retry transient startup connection failures')
    a = p.parse_args()
    config = json.loads(Path(a.config).read_text())
    if a.mode == 'check':
        print(json.dumps(check_ready(config, a.wait)), flush=True)
        return
    prepare(a.root, 'B')
    server = Server(config, a.root)
    def stop(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    print('HTTPS listening on %s:%s; allowed source=%s' % (
        *server.server_address, config['allowed_source']), flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()

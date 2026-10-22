#!/usr/bin/env python3
"""Durable, bidirectional random PNG exchange over one authenticated SSH channel."""
import argparse
import base64
import csv
import datetime as dt
import fcntl
import hashlib
import io
import ipaddress
import json
import os
from pathlib import Path
import queue
import random
import re
import shlex
import sqlite3
import struct
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
import uuid
import zlib

SOURCE = "https://cave.cs.toronto.edu/kriz/cifar-10-binary.tar.gz"
SOURCE_MD5 = "c32a1d4ab5d03f1284b67883e8d87530"
LABELS = "airplane automobile bird cat deer dog frog horse ship truck".split()
FIELDS = ["event_id", "time_utc", "time_kst", "round_id", "direction", "node",
          "peer", "filename", "saved_path", "bytes", "sha256", "status", "hour_bucket",
          "total_send_bytes", "total_receive_bytes", "average_payload_mbps",
          "exchange_seconds", "exchange_payload_bytes", "exchange_payload_mbps",
          "nic_interface", "nic_rx_bytes", "nic_tx_bytes", "nic_interval_seconds",
          "nic_rx_mbps", "nic_tx_mbps"]
MAX_LINE = 4 * 1024 * 1024
KST = dt.timezone(dt.timedelta(hours=9))


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    sync_dir(path.parent.parent)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)
    sync_dir(path.parent)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def png(rgb):
    def chunk(kind, data):
        return struct.pack("!I", len(data)) + kind + data + struct.pack("!I", zlib.crc32(kind + data))
    rows = b"".join(b"\0" + rgb[i:i+96] for i in range(0, len(rgb), 96))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack("!IIBBBBB", 32, 32, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


def prepare(root, node):
    state = Path(root) / ".image-relay"
    state.mkdir(parents=True, exist_ok=True)
    with (state / "dataset.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        _prepare(root, node)


def _prepare(root, node):
    """Only images/ is sent; received/, logs/, and hidden state are never selected."""
    root = Path(root)
    state = root / ".image-relay"
    images = root / "images"
    for p in [state, images, root / "received", root / "logs"]:
        p.mkdir(parents=True, exist_ok=True)
    marker = state / f"dataset-{node}.json"
    if marker.exists():
        manifest = json.loads(marker.read_text())
        if all((images / x["name"]).is_file() and digest((images / x["name"]).read_bytes()) == x["sha256"]
               for x in manifest["files"]):
            return
    archive = state / "cifar-10-binary.tar.gz"
    if not archive.exists() or hashlib.md5(archive.read_bytes()).hexdigest() != SOURCE_MD5:
        print("Downloading public CIFAR-10 archive (~162 MiB)...", file=sys.stderr, flush=True)
        temp = archive.with_suffix(".download")
        md5 = hashlib.md5()
        with urllib.request.urlopen(SOURCE, timeout=60) as response, temp.open("wb") as f:
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                md5.update(block)
                f.write(block)
            f.flush()
            os.fsync(f.fileno())
        if md5.hexdigest() != SOURCE_MD5:
            temp.unlink()
            raise ValueError("CIFAR-10 archive checksum mismatch")
        os.replace(temp, archive)
        sync_dir(state)
    with tarfile.open(archive, "r:gz") as tar:
        f = tar.extractfile("cifar-10-batches-bin/test_batch.bin")
        if f is None:
            raise ValueError("Missing CIFAR test batch")
        records = f.read()
    if len(records) != 10000 * 3073:
        raise ValueError("Unexpected CIFAR test batch length")
    indices = random.Random(20261001).sample(range(10000), 2000)
    indices = indices[:1000] if node == "A" else indices[1000:]
    files = []
    for i in indices:
        record = records[i*3073:(i+1)*3073]
        channels = record[1:]
        rgb = bytes(channels[c*1024+j] for j in range(1024) for c in range(3))
        data = png(rgb)
        name = f"{node}_cifar_{i:05d}_{LABELS[record[0]]}.png"
        atomic_write(images / name, data)
        files.append({"name": name, "sha256": digest(data), "index": i})
    atomic_write(marker, json.dumps({"source": SOURCE, "node": node, "files": files}, indent=2).encode())
    atomic_write(images / "SOURCE.txt", (
        "CIFAR-10 by Alex Krizhevsky, Vinod Nair and Geoffrey Hinton.\n"
        "https://www.cs.toronto.edu/~kriz/cifar.html\n"
        "32x32 color photographs; deterministic disjoint 1000-image subsets.\n"
    ).encode())
    print(f"Prepared 1000 PNG images for node {node}.", file=sys.stderr, flush=True)


class Store:
    def __init__(self, root, node, interface=None):
        self.root, self.node = Path(root), node
        self.interface = interface
        self.nic_previous = None
        self.state = self.root / ".image-relay"
        self.state.mkdir(parents=True, exist_ok=True)
        self.lock = (self.state / f"{node}.lock").open("a")
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        self.db = sqlite3.connect(self.state / f"{node}.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS rounds (
              id TEXT PRIMARY KEY, phase TEXT NOT NULL, payload TEXT NOT NULL,
              incoming_hash TEXT, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
              event_id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT NOT NULL,
              time_utc TEXT NOT NULL, time_kst TEXT NOT NULL, round_id TEXT NOT NULL,
              direction TEXT NOT NULL, node TEXT NOT NULL, peer TEXT NOT NULL,
              filename TEXT NOT NULL, saved_path TEXT NOT NULL, bytes INTEGER NOT NULL,
              sha256 TEXT NOT NULL, status TEXT NOT NULL,
              UNIQUE(round_id, direction));
        """)
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(events)")}
        with self.db:
            for name in FIELDS[12:]:
                if name not in columns:
                    kind = "TEXT" if name in ("hour_bucket", "nic_interface") else "REAL"
                    self.db.execute(f"ALTER TABLE events ADD COLUMN {name} {kind}")
            for row in self.db.execute("SELECT event_id,time_kst FROM events WHERE hour_bucket IS NULL").fetchall():
                bucket = dt.datetime.fromisoformat(row[1]).astimezone(KST).strftime("%Y-%m-%d_%H")
                self.db.execute("UPDATE events SET hour_bucket=? WHERE event_id=?", (bucket,row[0]))
            round_columns = {r[1] for r in self.db.execute("PRAGMA table_info(rounds)")}
            if "metrics" not in round_columns:
                self.db.execute("ALTER TABLE rounds ADD COLUMN metrics TEXT")
            self.db.execute('CREATE INDEX IF NOT EXISTS events_hour_bucket_idx ON events(hour_bucket,event_id)')
        self.csv_counts = {}
        self.csv_dirty = set()
        # SQLite is authoritative. Rebuild CSV at startup, repairing any interrupted append.
        for row in self.db.execute("SELECT DISTINCT hour_bucket FROM events"):
            self.export_day(row[0], rebuild=True)
        # Keep the old daily files as archives after rebuilding all hourly files.
        logs = self.root / "logs"
        if logs.exists():
            for old in logs.glob(f"node-{node}_????-??-??.csv"):
                archive = logs / "legacy-daily"
                archive.mkdir(exist_ok=True)
                destination = archive / old.name
                if not destination.exists():
                    os.rename(old, destination)
                    sync_dir(archive)
                    sync_dir(logs)
        sync_dir(self.state)

    def close(self):
        self.db.close()
        self.lock.close()

    def event(self, round_id, direction, peer, payload, path, metrics=None):
        now = dt.datetime.now(dt.timezone.utc)
        local = now.astimezone(KST)
        self.db.execute("""INSERT OR IGNORE INTO events
          (day,time_utc,time_kst,round_id,direction,node,peer,filename,saved_path,bytes,sha256,status)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
            local.date().isoformat(), now.isoformat(timespec="microseconds"),
            local.isoformat(timespec="microseconds"), round_id, direction, self.node, peer,
            payload["name"], str(path), payload["bytes"], payload["sha256"], "durable_ack"))
        # Statistics describe successful application payloads, not SSH wire bytes.
        totals = self.db.execute("""SELECT
          COALESCE(SUM(CASE WHEN direction='SEND' THEN bytes ELSE 0 END),0),
          COALESCE(SUM(CASE WHEN direction='RECEIVE' THEN bytes ELSE 0 END),0),
          MIN(time_utc) FROM events""").fetchone()
        first = dt.datetime.fromisoformat(totals[2])
        seconds = (now-first).total_seconds()
        values = dict(metrics or {}) if direction == "SEND" else {}
        values.update(hour_bucket=local.strftime("%Y-%m-%d_%H"),
                      total_send_bytes=totals[0], total_receive_bytes=totals[1],
                      average_payload_mbps=(totals[0]+totals[1])*8/seconds/1e6 if seconds>0 else None)
        if direction == "SEND":
            values.update(self.nic_sample())
        names = list(values)
        self.db.execute("UPDATE events SET " + ",".join(n+"=?" for n in names) +
                        " WHERE round_id=? AND direction=?",
                        [values[n] for n in names]+[round_id,direction])

    def nic_sample(self):
        if not self.interface:
            return {}
        try:
            base = Path("/sys/class/net") / self.interface / "statistics"
            rx, tx = [int((base/n).read_text()) for n in ("rx_bytes","tx_bytes")]
        except (OSError, ValueError):
            return {}
        now = time.monotonic()
        values = {"nic_interface": self.interface, "nic_rx_bytes": rx, "nic_tx_bytes": tx}
        previous = self.nic_previous
        self.nic_previous = (now, rx, tx)
        if previous and now>previous[0] and rx>=previous[1] and tx>=previous[2]:
            elapsed = now-previous[0]
            values.update(nic_interval_seconds=elapsed,
                          nic_rx_mbps=(rx-previous[1])*8/elapsed/1e6,
                          nic_tx_mbps=(tx-previous[2])*8/elapsed/1e6)
        return values


    def export_day(self, day, rebuild=False):
        rebuild = rebuild or day in self.csv_dirty
        logs = self.root / "logs"
        logs.mkdir(exist_ok=True)
        path = logs / f"node-{self.node}_{day}.csv"
        after = 0 if rebuild else self.csv_counts.get(day, 0)
        rows = list(self.db.execute("SELECT * FROM events WHERE hour_bucket=? AND event_id>? ORDER BY event_id", (day, after)))
        if not rebuild and not rows:
            return
        target = path.with_suffix(".csv.tmp") if rebuild else path
        exists = target.exists() and target.stat().st_size > 0
        self.csv_dirty.add(day)
        with target.open("w" if rebuild else "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
            if rebuild or not exists:
                writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
            f.flush()
            os.fsync(f.fileno())
        if rebuild:
            os.replace(target, path)
        sync_dir(logs)
        if rows:
            self.csv_counts[day] = rows[-1]["event_id"]
        self.csv_dirty.discard(day)

    def export(self):
        # Only export days containing newly committed events during normal operation.
        last = max(self.csv_counts.values(), default=0)
        days = set(self.csv_dirty)
        days.update(row[0] for row in self.db.execute("SELECT DISTINCT hour_bucket FROM events WHERE event_id>?", (last,)))
        for day in sorted(days):
            self.export_day(day)

    def choose(self):
        manifest = json.loads((self.state / f"dataset-{self.node}.json").read_text())
        item = random.SystemRandom().choice(manifest["files"])
        data = (self.root / "images" / item["name"]).read_bytes()
        if digest(data) != item["sha256"]:
            raise ValueError("An original image changed; run prepare to repair it")
        return {"name": item["name"], "sha256": digest(data), "bytes": len(data),
                "data": base64.b64encode(data).decode("ascii")}

    def receive(self, payload, peer):
        name = payload["name"]
        if not re.fullmatch(r"[AB]_cifar_[0-9]{5}_[a-z]+\.png", name) or not name.startswith(peer + "_"):
            raise ValueError("Invalid image filename")
        if payload["bytes"] > 1024 * 1024:
            raise ValueError("Image exceeds size limit")
        data = base64.b64decode(payload["data"], validate=True)
        if len(data) != payload["bytes"] or digest(data) != payload["sha256"]:
            raise ValueError("Image checksum mismatch")
        path = self.root / "received" / peer / name
        # Fully rewrite even repeats: each logged receive corresponds to actual bytes exchanged.
        atomic_write(path, data)
        return path

    def pending(self):
        row = self.db.execute("SELECT * FROM rounds WHERE phase!='done' ORDER BY created LIMIT 1").fetchone()
        if row:
            return dict(row)
        payload = self.choose()
        row = {"id": uuid.uuid4().hex, "phase": "exchange", "payload": json.dumps(payload), "created": time.time()}
        with self.db:
            self.db.execute("INSERT INTO rounds(id,phase,payload,created) VALUES(?,?,?,?)",
                            (row["id"], row["phase"], row["payload"], row["created"]))
        return row

    def exchange(self, round_id, incoming):
        if not re.fullmatch(r"[0-9a-f]{32}", round_id):
            raise ValueError("Invalid round id")
        row = self.db.execute("SELECT * FROM rounds WHERE id=?", (round_id,)).fetchone()
        if row:
            if row["incoming_hash"] != incoming["sha256"]:
                raise ValueError("A retry changed its payload")
            if not row["payload"]:
                raise ValueError("Old completed exchange was already compacted")
            self.export()
            return json.loads(row["payload"])
        path = self.receive(incoming, "A")
        outgoing = self.choose()
        with self.db:
            self.db.execute("INSERT INTO rounds(id,phase,payload,incoming_hash,created) VALUES(?,?,?,?,?)",
                            (round_id, "ack", json.dumps(outgoing), incoming["sha256"], time.time()))
            self.event(round_id, "RECEIVE", "A", incoming, path)
        self.export()
        return outgoing

    def accept_reply(self, row, reply, metrics=None):
        path = self.receive(reply, "B")
        with self.db:
            self.event(row["id"], "RECEIVE", "B", reply, path)
            self.event(row["id"], "SEND", "B", json.loads(row["payload"]), self.root / "images" / json.loads(row["payload"])["name"], metrics)
            self.db.execute("UPDATE rounds SET phase='ack',incoming_hash=?,metrics=? WHERE id=?", (reply["sha256"], json.dumps(metrics or {}), row["id"]))
        self.export()

    def ack(self, round_id, sha256, metrics=None):
        row = self.db.execute("SELECT * FROM rounds WHERE id=?", (round_id,)).fetchone()
        if not row:
            raise ValueError("Acknowledgement for unknown round")
        outgoing = json.loads(row["payload"]) if row["payload"] else None
        if outgoing and outgoing["sha256"] != sha256:
            raise ValueError("Acknowledgement checksum mismatch")
        if row["phase"] != "done":
            with self.db:
                self.event(round_id, "SEND", "A", outgoing, self.root / "images" / outgoing["name"], metrics)
                self.db.execute("UPDATE rounds SET phase='done' WHERE id=?", (round_id,))
        self.export()
        self.compact()

    def finish(self, round_id):
        with self.db:
            self.db.execute("UPDATE rounds SET phase='done' WHERE id=?", (round_id,))
        self.compact()

    def compact(self):
        # Keep recent completed responses for retries; retain every CSV event in SQLite.
        with self.db:
            self.db.execute("""DELETE FROM rounds WHERE phase='done' AND id NOT IN
              (SELECT id FROM rounds ORDER BY created DESC LIMIT 100)""")


class Channel:
    def __init__(self, command, timeout=600, expected_source=None):
        self.timeout = timeout
        self.p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  text=True, encoding="utf-8", bufsize=1)
        self.lines = queue.Queue()
        def pump():
            try:
                for line in self.p.stdout:
                    self.lines.put(line)
            finally:
                self.lines.put(None)
        self.reader = threading.Thread(target=pump, daemon=True)
        self.reader.start()
        try:
            hello = self.read()
        except Exception:
            self.close()
            raise
        if hello.get("ready") != "B" or hello.get("version") != 1:
            self.close()
            raise RuntimeError("Remote worker handshake mismatch")
        if expected_source and hello.get("source_address") != expected_source:
            self.close()
            raise RuntimeError("Remote sees SSH source %r, expected %r" %
                               (hello.get("source_address"), expected_source))
        self.hello = hello

    def read(self):
        line = self.lines.get(timeout=self.timeout)
        if line is None:
            raise ConnectionError("Remote SSH session closed")
        result = json.loads(line)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result

    def call(self, message):
        self.p.stdin.write(json.dumps(message) + "\n")
        self.p.stdin.flush()
        return self.read()

    def close(self):
        self.p.terminate()
        try:
            self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
        self.reader.join(timeout=5)
        self.p.stdin.close()
        self.p.stdout.close()


def route_interface(peer, source_address=None):
    try:
        command = ["ip", "-j", "route", "get", peer]
        if source_address:
            command.extend(["from", source_address])
        routes = json.loads(subprocess.check_output(command, text=True))
        return routes[0].get("dev")
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return None


def serve(root):
    prepare(root, "B")
    store = Store(root, "B", route_interface(os.environ.get("SSH_CONNECTION", "").split()[0])
                  if os.environ.get("SSH_CONNECTION") else None)
    connection = os.environ.get("SSH_CONNECTION", "").split()
    print(json.dumps({"ready": "B", "version": 1,
                      "source_address": connection[0] if connection else None,
                      "destination_address": connection[2] if len(connection) >= 4 else None}), flush=True)
    try:
        while True:
            line = sys.stdin.readline(MAX_LINE)
            if not line:
                break
            if not line.endswith("\n"):
                raise ValueError("Protocol line too long")
            try:
                message = json.loads(line)
                if message["op"] == "exchange":
                    result = {"image": store.exchange(message["id"], message["image"])}
                elif message["op"] == "ack":
                    store.ack(message["id"], message["sha256"], message.get("metrics"))
                    result = {"acked": message["id"]}
                else:
                    raise ValueError("Unknown operation")
                print(json.dumps(result), flush=True)
            except Exception as e:
                print(json.dumps({"error": str(e)}), flush=True)
    finally:
        store.close()


def run(root, config_path):
    config = json.loads(Path(config_path).read_text())
    transport = config.get('transport', 'ssh')
    if transport not in ('ssh', 'https'):
        raise ValueError('Unsupported image transport')
    if float(config["interval_seconds"]) < 0.1:
        raise ValueError("Interval must be at least 0.1 seconds")
    source_address = config.get("source_address")
    if source_address:
        source_address = str(ipaddress.ip_address(source_address))
    prepare(root, "A")
    peer_host = config.get('peer_host') or config['peer'].rsplit('@', 1)[-1]
    store = Store(root, "A", route_interface(peer_host, source_address))
    command = None
    if transport == 'ssh':
        command = ["ssh", "-T", "-i", str(Path(config["identity_file"]).expanduser()),
                   "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
                   "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
                   *(["-b", source_address] if source_address else []),
                   config["peer"], shlex.join(["python3", config["remote_script"], "serve", "--root", config["remote_root"]])]
    channel = None
    try:
        while True:
            try:
                if channel is None:
                    if transport == 'https':
                        from image_https import HTTPSChannel
                        channel = HTTPSChannel(config)
                    else:
                        channel = Channel(command, expected_source=source_address)
                    print("%s path: %s -> %s" % (transport.upper(), channel.hello.get("source_address"),
                                                  channel.hello.get("destination_address")), flush=True)
                row = store.pending()
                if row["phase"] == "exchange":
                    started = time.monotonic()
                    result = channel.call({"op": "exchange", "id": row["id"], "image": json.loads(row["payload"])})
                    elapsed = time.monotonic()-started
                    size = json.loads(row["payload"])["bytes"]+result["image"]["bytes"]
                    metrics = {"exchange_seconds": elapsed, "exchange_payload_bytes": size,
                               "exchange_payload_mbps": size*8/elapsed/1e6}
                    store.accept_reply(row, result["image"], metrics)
                    row = dict(store.db.execute("SELECT * FROM rounds WHERE id=?", (row["id"],)).fetchone())
                result = channel.call({"op": "ack", "id": row["id"], "sha256": row["incoming_hash"],
                                       "metrics": json.loads(row.get("metrics") or "{}")})
                if result.get("acked") != row["id"]:
                    raise ValueError("Wrong acknowledgement")
                store.finish(row["id"])
                print(f"{dt.datetime.now(KST).isoformat(timespec='seconds')} exchanged round {row['id']}", flush=True)
                time.sleep(float(config["interval_seconds"]))
            except (OSError, ValueError, RuntimeError, queue.Empty) as e:
                print(f"Exchange interrupted: {e}; retry in 15 seconds", file=sys.stderr, flush=True)
                if channel:
                    channel.close()
                    channel = None
                time.sleep(15)
    finally:
        if channel:
            channel.close()
        store.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["prepare", "serve", "run"])
    parser.add_argument("--root", default="/var/lib/ontime-cache-poc")
    parser.add_argument("--node", choices=["A", "B"], default="A")
    parser.add_argument("--config")
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare(args.root, args.node)
    elif args.mode == "serve":
        serve(args.root)
    elif args.config:
        run(args.root, args.config)
    else:
        parser.error("run requires --config")


if __name__ == "__main__":
    main()

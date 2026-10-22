#!/usr/bin/env python3
"""Consistent, read-only SQLite backup and labelled summary of the 0.5 s baseline."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import sqlite3
from experiment_report import export_hourly

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--state',default='/var/lib/ontime-cache-poc/.ontime-cache-poc')
p.add_argument('--output',default=str(Path(__file__).parent/'margin-experiments/baseline-0p5s'))
a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
source=Path(a.state)/'experiments-ack-v3.sqlite3'
src=sqlite3.connect('file:'+str(source)+'?mode=ro',uri=True)
temp=out/'baseline-0p5s.tmp.sqlite3'
dst=sqlite3.connect(temp)
src.backup(dst);src.close()
configs=[json.loads(r[0]) for r in dst.execute('SELECT configuration_json FROM phases')]
if not configs or any(c.get('prefetch_margin')!=.5 for c in configs):
    dst.close();temp.unlink()
    raise SystemExit('Baseline contains missing/different margins; refusing to label it 0.5 seconds')
export_hourly(dst,out/'baseline-0p5s-hourly.csv')
counts=dst.execute('SELECT policy,status,COUNT(*) FROM phases GROUP BY policy,status').fetchall()
dst.close();temp.replace(out/'baseline-0p5s.sqlite3')
metadata=dict(snapshot_at_utc=datetime.now(timezone.utc).isoformat(),prefetch_margin_seconds=.5,
              algorithm_revision='ack-committed-v3',source_database=str(source),
              phase_counts=counts,note='Completed phases are comparable; running/interrupted/error phases must be excluded.')
(out/'baseline-0p5s-metadata.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(metadata,ensure_ascii=False))

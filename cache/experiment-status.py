#!/usr/bin/env python3
"""Wait for a fresh running phase, not just systemd active state."""
import argparse
import json
from pathlib import Path
import time
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--wait',type=int,default=0)
p.add_argument('--state',default='/var/lib/ontime-cache-poc/.ontime-cache-poc')
p.add_argument('--experiment-id')
p.add_argument('--prefetch-margin',type=float)
a=p.parse_args();deadline=time.monotonic()+a.wait
path=Path(a.state)/'experiment-current.json'
while True:
    try:
        result=json.loads(path.read_text())
        age=time.time()-path.stat().st_mtime
        if (a.experiment_id is None or result.get('experiment_id')==a.experiment_id) and (a.prefetch_margin is None or result.get('prefetch_margin_seconds')==a.prefetch_margin) and result.get('algorithm_revision')=='ack-committed-v3' and result.get('prediction_commit')=='after_verified_ack' and result.get('playback_model')=='per-user-video-resume-v2' and result['status']=='running' and age<75 and result['requests']>0 and result['koren_source_verified']:
            print(json.dumps(result,ensure_ascii=False,indent=2));break
    except (OSError,ValueError,KeyError):pass
    if time.monotonic()>=deadline:
        raise SystemExit('No fresh successful comparison metrics yet. Inspect journalctl -u ontime-cache-poc.service')
    time.sleep(1)

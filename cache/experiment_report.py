#!/usr/bin/env python3
"""Export authoritative phase records as UTF-8 BOM CSV (opens in Excel)."""
import argparse
import csv
import json
from pathlib import Path
import sqlite3


def export_hourly(db, path):
    rows=[]
    for ident, ordinal, policy, start, end, status, result, config in db.execute(
            'SELECT id,ordinal,policy,started_utc,ended_utc,status,result_json,configuration_json FROM phases ORDER BY started_utc'):
        row=dict(phase_id=ident,ordinal=ordinal,policy=policy,started_utc=start,ended_utc=end,status=status)
        configuration=json.loads(config)
        row.update(experiment_id=configuration.get('experiment_id','baseline'),
                   prefetch_margin_seconds=configuration.get('prefetch_margin'))
        row.update(json.loads(result) if result else {})
        # DB terminal status is authoritative, including interrupted-process recovery.
        row['status']=status
        rows.append(row)
    if not rows:
        return
    temp=path.with_suffix('.tmp')
    with temp.open('w',encoding='utf-8-sig',newline='') as stream:
        fields=list(dict.fromkeys(k for row in rows for k in row))
        writer=csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader();writer.writerows(rows)
    temp.replace(path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state',default='/var/lib/ontime-cache-poc/.ontime-cache-poc')
    p.add_argument('--experiment-id',default='baseline')
    p.add_argument('--samples',action='store_true')
    a=p.parse_args();state=Path(a.state)
    from experiment import experiment_paths
    db_path,csv_path=experiment_paths(state,a.experiment_id)
    db=sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True)
    export_hourly(db,csv_path)
    if a.samples:
        target=state/('experiment-samples'+('' if a.experiment_id=='baseline' else '-'+a.experiment_id)+'.csv')
        columns=[row[0] for row in db.execute('SELECT metrics_json FROM samples LIMIT 1')]
        if columns:
            fields=list(json.loads(columns[0]))
            with target.open('w',encoding='utf-8-sig',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore')
                writer.writeheader()
                for (payload,) in db.execute('SELECT metrics_json FROM samples ORDER BY rowid'):
                    writer.writerow(json.loads(payload))
    for policy,count in db.execute("SELECT policy,COUNT(*) FROM phases WHERE status='complete' GROUP BY policy"):
        print(policy,count,'completed phases')
    print(csv_path)
    db.close()


if __name__=='__main__':main()

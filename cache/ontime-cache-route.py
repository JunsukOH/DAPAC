#!/usr/bin/env python3
"""Reconcile only the owned source/destination KOREN policy, preserving other rules."""
import fcntl
import ipaddress
import json
import os
import subprocess

SOURCE='192.0.2.20'
DESTINATION='198.51.100.10'
GATEWAY='192.0.2.17'
DEVICE='eth0'
TABLE=PRIORITY=1053


def host(value, expected):
    try:return ipaddress.ip_network(value,strict=False)==ipaddress.ip_network(expected+'/32')
    except (ValueError,TypeError):return False


def plan(rules,routes):
    occupied=[r for r in rules if r.get('priority')==PRIORITY]
    if occupied:
        valid=len(occupied)==1 and host(occupied[0].get('src'),SOURCE) and host(occupied[0].get('dst'),DESTINATION)
        valid=valid and str(occupied[0].get('table'))==str(TABLE)
        valid=valid and not any(k in occupied[0] for k in ('fwmark','iif','oif','ipproto','sport','dport')) and not occupied[0].get('not')
        if not valid:raise ValueError('Priority 1053 belongs to a different policy; no changes made')
    if routes:
        valid=len(routes)==1 and host(routes[0].get('dst'),DESTINATION)
        valid=valid and routes[0].get('gateway')==GATEWAY and routes[0].get('dev')==DEVICE
        valid=valid and routes[0].get('prefsrc') in (None,SOURCE)
        if not valid:raise ValueError('Table 1053 contains a different route; no changes made')
    actions=[]
    if not routes:
        actions.append(['ip','-4','route','add','table',str(TABLE),DESTINATION+'/32',
                        'via',GATEWAY,'dev',DEVICE,'onlink','src',SOURCE])
    if not occupied:
        actions.append(['ip','-4','rule','add','priority',str(PRIORITY),
                        'from',SOURCE+'/32','to',DESTINATION+'/32','table',str(TABLE)])
    return actions


def read_ip(*args,missing_table=False):
    result=subprocess.run(['ip','-j','-4',*args],capture_output=True,text=True)
    if result.returncode:
        if missing_table and 'does not exist' in result.stderr:return []
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout)


def main():
    if os.geteuid()!=0:raise SystemExit('Run with sudo')
    with open('/run/lock/ontime-cache-route.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        addresses=read_ip('addr','show','dev',DEVICE)
        if not any(a.get('local')==SOURCE and a.get('prefixlen')==29 for row in addresses for a in row.get('addr_info',[])):
            raise RuntimeError('Expected KOREN address/interface is missing; no changes made')
        actions=plan(read_ip('rule','show'),read_ip('route','show','table',str(TABLE),missing_table=True))
        for command in actions:subprocess.run(command,check=True)
        route=read_ip('route','get',DESTINATION,'from',SOURCE)[0]
        if route.get('dev')!=DEVICE or route.get('gateway')!=GATEWAY:
            raise RuntimeError('A different policy still overrides the KOREN route; inspect ip rule')
        if actions:print(json.dumps({'repaired':True,'actions':actions,'route':route}),flush=True)


if __name__=='__main__':main()

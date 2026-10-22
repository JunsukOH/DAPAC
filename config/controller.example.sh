#!/usr/bin/env bash
# Documentation addresses only. Start manually after adapting all endpoints,
# source-validation constants and TLS configuration to your own testbed.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
exec python3 "$root/cache/experiment.py" run \
  --origin-host 198.51.100.10 --origin-port 443 \
  --client-host 192.0.2.21 --client-port 8443 --origin-bind 192.0.2.20 \
  --origin-ca /etc/ontime-cache-poc/origin/ca.crt \
  --origin-cert /etc/ontime-cache-poc/origin/client.crt \
  --origin-key /etc/ontime-cache-poc/origin/client.key \
  --client-ca /etc/ontime-cache-poc/receiver/ca.crt \
  --client-cert /etc/ontime-cache-poc/receiver/client.crt \
  --client-key /etc/ontime-cache-poc/receiver/client.key \
  --state /var/lib/ontime-cache-poc \
  --phase-seconds 3600 --sample-seconds 60 \
  --users 8 --streams 8 --size 2097152 --cache-mib 128 --ttl 30 \
  --mbps 100 --workload-mbps 110 --seed 42 \
  --prefetch-margin 1 --experiment-id margin-1s "$@"

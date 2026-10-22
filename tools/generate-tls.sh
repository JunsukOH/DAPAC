#!/usr/bin/env bash
set -euo pipefail
umask 077
client_dir=${1:-/opt/ontime-cache-poc/work/image-relay-https/client-tls}
server_dir=${2:-/opt/ontime-cache-poc/work/image-relay-https/server-tls}
server_ip=${3:-198.51.100.10}
python3 - "$server_ip" <<'PY'
import ipaddress,sys
ipaddress.ip_address(sys.argv[1])
PY
mkdir -p "$client_dir" "$server_dir"
chmod 700 "$client_dir" "$server_dir"
if [[ -s $client_dir/ca.crt && -s $client_dir/client.crt && -s $client_dir/client.key &&
      -s $server_dir/server.crt && -s $server_dir/server.key ]] &&
   openssl x509 -checkend 604800 -noout -in "$client_dir/client.crt" >/dev/null &&
   openssl x509 -checkend 604800 -noout -in "$server_dir/server.crt" >/dev/null; then
    echo 'Existing TLS certificates reused.'
    exit 0
fi
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 365 \
    -subj '/CN=image-relay-private-ca' -keyout "$scratch/ca.key" -out "$scratch/ca.crt" 2>"$scratch/openssl.log"
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=image-relay-A' \
    -keyout "$scratch/client.key" -out "$scratch/client.csr" 2>>"$scratch/openssl.log"
cat > "$scratch/client.ext" <<'EXT'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectAltName=IP:192.0.2.20,IP:127.0.0.1
EXT
openssl x509 -req -in "$scratch/client.csr" -CA "$scratch/ca.crt" -CAkey "$scratch/ca.key" \
    -set_serial 2 -days 365 -sha256 -extfile "$scratch/client.ext" -out "$scratch/client.crt" 2>>"$scratch/openssl.log"
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=image-relay-B' \
    -keyout "$scratch/server.key" -out "$scratch/server.csr" 2>>"$scratch/openssl.log"
cat > "$scratch/server.ext" <<EXT
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=IP:$server_ip,IP:127.0.0.1
EXT
openssl x509 -req -in "$scratch/server.csr" -CA "$scratch/ca.crt" -CAkey "$scratch/ca.key" \
    -set_serial 3 -days 365 -sha256 -extfile "$scratch/server.ext" -out "$scratch/server.crt" 2>>"$scratch/openssl.log"
install -m 644 "$scratch/ca.crt" "$client_dir/ca.crt"
install -m 644 "$scratch/client.crt" "$client_dir/client.crt"
install -m 600 "$scratch/client.key" "$client_dir/client.key"
install -m 644 "$scratch/ca.crt" "$server_dir/ca.crt"
install -m 644 "$scratch/server.crt" "$server_dir/server.crt"
install -m 600 "$scratch/server.key" "$server_dir/server.key"
echo 'Dedicated client/server TLS certificates created; issuer private key discarded.'

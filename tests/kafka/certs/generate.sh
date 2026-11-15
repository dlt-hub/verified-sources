#!/usr/bin/env bash
# self-signed CA, broker and client certificates for the SSL listener of docker-compose.yml
set -euo pipefail
cd "$(dirname "$0")"
[ -f ca.pem ] && [ -f server.pem ] && [ -f client.pem ] && exit 0

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=dlt-test-ca" \
  -keyout ca.key -out ca.pem 2>/dev/null

for name in server client; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" 2>/dev/null
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") -out "$name.crt" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$name.key" -out "$name.pk8"
  cat "$name.pk8" "$name.crt" > "$name.pem"
done
chmod 644 ./*.pem ./*.pk8 ./*.crt
rm -f ./*.csr ca.srl

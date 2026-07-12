#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
profile="$root/deploy/reference-runtime/topology-profiles.json"
destination=${1:-"$root/.tmp/k3d"}

platform=$(uname -s | tr '[:upper:]' '[:lower:]')
architecture=$(uname -m)
case "$architecture" in
  x86_64) architecture=amd64 ;;
  aarch64) architecture=arm64 ;;
esac
asset="${platform}-${architecture}"

metadata=$(python - "$profile" "$asset" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
profiles = document.get("profiles")
if document.get("contractVersion") != 1 or not isinstance(profiles, list):
    raise SystemExit("topology profile is invalid")
if len(profiles) != 1:
    raise SystemExit("exactly one topology profile is required")
profile = profiles[0]
checksum = profile.get("k3dChecksums", {}).get(sys.argv[2])
version = profile.get("k3dVersion")
if not isinstance(checksum, str) or len(checksum) != 64:
    raise SystemExit("k3d is unsupported on this platform")
if not isinstance(version, str) or not version.startswith("v"):
    raise SystemExit("k3d version is invalid")
print(version)
print(checksum)
PY
)
version=$(printf '%s\n' "$metadata" | sed -n '1p')
expected=$(printf '%s\n' "$metadata" | sed -n '2p')

workdir=$(mktemp -d "${TMPDIR:-/tmp}/prometa-k3d.XXXXXX")
trap 'rm -rf "$workdir"' EXIT HUP INT TERM
binary="$workdir/k3d"
url="https://github.com/k3d-io/k3d/releases/download/${version}/k3d-${platform}-${architecture}"
curl --fail --location --silent --show-error "$url" --output "$binary"

actual=$(python - "$binary" <<'PY'
import hashlib
import sys

digest = hashlib.sha256()
with open(sys.argv[1], "rb") as stream:
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(block)
print(digest.hexdigest())
PY
)
if [ "$actual" != "$expected" ]; then
  echo "k3d checksum verification failed" >&2
  exit 2
fi

mkdir -p "$(dirname -- "$destination")"
chmod 0755 "$binary"
mv "$binary" "$destination"
"$destination" version

#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
python_command=${PYTHON:-python}
baseline_manifest="$root/deploy/reference-runtime/compatibility-baselines.json"

baseline_ref=$(
  "$python_command" - "$baseline_manifest" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
if document.get("contractVersion") != 1 or len(document.get("baselines", [])) != 1:
    raise SystemExit("compatibility baseline manifest is invalid")
baseline = document["baselines"][0]
if baseline.get("artifactStatus") != "source-baseline-not-published-release":
    raise SystemExit("compatibility baseline status is invalid")
print(baseline["gitRef"])
PY
)

git -C "$root" cat-file -e "${baseline_ref}^{commit}"
workdir=$(mktemp -d "${TMPDIR:-/tmp}/prometa-runtime-upgrade.XXXXXX")
trap 'rm -rf "$workdir"' EXIT HUP INT TERM

mkdir -p "$workdir/baseline"
git -C "$root" archive --format=tar --output="$workdir/baseline.tar" "$baseline_ref"
tar -xf "$workdir/baseline.tar" -C "$workdir/baseline"

PROMETA_RUNTIME_UPGRADE_BASELINE="$workdir/baseline" \
PROMETA_RUNTIME_UPGRADE_BASELINE_REF="$baseline_ref" \
  "$python_command" -m pytest -q "$root/tests/test_runtime_upgrade_rollback.py"

#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 CLUSTER SERVER_NODES AGENT_NODES IMAGE [IMAGE ...]" >&2
  exit 2
fi

cluster=$1
server_nodes=$2
agent_nodes=$3
shift 3
images=("$@")

k3d_command=${K3D:-k3d}
docker_command=${DOCKER:-docker}
python_command=${PYTHON:-python3}
attempts=${PROMETA_K3D_IMAGE_IMPORT_ATTEMPTS:-3}
retry_seconds=${PROMETA_K3D_IMAGE_IMPORT_RETRY_SECONDS:-2}
import_timeout_seconds=${PROMETA_K3D_IMAGE_IMPORT_TIMEOUT_SECONDS:-120}

for value in "$server_nodes" "$agent_nodes" "$attempts" "$retry_seconds" \
  "$import_timeout_seconds"; do
  case "$value" in
  ''|*[!0-9]*)
    echo "Node counts, attempts, retry delay, and import timeout must be non-negative integers." >&2
    exit 2
    ;;
  esac
done
if [ "$server_nodes" -eq 0 ] || [ "$attempts" -eq 0 ] || \
   [ "$import_timeout_seconds" -eq 0 ]; then
  echo "At least one server node, one import attempt, and a positive import timeout are required." >&2
  exit 2
fi

if ! command -v "$python_command" >/dev/null 2>&1; then
  echo "Python is required to enforce the K3d image-import deadline." >&2
  exit 2
fi

nodes=()
for ((index = 0; index < server_nodes; index++)); do
  nodes+=("k3d-$cluster-server-$index")
done
for ((index = 0; index < agent_nodes; index++)); do
  nodes+=("k3d-$cluster-agent-$index")
done

node_has_image() {
  local node=$1
  local image=$2
  local reference

  while IFS= read -r reference; do
    case "$reference" in
      "$image"|"docker.io/library/$image") return 0 ;;
    esac
  done < <(
    "$docker_command" exec "$node" \
      ctr --namespace k8s.io images list -q 2>/dev/null
  )
  return 1
}

verify_images() {
  local node
  local image
  missing=()

  for node in "${nodes[@]}"; do
    for image in "${images[@]}"; do
      if ! node_has_image "$node" "$image"; then
        missing+=("$node=$image")
      fi
    done
  done
  [ "${#missing[@]}" -eq 0 ]
}

run_import_with_deadline() {
  "$python_command" - "$import_timeout_seconds" "$k3d_command" \
    image import --mode direct --cluster "$cluster" "${images[@]}" <<'PY'
import os
import signal
import subprocess
import sys

timeout_seconds = int(sys.argv[1])
command = sys.argv[2:]
process = subprocess.Popen(command, start_new_session=True)
try:
    raise SystemExit(process.wait(timeout=timeout_seconds))
except subprocess.TimeoutExpired:
    print(
        f"K3d image import exceeded {timeout_seconds} seconds; terminating it.",
        file=sys.stderr,
    )
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    raise SystemExit(124)
PY
}

for ((attempt = 1; attempt <= attempts; attempt++)); do
  import_status=0
  if run_import_with_deadline; then
    import_status=0
  else
    import_status=$?
  fi

  if verify_images; then
    exit 0
  fi

  printf 'Verified K3d image import attempt %d/%d incomplete (status=%d; missing=%s).\n' \
    "$attempt" "$attempts" "$import_status" "${missing[*]}" >&2
  if [ "$attempt" -lt "$attempts" ]; then
    sleep "$retry_seconds"
  fi
done

echo "K3d image import did not converge after $attempts attempts." >&2
exit 2

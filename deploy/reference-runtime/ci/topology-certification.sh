#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
python_command=${PYTHON:-python}
kubectl_command=${KUBECTL:-kubectl}
helm_command=${HELM:-helm}
k3d_command=${K3D:-k3d}
profile="$root/deploy/reference-runtime/topology-profiles.json"
fixture="$root/deploy/reference-runtime/ci/topology_fixture.py"
probe_source="$root/deploy/reference-runtime/ci/topology_probe.py"
chart="$root/deploy/reference-runtime/chart"
runtime_image=${PROMETA_RUNTIME_TOPOLOGY_IMAGE:-prometa-runtime-host:topology-cert}
cluster=${PROMETA_RUNTIME_TOPOLOGY_CLUSTER:-prometa-runtime-topology}
report=${PROMETA_RUNTIME_TOPOLOGY_REPORT:-"$root/runtime-topology-certification.json"}
keep_cluster=${PROMETA_RUNTIME_KEEP_TOPOLOGY_CLUSTER:-false}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/prometa-runtime-topology.XXXXXX")
assets="$workdir/assets"
kubeconfig="$workdir/kubeconfig"
cluster_created=false

profile_value() {
  "$python_command" "$fixture" profile-value --profile "$profile" --key "$1"
}

diagnostics() {
  if [ "$cluster_created" != true ]; then
    return
  fi
  echo "Topology certification failed; collecting payload-free diagnostics." >&2
  KUBECONFIG="$kubeconfig" "$kubectl_command" get nodes -o wide >&2 || true
  KUBECONFIG="$kubeconfig" "$kubectl_command" get pods --all-namespaces -o wide >&2 || true
  KUBECONFIG="$kubeconfig" "$kubectl_command" get events --all-namespaces \
    --sort-by=.metadata.creationTimestamp >&2 || true
  for namespace in runtime-a runtime-b; do
    KUBECONFIG="$kubeconfig" "$kubectl_command" logs -n "$namespace" \
      -l app.kubernetes.io/component=runtime --all-containers=true \
      --prefix=true --tail=120 >&2 || true
  done
}

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ "$status" -ne 0 ]; then
    diagnostics
  fi
  if [ "$cluster_created" = true ] && [ "$keep_cluster" != true ]; then
    "$k3d_command" cluster delete "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$workdir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

require_command() {
  if ! command -v "$1" >/dev/null 2>&1 && [ ! -x "$1" ]; then
    echo "Required command is unavailable: $1" >&2
    exit 2
  fi
}

verify_image_digest() {
  image=$1
  expected=$2
  docker pull "$image" >/dev/null
  docker image inspect "$image" | "$python_command" -c '
import json, sys
document = json.load(sys.stdin)
digests = {
    item.rsplit("@", 1)[-1]
    for image in document
    for item in image.get("RepoDigests", [])
    if "@" in item
}
if sys.argv[1] not in digests:
    raise SystemExit("container image digest mismatch")
' "$expected"
}

probe() {
  namespace=$1
  pod=$2
  shift 2
  KUBECONFIG="$kubeconfig" "$kubectl_command" exec -n "$namespace" "$pod" -- \
    python /opt/topology/topology_probe.py "$@"
}

wait_socket_policy() {
  namespace=$1
  pod=$2
  host=$3
  port=$4
  expected=$5
  output=
  for _ in {1..20}; do
    if output=$(probe "$namespace" "$pod" socket \
      --host "$host" --port "$port" --expect "$expected" --timeout 2 2>&1); then
      printf '%s\n' "$output"
      return
    fi
    sleep 1
  done
  printf '%s\n' "$output" >&2
  return 2
}

model_count() {
  tenant=$1
  output=$(probe "gateway-$tenant" probe model-count \
    --url "http://model-gateway.models-$tenant.svc.cluster.local:8000/count")
  printf '%s\n' "$output" | "$python_command" -c \
    'import json,sys; value=json.load(sys.stdin); assert value.get("passed") is True; print(value["count"])'
}

capture_pods() {
  tenant=$1
  destination=$2
  KUBECONFIG="$kubeconfig" "$kubectl_command" get pods -n "runtime-$tenant" \
    -l app.kubernetes.io/component=runtime -o json >"$destination"
}

capture_logs() {
  tenant=$1
  pods_json=$2
  output=$3
  expected_created=$4
  expected_joined=$5
  replacement_only=${6:-false}
  name_args=(pod-names --input "$pods_json")
  if [ "$replacement_only" = true ]; then
    name_args+=(--replacement-only)
  fi
  names=$("$python_command" "$fixture" "${name_args[@]}")
  inputs=()
  for pod in $names; do
    log="$workdir/${tenant}-${pod}.log"
    KUBECONFIG="$kubeconfig" "$kubectl_command" logs -n "runtime-$tenant" "$pod" >"$log"
    inputs+=(--input "$log")
  done
  "$python_command" "$fixture" inspect-host-logs \
    "${inputs[@]}" \
    --output "$output" \
    --expected-created "$expected_created" \
    --expected-joined "$expected_joined"
}

database_scalar() {
  tenant=$1
  query=$2
  KUBECONFIG="$kubeconfig" "$kubectl_command" exec -n "data-$tenant" \
    deployment/postgres -- psql -X -v ON_ERROR_STOP=1 -U runtime -d runtime \
    -tAc "$query" | tr -d '[:space:]'
}

verify_node_image() {
  node=$1
  image=$2
  found=false
  while IFS= read -r reference; do
    case "$reference" in
      "$image"|"docker.io/library/$image") found=true ;;
    esac
  done < <(docker exec "$node" ctr --namespace k8s.io images list -q)
  if [ "$found" != true ]; then
    echo "Imported image is absent from node $node: $image" >&2
    exit 2
  fi
}

for required in docker "$python_command" "$kubectl_command" "$helm_command" "$k3d_command"; do
  require_command "$required"
done

if [ "$(profile_value evidenceStatus)" != "reference-profile-not-production-certification" ]; then
  echo "Topology evidence status must remain explicitly non-production." >&2
  exit 2
fi
if [ "$(profile_value networkPolicyController)" != "k3s-kube-router" ]; then
  echo "Unexpected NetworkPolicy controller in topology profile." >&2
  exit 2
fi

k3s_image=$(profile_value k3sImage)
k3s_digest=$(profile_value k3sImageDigest)
postgres_image=$(profile_value postgresImage)
postgres_digest=$(profile_value postgresImageDigest)
postgres_node_image=$(profile_value postgresNodeImage)
server_nodes=$(profile_value serverNodes)
agent_nodes=$(profile_value agentNodes)
replicas=$(profile_value runtimeReplicasPerTenant)
load_requests=$(profile_value uniqueLoadRequestsPerTenant)
duplicate_attempts=$(profile_value duplicateAttemptsPerTenant)
runtime_version=$(
  "$python_command" -c 'import prometa; print(prometa.__version__)'
)
expected_runtime_version=$(profile_value runtimeVersion)
expected_chart_version=$(profile_value chartVersion)
actual_chart_version=$(
  "$helm_command" show chart "$chart" | awk '$1 == "version:" {print $2; exit}'
)
if [ "$runtime_version" != "$expected_runtime_version" ]; then
  echo "Installed runtime version does not match the topology profile." >&2
  exit 2
fi
if [ "$actual_chart_version" != "$expected_chart_version" ]; then
  echo "Runtime chart version does not match the topology profile." >&2
  exit 2
fi

verify_image_digest "$k3s_image" "$k3s_digest"
verify_image_digest "$postgres_image" "$postgres_digest"

docker build --provenance=false --load \
  -f "$root/deploy/reference-runtime/Dockerfile" \
  -t "$runtime_image" "$root"
printf 'FROM %s@%s\n' "$postgres_image" "$postgres_digest" | \
  docker build --provenance=false --load -t "$postgres_node_image" -

"$python_command" "$fixture" prepare \
  --profile "$profile" \
  --output-dir "$assets" \
  --probe-source "$probe_source" \
  --runtime-image "$runtime_image" \
  --runtime-version "$runtime_version"

"$k3d_command" cluster create "$cluster" \
  --servers "$server_nodes" \
  --agents "$agent_nodes" \
  --image "$k3s_image" \
  --k3s-arg '--disable=traefik@server:*' \
  --k3s-arg '--disable=servicelb@server:*' \
  --kubeconfig-update-default=false \
  --kubeconfig-switch-context=false \
  --wait \
  --timeout 240s
cluster_created=true
"$k3d_command" kubeconfig get "$cluster" >"$kubeconfig"
chmod 0600 "$kubeconfig"

"$k3d_command" image import --cluster "$cluster" \
  "$runtime_image" "$postgres_node_image"
for ((index = 0; index < server_nodes; index++)); do
  verify_node_image "k3d-$cluster-server-$index" "$runtime_image"
  verify_node_image "k3d-$cluster-server-$index" "$postgres_node_image"
done
for ((index = 0; index < agent_nodes; index++)); do
  verify_node_image "k3d-$cluster-agent-$index" "$runtime_image"
  verify_node_image "k3d-$cluster-agent-$index" "$postgres_node_image"
done
KUBECONFIG="$kubeconfig" "$kubectl_command" wait --for=condition=Ready nodes \
  --all --timeout=180s
KUBECONFIG="$kubeconfig" "$kubectl_command" rollout status \
  -n kube-system deployment/coredns --timeout=180s
KUBECONFIG="$kubeconfig" "$kubectl_command" apply -f "$assets/support-resources.json"

for tenant in a b; do
  KUBECONFIG="$kubeconfig" "$kubectl_command" rollout status \
    -n "data-$tenant" deployment/postgres --timeout=180s
  KUBECONFIG="$kubeconfig" "$kubectl_command" rollout status \
    -n "models-$tenant" deployment/model-gateway --timeout=180s
  KUBECONFIG="$kubeconfig" "$kubectl_command" wait --for=condition=Ready \
    -n "gateway-$tenant" pod/probe pod/rogue --timeout=180s

  KUBECONFIG="$kubeconfig" "$kubectl_command" create secret generic runtime-release \
    -n "runtime-$tenant" \
    --from-file="config.json=$assets/tenant-$tenant-config.json"
  KUBECONFIG="$kubeconfig" "$kubectl_command" create secret generic runtime-credentials \
    -n "runtime-$tenant" \
    --from-env-file="$assets/tenant-$tenant-credentials.env"

  KUBECONFIG="$kubeconfig" "$helm_command" upgrade --install runtime "$chart" \
    --namespace "runtime-$tenant" \
    --values "$assets/tenant-$tenant-values.json" \
    --wait --timeout 5m

  capture_pods "$tenant" "$workdir/pods-$tenant-initial.json"
  "$python_command" "$fixture" inspect-pods \
    --input "$workdir/pods-$tenant-initial.json" \
    --output "$workdir/pods-$tenant-inspected.json" \
    --expected-replicas "$replicas"
  capture_logs "$tenant" "$workdir/pods-$tenant-inspected.json" \
    "$workdir/activations-$tenant.json" 1 1
done

service_a=http://runtime.runtime-a.svc.cluster.local:8080
service_b=http://runtime.runtime-b.svc.cluster.local:8080
urls_a=$(
  "$python_command" "$fixture" pod-urls --input "$workdir/pods-a-inspected.json"
)
urls_b=$(
  "$python_command" "$fixture" pod-urls --input "$workdir/pods-b-inspected.json"
)

IFS=, read -r -a direct_a <<<"$urls_a"
IFS=, read -r -a direct_b <<<"$urls_b"
for index in "${!direct_a[@]}"; do
  probe gateway-a probe request --url "${direct_a[$index]}" \
    --request-id "direct-a-$index" --expect-answer tenant-a
  probe gateway-b probe request --url "${direct_b[$index]}" \
    --request-id "direct-b-$index" --expect-answer tenant-b
done

probe gateway-a probe load --urls "$service_a" --prefix unique-a \
  --requests "$load_requests" --concurrency 8 --expect-answer tenant-a
probe gateway-b probe load --urls "$service_b" --prefix unique-b \
  --requests "$load_requests" --concurrency 8 --expect-answer tenant-b

probe gateway-b probe blocked-request --url "$service_a" --timeout 2
probe gateway-a probe blocked-request --url "$service_b" --timeout 2
probe gateway-a rogue blocked-request --url "$service_a" --timeout 2
probe gateway-b rogue blocked-request --url "$service_b" --timeout 2

runtime_pod_a=$(
  "$python_command" "$fixture" pod-names \
    --input "$workdir/pods-a-inspected.json" | awk '{print $1}'
)
runtime_pod_b=$(
  "$python_command" "$fixture" pod-names \
    --input "$workdir/pods-b-inspected.json" | awk '{print $1}'
)
probe runtime-a "$runtime_pod_a" socket \
  --host postgres.data-a.svc.cluster.local --port 5432 --expect allowed
probe runtime-a "$runtime_pod_a" socket \
  --host model-gateway.models-a.svc.cluster.local --port 8000 --expect allowed
probe runtime-a "$runtime_pod_a" socket \
  --host postgres.data-b.svc.cluster.local --port 5432 --expect denied
probe runtime-a "$runtime_pod_a" socket \
  --host model-gateway.models-b.svc.cluster.local --port 8000 --expect denied
probe runtime-b "$runtime_pod_b" socket \
  --host postgres.data-b.svc.cluster.local --port 5432 --expect allowed
probe runtime-b "$runtime_pod_b" socket \
  --host model-gateway.models-b.svc.cluster.local --port 8000 --expect allowed
probe runtime-b "$runtime_pod_b" socket \
  --host postgres.data-a.svc.cluster.local --port 5432 --expect denied
probe runtime-b "$runtime_pod_b" socket \
  --host model-gateway.models-a.svc.cluster.local --port 8000 --expect denied

count_before=$(model_count a)
probe gateway-a probe duplicates --urls "$urls_a" \
  --request-id duplicate-a --attempts "$duplicate_attempts" --expect-answer tenant-a
count_after=$(model_count a)
test "$((count_after - count_before))" -eq 1

count_before=$(model_count b)
probe gateway-b probe duplicates --urls "$urls_b" \
  --request-id duplicate-b --attempts "$duplicate_attempts" --expect-answer tenant-b
count_after=$(model_count b)
test "$((count_after - count_before))" -eq 1

KUBECONFIG="$kubeconfig" "$kubectl_command" get networkpolicy runtime \
  -n runtime-a -o json >"$workdir/runtime-a-policy.json"
"$python_command" "$fixture" partition-policy \
  --input "$workdir/runtime-a-policy.json" \
  --original-output "$workdir/runtime-a-policy-original.json" \
  --partition-output "$workdir/runtime-a-policy-partition.json"

count_before=$(model_count a)
KUBECONFIG="$kubeconfig" "$kubectl_command" apply \
  -f "$workdir/runtime-a-policy-partition.json"
wait_socket_policy runtime-a "$runtime_pod_a" \
  postgres.data-a.svc.cluster.local 5432 denied
probe gateway-a probe request --url "$service_a" \
  --request-id database-partition-a --expect-status 503 \
  --expect-error task_store_unavailable --timeout 12
test "$(model_count a)" -eq "$count_before"
probe gateway-b probe request --url "$service_b" \
  --request-id database-partition-control-b --expect-answer tenant-b

KUBECONFIG="$kubeconfig" "$kubectl_command" apply \
  -f "$workdir/runtime-a-policy-original.json"
wait_socket_policy runtime-a "$runtime_pod_a" \
  postgres.data-a.svc.cluster.local 5432 allowed
probe gateway-a probe request --url "$service_a" \
  --request-id database-partition-a --expect-answer tenant-a
test "$(model_count a)" -eq "$((count_before + 1))"

survivor_request=pod-replacement-survivor-a
probe gateway-a probe request --url "$service_a" \
  --request-id "$survivor_request" --expect-answer tenant-a
victim=$(
  "$python_command" "$fixture" pod-names \
    --input "$workdir/pods-a-inspected.json" | awk '{print $1}'
)
KUBECONFIG="$kubeconfig" "$kubectl_command" delete pod -n runtime-a "$victim" \
  --wait=false
probe gateway-a probe load --urls "$service_a" --prefix replacement-a \
  --requests 12 --concurrency 6 --expect-answer tenant-a
KUBECONFIG="$kubeconfig" "$kubectl_command" rollout status -n runtime-a \
  deployment/runtime --timeout=180s

capture_pods a "$workdir/pods-a-replaced.json"
"$python_command" "$fixture" inspect-pods \
  --input "$workdir/pods-a-replaced.json" \
  --output "$workdir/pods-a-replaced-inspected.json" \
  --expected-replicas "$replicas" \
  --previous "$workdir/pods-a-inspected.json"
capture_logs a "$workdir/pods-a-replaced-inspected.json" \
  "$workdir/replacement-activation-a.json" 0 1 true
probe gateway-a probe task-status --url "$service_a" \
  --request-id "$survivor_request" --expect-status completed

activation_a=$(database_scalar a \
  "SELECT COUNT(*) FROM prometa_runtime_release_activation;")
activation_b=$(database_scalar b \
  "SELECT COUNT(*) FROM prometa_runtime_release_activation;")
foreign_a=$(database_scalar a \
  "SELECT COUNT(*) FROM prometa_runtime_release_activation WHERE tenant_id <> 'tenant-topology-a';")
foreign_b=$(database_scalar b \
  "SELECT COUNT(*) FROM prometa_runtime_release_activation WHERE tenant_id <> 'tenant-topology-b';")
test "$activation_a" -eq 1
test "$activation_b" -eq 1
test "$foreign_a" -eq 0
test "$foreign_b" -eq 0

node_count_a=$(
  "$python_command" "$fixture" json-value \
    --input "$workdir/pods-a-replaced-inspected.json" --key nodeCount
)
node_count_b=$(
  "$python_command" "$fixture" json-value \
    --input "$workdir/pods-b-inspected.json" --key nodeCount
)
kubernetes_version=$(
  KUBECONFIG="$kubeconfig" "$kubectl_command" version -o json | \
    "$python_command" -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])'
)

mkdir -p "$(dirname -- "$report")"
"$python_command" "$fixture" report \
  --profile "$profile" \
  --output "$report" \
  --kubernetes-version "$kubernetes_version" \
  --activation-count-a "$activation_a" \
  --activation-count-b "$activation_b" \
  --node-count-a "$node_count_a" \
  --node-count-b "$node_count_b"

echo "Tenant runtime topology certification passed: $report"

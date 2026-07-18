#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
python_command=${PYTHON:-python}
kubectl_command=${KUBECTL:-kubectl}
helm_command=${HELM:-helm}
k3d_command=${K3D:-k3d}
fixture="$root/deploy/reference-runtime/ci/published_upgrade_fixture.py"
cluster=${PROMETA_RUNTIME_PUBLISHED_UPGRADE_CLUSTER:-runtime-published-upgrade}
baseline_descriptor=${PROMETA_RUNTIME_PUBLISHED_BASELINE_DESCRIPTOR:-}
target_descriptor=${PROMETA_RUNTIME_PUBLISHED_TARGET_DESCRIPTOR:-}
report=${PROMETA_RUNTIME_PUBLISHED_UPGRADE_REPORT:-"$root/runtime-published-upgrade-rollback.json"}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/prometa-runtime-published-upgrade.XXXXXX")
kubeconfig="$workdir/kubeconfig"
assets="$workdir/assets"
observations="$workdir/observations.tsv"
trap 'rm -rf "$workdir"' EXIT HUP INT TERM

for command in "$python_command" "$kubectl_command" "$helm_command" "$k3d_command"; do
  if ! command -v "$command" >/dev/null 2>&1 && [ ! -x "$command" ]; then
    echo "Required command is unavailable: $command" >&2
    exit 2
  fi
done
for descriptor in "$baseline_descriptor" "$target_descriptor"; do
  if [ ! -f "$descriptor" ]; then
    echo "Published release descriptor is unavailable: ${descriptor:-<empty>}" >&2
    exit 2
  fi
done
if ! "$k3d_command" cluster list -o json | "$python_command" -c '
import json, sys
name = sys.argv[1]
clusters = json.load(sys.stdin)
raise SystemExit(0 if any(cluster.get("name") == name for cluster in clusters) else 1)
' "$cluster"; then
  echo "Prepared K3d cluster is unavailable: $cluster" >&2
  exit 2
fi

json_value() {
  "$python_command" -c '
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    value = json.load(stream).get(sys.argv[2])
if not isinstance(value, (str, int)) or isinstance(value, bool):
    raise SystemExit("descriptor value is invalid")
print(value)
' "$1" "$2"
}

baseline_version=$(json_value "$baseline_descriptor" runtimeVersion)
target_version=$(json_value "$target_descriptor" runtimeVersion)
baseline_image=$(json_value "$baseline_descriptor" runtimeImage)
target_image=$(json_value "$target_descriptor" runtimeImage)
baseline_chart=$(json_value "$baseline_descriptor" chartPackage)
target_chart=$(json_value "$target_descriptor" chartPackage)
baseline_chart_version=$(json_value "$baseline_descriptor" chartVersion)
target_chart_version=$(json_value "$target_descriptor" chartVersion)

"$python_command" "$fixture" prepare \
  --output-dir "$assets" \
  --baseline-version "$baseline_version" \
  --target-version "$target_version" \
  --replicas 2
"$k3d_command" kubeconfig get "$cluster" >"$kubeconfig"
chmod 0600 "$kubeconfig"
: >"$observations"

sequence_value() {
  tenant=$1
  stage=$2
  key=$3
  "$python_command" -c '
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    sequence = json.load(stream)
for tenant in sequence["tenants"]:
    if tenant["tenant"] != sys.argv[2]:
        continue
    for stage in tenant["stages"]:
        if stage["stage"] == sys.argv[3]:
            value = stage[sys.argv[4]]
            if not isinstance(value, (str, int)) or isinstance(value, bool):
                raise SystemExit("sequence value is invalid")
            print(value)
            raise SystemExit(0)
raise SystemExit("sequence value is missing")
' "$assets/sequence.json" "$tenant" "$stage" "$key"
}

database_activation() {
  tenant=$1
  deployment_id=$2
  KUBECONFIG="$kubeconfig" "$kubectl_command" exec -n "data-$tenant" \
    deployment/postgres -- psql -X -v ON_ERROR_STOP=1 -U runtime -d runtime \
    -At -F $'\t' -c \
    "SELECT deployment_id, release_id, artifact_digest, bundle_jti, promotion_jti
       FROM prometa_runtime_release_activation
      WHERE tenant_id = 'tenant-topology-$tenant'
        AND runtime_id = 'runtime-topology-$tenant'
        AND deployment_id = '$deployment_id';"
}

wait_activation() {
  tenant=$1
  deployment_id=$2
  row=
  for _ in {1..60}; do
    row=$(database_activation "$tenant" "$deployment_id")
    if [ -n "$row" ]; then
      printf '%s\n' "$row"
      return
    fi
    sleep 1
  done
  echo "Activation did not converge for $tenant/$deployment_id" >&2
  return 2
}

run_stage() {
  stage=$1
  chart=$2
  chart_version=$3
  image=$4
  repository=${image%@*}
  digest=${image##*@}
  for tenant in a b; do
    deployment_id=$(sequence_value "$tenant" "$stage" deploymentId)
    config_file=$(sequence_value "$tenant" "$stage" configFile)
    KUBECONFIG="$kubeconfig" "$kubectl_command" create secret generic runtime-release \
      --namespace "runtime-$tenant" \
      --from-file="config.json=$assets/$config_file" \
      --dry-run=client -o json | \
      KUBECONFIG="$kubeconfig" "$kubectl_command" apply -f - >/dev/null

    KUBECONFIG="$kubeconfig" "$helm_command" upgrade runtime "$chart" \
      --namespace "runtime-$tenant" \
      --reuse-values \
      --set-string "image.repository=$repository" \
      --set-string "image.digest=$digest" \
      --set-string "runtimeConfig.rolloutId=$deployment_id" \
      --wait --timeout 5m
    KUBECONFIG="$kubeconfig" "$kubectl_command" rollout status \
      --namespace "runtime-$tenant" deployment/runtime --timeout=180s

    deployed_image=$(
      KUBECONFIG="$kubeconfig" "$kubectl_command" get deployment/runtime \
        --namespace "runtime-$tenant" \
        -o jsonpath='{.spec.template.spec.containers[?(@.name=="runtime")].image}'
    )
    if [ "$deployed_image" != "$image" ]; then
      echo "Runtime image mismatch at $stage/$tenant" >&2
      return 2
    fi
    deployed_chart=$(
      KUBECONFIG="$kubeconfig" "$helm_command" list \
        --namespace "runtime-$tenant" --filter '^runtime$' -o json | \
        "$python_command" -c '
import json, sys
releases = json.load(sys.stdin)
if len(releases) != 1:
    raise SystemExit("runtime Helm release is missing")
print(releases[0]["chart"].removeprefix("prometa-runtime-"))
'
    )
    if [ "$deployed_chart" != "$chart_version" ]; then
      echo "Runtime chart mismatch at $stage/$tenant" >&2
      return 2
    fi
    ready_replicas=$(
      KUBECONFIG="$kubeconfig" "$kubectl_command" get deployment/runtime \
        --namespace "runtime-$tenant" -o jsonpath='{.status.readyReplicas}'
    )
    if [ "$ready_replicas" != 2 ]; then
      echo "Runtime replicas are not ready at $stage/$tenant" >&2
      return 2
    fi
    KUBECONFIG="$kubeconfig" "$kubectl_command" exec \
      --namespace "gateway-$tenant" pod/probe -- \
      python /opt/topology/topology_probe.py request \
      --url "http://runtime.runtime-$tenant.svc.cluster.local:8080" \
      --request-id "published-$stage-$tenant" \
      --expect-answer "tenant-$tenant" >/dev/null
    activation=$(wait_activation "$tenant" "$deployment_id")
    schema_versions=$(
      KUBECONFIG="$kubeconfig" "$kubectl_command" exec -n "data-$tenant" \
        deployment/postgres -- psql -X -v ON_ERROR_STOP=1 -U runtime -d runtime \
        -At -c \
        "SELECT string_agg(version::text, ',' ORDER BY version)
           FROM prometa_runtime_schema_migrations;"
    )
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$stage" "$tenant" "$image" "$chart_version" "$ready_replicas" \
      "$schema_versions" "$activation" >>"$observations"
  done
}

run_stage baseline "$baseline_chart" "$baseline_chart_version" "$baseline_image"
run_stage target "$target_chart" "$target_chart_version" "$target_image"
run_stage rollback "$baseline_chart" "$baseline_chart_version" "$baseline_image"

kubernetes_version=$(
  KUBECONFIG="$kubeconfig" "$kubectl_command" version -o json | \
    "$python_command" -c \
    'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])'
)
mkdir -p "$(dirname -- "$report")"
"$python_command" "$fixture" report \
  --sequence "$assets/sequence.json" \
  --observations "$observations" \
  --baseline-descriptor "$baseline_descriptor" \
  --target-descriptor "$target_descriptor" \
  --kubernetes-version "$kubernetes_version" \
  --output "$report"

echo "Published runtime upgrade/rollback proof passed: $report"

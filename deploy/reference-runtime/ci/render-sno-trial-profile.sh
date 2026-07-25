#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
runtime_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)
chart="$runtime_root/chart"
helm_bin=${HELM_BIN:-helm}
output=${1:-}
workdir=$(mktemp -d "${TMPDIR:-/tmp}/orchestra-runtime-sno.XXXXXX")
trap 'rm -rf "$workdir"' EXIT HUP INT TERM

manifest=${output:-"$workdir/openshift-sno-runtime.yaml"}
if [ -n "$output" ]; then
  mkdir -p "$(dirname -- "$output")"
fi

if "$helm_bin" template orchestra-runtime "$chart" \
  --namespace orchestra-runtime \
  -f "$chart/values.openshift-sno-trial.yaml" >/dev/null 2>&1; then
  echo "OpenShift SNO trial values rendered without a released image digest" >&2
  exit 1
fi

required=(
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
)

render_profile() {
  "$helm_bin" template orchestra-runtime "$chart" \
    --namespace orchestra-runtime \
    -f "$chart/values.openshift-sno-trial.yaml" "${required[@]}" "$@"
}

expect_profile_failure() {
  local description=$1
  shift
  if render_profile "$@" >/dev/null 2>&1; then
    echo "OpenShift SNO trial profile accepted ${description}" >&2
    exit 1
  fi
}

"$helm_bin" lint "$chart" -f "$chart/values.openshift-sno-trial.yaml" \
  "${required[@]}"
render_profile >"$manifest"

grep -qF 'kind: Deployment' "$manifest"
grep -qF 'replicas: 1' "$manifest"
grep -qF \
  'prometa.io/engineering-trial-profile-id: "orchestra-ocp-sno-trial-amd64-v1"' \
  "$manifest"
grep -qF \
  'image: "ghcr.io/prometa-ai/orchestra-python-sdk/prometa-runtime-host-ubi9@sha256:aaaaaaaa' \
  "$manifest"
grep -qF 'prometa.io/runtime-config-rollout-id: "orchestra-sno-trial-runtime-config-v1"' \
  "$manifest"
grep -qF 'prometa.io/server-tls-rollout-id: "orchestra-sno-trial-runtime-tls-v1"' \
  "$manifest"
grep -A1 -F 'name: PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT' "$manifest" \
  | grep -qF 'orchestra-runtime-edge-overload-v1'
grep -A1 -F 'name: SSL_CERT_FILE' "$manifest" \
  | grep -qF '/etc/prometa-runtime-ca/ca-bundle.crt'
grep -qF 'name: "orchestra-runtime-trial-ca"' "$manifest"
grep -qF 'secretName: "orchestra-runtime-trial-config"' "$manifest"
grep -qF 'secretName: "orchestra-runtime-trial-server-tls"' "$manifest"
grep -qF 'name: "orchestra-runtime-trial-credentials"' "$manifest"
grep -qF 'name: "orchestra-runtime-trial-migration"' "$manifest"
grep -qF 'dns.operator.openshift.io/daemonset-dns: default' "$manifest"
grep -qF 'port: 5432' "$manifest"
grep -qF 'port: 8080' "$manifest"
grep -qF 'port: 3443' "$manifest"
grep -qF 'port: 8443' "$manifest"
grep -qF 'cpu: 100m' "$manifest"
grep -qF 'memory: 128Mi' "$manifest"
grep -qF 'cpu: 500m' "$manifest"
grep -qF 'memory: 512Mi' "$manifest"
grep -qF 'automountServiceAccountToken: false' "$manifest"
grep -qF 'readOnlyRootFilesystem: true' "$manifest"
grep -qF 'allowPrivilegeEscalation: false' "$manifest"
grep -qF 'type: RuntimeDefault' "$manifest"

for kind in ServiceAccount Service Deployment Job NetworkPolicy; do
  grep -qF "kind: $kind" "$manifest"
done
if grep -Eq '^kind: (Secret|ConfigMap|Route|Ingress|HorizontalPodAutoscaler|PodDisruptionBudget|CronJob|PersistentVolumeClaim)$' \
  "$manifest"; then
  echo "SNO trial render emitted a forbidden object" >&2
  exit 1
fi
if grep -qF 'prometa.io/production-profile-id:' "$manifest"; then
  echo "SNO trial render claimed the production profile" >&2
  exit 1
fi
if grep -Eq '(^|[[:space:]])(runAsUser|runAsGroup|fsGroup):' "$manifest"; then
  echo "SNO trial render pinned an identity managed by restricted-v2" >&2
  exit 1
fi

expect_profile_failure "the production profile" \
  --set productionProfile.enabled=true
expect_profile_failure "an unknown profile ID" \
  --set engineeringTrialProfile.profileId=another-profile
expect_profile_failure "missing source-only acknowledgement" \
  --set engineeringTrialProfile.sourceOnlyAcknowledged=false
expect_profile_failure "two replicas" --set replicaCount=2
expect_profile_failure "autoscaling" --set autoscaling.enabled=true
expect_profile_failure "a PodDisruptionBudget" \
  --set podDisruptionBudget.enabled=true
expect_profile_failure "a mutable image" --set-string image.digest=
expect_profile_failure "an unbounded runtime edge" \
  --set-string runtimeEdge.overloadContract=
expect_profile_failure "ConfigMap runtime configuration" \
  --set-string runtimeConfig.existingSecret= \
  --set runtimeConfig.existingConfigMap=runtime-config
expect_profile_failure "optional model credentials" \
  --set credentials.modelGatewayApiKeyOptional=true
expect_profile_failure "optional receipt credentials" \
  --set credentials.receiptApiKeyOptional=true
expect_profile_failure "required synchronous control-plane pull credentials" \
  --set credentials.controlPlaneApiKeyOptional=false
expect_profile_failure "plaintext serving" --set serverTls.enabled=false \
  --set-string serverTls.existingSecret= \
  --set-string serverTls.rolloutId=
expect_profile_failure "an absent outbound trust bundle" \
  --set trustedCA.enabled=false \
  --set-string trustedCA.configMapName=
expect_profile_failure "a shared migration identity" \
  --set migration.existingSecret=orchestra-runtime-trial-credentials
expect_profile_failure "disabled NetworkPolicy" --set networkPolicy.enabled=false
expect_profile_failure "unscoped runtime DNS" \
  --set-string 'networkPolicy.dnsPodSelector.matchLabels.dns\.operator\.openshift\.io/daemonset-dns='
expect_profile_failure "unscoped migration DNS" \
  --set-string 'migration.networkPolicy.dnsPodSelector.matchLabels.dns\.operator\.openshift\.io/daemonset-dns='
expect_profile_failure "service-account token automount" \
  --set serviceAccount.automountServiceAccountToken=true
expect_profile_failure "runtime backup claims" --set backup.enabled=true
expect_profile_failure "resource request drift" \
  --set resources.requests.cpu=150m

echo "Source-only OpenShift SNO runtime render passed: $manifest"

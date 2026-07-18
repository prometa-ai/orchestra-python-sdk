#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 RELEASE_TAG SOURCE_DIRECTORY RELEASE_DIRECTORY DESCRIPTOR" >&2
  exit 2
fi

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
release_tag=$1
source_directory=$2
release_directory=$3
descriptor=$4

for command in gh jq cosign helm sha256sum; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Required command is unavailable: $command" >&2
    exit 2
  fi
done
if ! printf '%s' "$release_tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "Published release tag is invalid: $release_tag" >&2
  exit 2
fi
if [ ! -d "$source_directory/.git" ]; then
  echo "Exact release source checkout is unavailable: $source_directory" >&2
  exit 2
fi
if [ -z "${GITHUB_REPOSITORY:-}" ]; then
  echo "GITHUB_REPOSITORY is required." >&2
  exit 2
fi

actual_tag=$(git -C "$source_directory" describe --tags --exact-match HEAD)
if [ "$actual_tag" != "$release_tag" ]; then
  echo "Source resolved to $actual_tag instead of $release_tag" >&2
  exit 2
fi
"$root/scripts/verify_runtime_release_contract.sh" \
  "$release_tag" "$source_directory" >/dev/null
source_sha=$(git -C "$source_directory" rev-parse HEAD)

mkdir -p "$release_directory" "$(dirname -- "$descriptor")"
gh release download "$release_tag" \
  --repo "$GITHUB_REPOSITORY" \
  --dir "$release_directory" \
  --pattern release-prometa-runtime-host-ubi9.json \
  --pattern release-chart.json \
  --pattern 'prometa-runtime-*.tgz'
gh release view "$release_tag" \
  --repo "$GITHUB_REPOSITORY" \
  --json assets >"$release_directory/github-release.json"

image_metadata="$release_directory/release-prometa-runtime-host-ubi9.json"
chart_metadata="$release_directory/release-chart.json"
jq -e --arg tag "$release_tag" --arg revision "$source_sha" \
  '.schemaVersion == 1 and .tag == $tag and .revision == $revision and
   .variant == "ubi9" and (.digest | test("^sha256:[a-f0-9]{64}$"))' \
  "$image_metadata" >/dev/null
jq -e --arg tag "$release_tag" --arg revision "$source_sha" \
  '.schemaVersion == 1 and .releaseTag == $tag and .revision == $revision and
   (.digest | test("^sha256:[a-f0-9]{64}$"))' \
  "$chart_metadata" >/dev/null

runtime_version=${release_tag#v}
chart_version=$(jq -r .chartVersion "$chart_metadata")
chart_package="$release_directory/prometa-runtime-$chart_version.tgz"
test -f "$chart_package"
for path in "$image_metadata" "$chart_metadata" "$chart_package"; do
  name=$(basename "$path")
  expected=$(jq -r --arg name "$name" \
    '.assets[] | select(.name == $name) | .digest' \
    "$release_directory/github-release.json")
  if [ -z "$expected" ] || [ "$expected" = null ]; then
    echo "GitHub release digest is missing for $name" >&2
    exit 2
  fi
  actual="sha256:$(sha256sum "$path" | awk '{print $1}')"
  if [ "$actual" != "$expected" ]; then
    echo "GitHub release digest mismatch for $name" >&2
    exit 2
  fi
done

image_ref="$(jq -r .image "$image_metadata")@$(jq -r .digest "$image_metadata")"
chart_ref="$(jq -r .repository "$chart_metadata")@$(jq -r .digest "$chart_metadata")"
identity="https://github.com/${GITHUB_REPOSITORY}/.github/workflows/publish-runtime-artifacts.yml@refs/.*"
issuer=https://token.actions.githubusercontent.com
for ref in "$image_ref" "$chart_ref"; do
  cosign verify \
    --certificate-identity-regexp "$identity" \
    --certificate-oidc-issuer "$issuer" \
    "$ref" >/dev/null
  cosign verify-attestation \
    --type cyclonedx \
    --certificate-identity-regexp "$identity" \
    --certificate-oidc-issuer "$issuer" \
    "$ref" >/dev/null
done

observed_chart_version=$(
  helm show chart "$chart_package" | awk '$1 == "version:" {print $2; exit}'
)
observed_app_version=$(
  helm show chart "$chart_package" | \
    awk '$1 == "appVersion:" {gsub(/"/, "", $2); print $2; exit}'
)
if [ "$observed_chart_version" != "$chart_version" ] || \
   [ "$observed_app_version" != "$runtime_version" ] || \
   [ "$(jq -r .appVersion "$chart_metadata")" != "$runtime_version" ]; then
  echo "Published chart metadata does not match $release_tag" >&2
  exit 2
fi

chart_sha256="sha256:$(sha256sum "$chart_package" | awk '{print $1}')"
jq -n \
  --arg releaseTag "$release_tag" \
  --arg releaseRevision "$source_sha" \
  --arg runtimeVersion "$runtime_version" \
  --arg runtimeImage "$image_ref" \
  --arg chartVersion "$chart_version" \
  --arg chartPackage "$chart_package" \
  --arg chartPackageSha256 "$chart_sha256" \
  --arg chartOciReference "$chart_ref" \
  '{
    contractVersion: 1,
    releaseTag: $releaseTag,
    releaseRevision: $releaseRevision,
    runtimeVersion: $runtimeVersion,
    runtimeImage: $runtimeImage,
    chartVersion: $chartVersion,
    chartPackage: $chartPackage,
    chartPackageSha256: $chartPackageSha256,
    chartOciReference: $chartOciReference
  }' >"$descriptor"

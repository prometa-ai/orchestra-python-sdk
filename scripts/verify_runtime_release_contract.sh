#!/usr/bin/env bash
# Bind one immutable source tag to the SDK, runtime images, and Helm chart.
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 RELEASE_TAG [REPOSITORY_ROOT]" >&2
  exit 2
fi

release_tag=$1
repo_root=${2:-$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)}

case "$release_tag" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "ERROR: release tag must be vMAJOR.MINOR.PATCH: $release_tag" >&2
    exit 2
    ;;
esac
if ! printf '%s' "$release_tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "ERROR: release tag must be vMAJOR.MINOR.PATCH: $release_tag" >&2
  exit 2
fi

read_assignment() {
  local path=$1
  local key=$2
  sed -n "s/^${key} = \"\([^\"]*\)\"$/\1/p" "$path" | head -n 1
}

read_chart_value() {
  local key=$1
  sed -n "s/^${key}: *\"\{0,1\}\([^\"]*\)\"\{0,1\}$/\1/p" \
    "$repo_root/deploy/reference-runtime/chart/Chart.yaml" | head -n 1
}

package_version=$(read_assignment "$repo_root/pyproject.toml" version)
runtime_version=$(sed -n 's/^__version__ = "\([^"]*\)"$/\1/p' \
  "$repo_root/prometa/__init__.py" | head -n 1)
chart_version=$(read_chart_value version)
chart_app_version=$(read_chart_value appVersion)

semver='^[0-9]+\.[0-9]+\.[0-9]+$'
for entry in \
  "package:$package_version" \
  "runtime:$runtime_version" \
  "chart:$chart_version" \
  "chart app:$chart_app_version"; do
  label=${entry%%:*}
  value=${entry#*:}
  if ! printf '%s' "$value" | grep -Eq "$semver"; then
    echo "ERROR: $label version is not MAJOR.MINOR.PATCH: ${value:-<empty>}" >&2
    exit 2
  fi
done

if [ "$package_version" != "$runtime_version" ] || \
   [ "$package_version" != "$chart_app_version" ]; then
  echo "ERROR: release versions differ" >&2
  echo "  pyproject.toml: $package_version" >&2
  echo "  prometa runtime: $runtime_version" >&2
  echo "  chart app:       $chart_app_version" >&2
  exit 2
fi
if [ "$release_tag" != "v$package_version" ]; then
  echo "ERROR: release tag $release_tag does not match v$package_version" >&2
  exit 2
fi

package_pin="prometa-sdk[runtime-host,runtime-mcp]==$package_version"
for dockerfile in \
  "$repo_root/deploy/reference-runtime/Dockerfile" \
  "$repo_root/deploy/reference-runtime/Dockerfile.ubi"; do
  if ! grep -Fq "ARG IMAGE_VERSION=$package_version" "$dockerfile"; then
    # v0.18.0 is the only runtime release created before the Debian image
    # gained build-injected OCI metadata. Its immutable source still carries
    # the exact literal version; publication adds revision/version labels and
    # the pinned base without changing those source bytes.
    if [ "$release_tag" != "v0.18.0" ] || \
       [ "$dockerfile" != "$repo_root/deploy/reference-runtime/Dockerfile" ] || \
       ! grep -Fq "org.opencontainers.image.version=\"$package_version\"" \
         "$dockerfile"; then
      echo "ERROR: $dockerfile does not default IMAGE_VERSION to $package_version" >&2
      exit 2
    fi
  fi
  if ! grep -Fq "$package_pin" "$dockerfile"; then
    echo "ERROR: $dockerfile does not install $package_pin" >&2
    exit 2
  fi
done

compose_image="prometa-runtime-host:$package_version"
compose_matches=$(grep -Foc "$compose_image" \
  "$repo_root/deploy/reference-runtime/compose.yaml" || true)
if [ "$compose_matches" -ne 2 ]; then
  echo "ERROR: compose defaults must reference $compose_image exactly twice" >&2
  exit 2
fi

printf 'release_version=%s\n' "$package_version"
printf 'release_tag=%s\n' "$release_tag"
printf 'chart_version=%s\n' "$chart_version"
printf 'chart_app_version=%s\n' "$chart_app_version"

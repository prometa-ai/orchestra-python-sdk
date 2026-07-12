#!/bin/sh
set -eu

fail() {
  printf 'prometa runtime restore failed: %s\n' "$1" >&2
  exit 2
}

digest_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    fail "sha256 utility unavailable"
  fi
}

archive=${PROMETA_RUNTIME_RESTORE_FILE:-}
[ -n "$archive" ] || fail "PROMETA_RUNTIME_RESTORE_FILE is required"
[ "${PROMETA_RUNTIME_RESTORE_CONFIRM:-}" = "restore-tenant-runtime" ] \
  || fail "PROMETA_RUNTIME_RESTORE_CONFIRM is invalid"
case "$archive" in
  /*) ;;
  *) fail "restore file must be an absolute path" ;;
esac
case "$archive" in
  *"
"*|*""*) fail "restore file contains a line break" ;;
esac
[ -r "$archive" ] || fail "restore archive is not readable"
[ -r "${archive}.sha256" ] || fail "restore checksum is not readable"
name=$(basename "$archive")
case "$name" in
  ''|*[!A-Za-z0-9._-]*) fail "restore basename contains unsupported characters" ;;
esac
[ -n "${PGHOST:-}" ] || fail "PGHOST is required"
[ -n "${PGDATABASE:-}" ] || fail "PGDATABASE is required"
[ -n "${PGUSER:-}" ] || fail "PGUSER is required"
command -v pg_restore >/dev/null 2>&1 || fail "pg_restore unavailable"
command -v psql >/dev/null 2>&1 || fail "psql unavailable"

expected=$(awk 'NR == 1 {print $1}' "${archive}.sha256")
case "$expected" in
  ''|*[!0-9a-f]*) fail "restore checksum is invalid" ;;
esac
[ "${#expected}" -eq 64 ] || fail "restore checksum is invalid"
actual=$(digest_file "$archive")
[ "$actual" = "$expected" ] || fail "restore checksum mismatch"
pg_restore --list "$archive" >/dev/null || fail "archive validation failed"

export PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT:-10}
already_initialized=$(psql -X -A -t -v ON_ERROR_STOP=1 -c \
  "SELECT to_regclass('public.prometa_runtime_schema_migrations') IS NOT NULL") \
  || fail "target database check failed"
[ "$already_initialized" = "f" ] \
  || fail "target database is not empty; restore requires a fresh database"

pg_restore --exit-on-error --no-owner --no-privileges \
  --dbname "$PGDATABASE" "$archive" || fail "pg_restore failed"
schema_version=$(psql -X -A -t -v ON_ERROR_STOP=1 -c \
  "SELECT MAX(version) FROM prometa_runtime_schema_migrations") \
  || fail "restored schema check failed"
[ "$schema_version" = "${PROMETA_RUNTIME_EXPECTED_SCHEMA_VERSION:-5}" ] \
  || fail "restored schema version is incompatible"

printf '{"archive":"%s","restore":"completed","schemaVersion":%s}\n' \
  "$name" "$schema_version"

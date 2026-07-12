#!/bin/sh
set -eu

fail() {
  printf 'prometa runtime backup failed: %s\n' "$1" >&2
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

archive=${PROMETA_RUNTIME_BACKUP_FILE:-}
[ -n "$archive" ] || fail "PROMETA_RUNTIME_BACKUP_FILE is required"
case "$archive" in
  /*) ;;
  *) fail "backup file must be an absolute path" ;;
esac
case "$archive" in
  *"
"*|*""*) fail "backup file contains a line break" ;;
esac
[ -n "${PGHOST:-}" ] || fail "PGHOST is required"
[ -n "${PGDATABASE:-}" ] || fail "PGDATABASE is required"
[ -n "${PGUSER:-}" ] || fail "PGUSER is required"
command -v pg_dump >/dev/null 2>&1 || fail "pg_dump unavailable"
command -v pg_restore >/dev/null 2>&1 || fail "pg_restore unavailable"

directory=$(dirname "$archive")
name=$(basename "$archive")
case "$name" in
  ''|*[!A-Za-z0-9._-]*) fail "backup basename contains unsupported characters" ;;
esac
[ -d "$directory" ] || fail "backup directory does not exist"
[ -w "$directory" ] || fail "backup directory is not writable"
if [ -e "$archive" ] || [ -L "$archive" ]; then
  fail "backup archive already exists"
fi
if [ -e "${archive}.sha256" ] || [ -L "${archive}.sha256" ]; then
  fail "backup checksum already exists"
fi

export PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT:-10}
umask 077
temporary=$(mktemp "${archive}.tmp.XXXXXX") || fail "temporary archive unavailable"
checksum_temporary=$(mktemp "${archive}.sha256.tmp.XXXXXX") || {
  rm -f "$temporary"
  fail "temporary checksum unavailable"
}
cleanup() {
  rm -f "$temporary" "$checksum_temporary"
}
trap cleanup EXIT HUP INT TERM

pg_dump --format=custom --no-owner --no-privileges --file "$temporary" \
  || fail "pg_dump failed"
pg_restore --list "$temporary" >/dev/null || fail "archive validation failed"
digest=$(digest_file "$temporary")
printf '%s  %s\n' "$digest" "$name" >"$checksum_temporary"
chmod 600 "$temporary" "$checksum_temporary"
mv "$temporary" "$archive"
temporary=
mv "$checksum_temporary" "${archive}.sha256"
checksum_temporary=
trap - EXIT HUP INT TERM

printf '{"archive":"%s","backup":"created","checksum":"sha256"}\n' "$name"

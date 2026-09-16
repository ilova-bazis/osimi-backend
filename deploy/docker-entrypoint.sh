#!/bin/sh
set -eu

load_secret() {
  variable_name="$1"
  file_path="$2"

  if [ -z "$file_path" ]; then
    return
  fi
  if [ ! -r "$file_path" ]; then
    echo "Secret file for ${variable_name} is not readable: ${file_path}" >&2
    exit 1
  fi

  secret_value="$(cat "$file_path")"
  if [ -z "$secret_value" ]; then
    echo "Secret file for ${variable_name} is empty: ${file_path}" >&2
    exit 1
  fi
  export "${variable_name}=${secret_value}"
}

load_secret DATABASE_URL "${DATABASE_URL_FILE:-}"
load_secret UPLOAD_SIGNING_SECRET "${UPLOAD_SIGNING_SECRET_FILE:-}"
load_secret LEASE_SIGNING_SECRET "${LEASE_SIGNING_SECRET_FILE:-}"
load_secret WORKER_AUTH_TOKEN "${WORKER_AUTH_TOKEN_FILE:-}"

exec "$@"

#!/bin/sh
set -eu
if [ -z "${ORBITA_APP_PASSWORD:-}" ] || [ -z "${POSTGRES_PASSWORD:-}" ]; then
  printf '%s\n' 'Separate nonempty app and administrator passwords are required.' >&2
  exit 1
fi
if [ "$ORBITA_APP_PASSWORD" = "$POSTGRES_PASSWORD" ]; then
  printf '%s\n' 'App and administrator passwords must differ.' >&2
  exit 1
fi

#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
EXAMPLE_ENV="$ROOT_DIR/.env.example"
TARGET_ENV="$ROOT_DIR/.env"

if [ ! -f "$EXAMPLE_ENV" ]; then
  echo "Missing $EXAMPLE_ENV" >&2
  exit 1
fi

if [ -f "$TARGET_ENV" ] && [ "${1:-}" != "--force" ]; then
  echo ".env already exists at $TARGET_ENV"
  echo "Edit it directly or rerun with --force to replace it."
  exit 0
fi

cp "$EXAMPLE_ENV" "$TARGET_ENV"
echo "Created $TARGET_ENV from .env.example"
echo "Edit .env to override the checked-in local Docker defaults."

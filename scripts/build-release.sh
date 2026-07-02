#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO_DIR/build}"
OUT_TGZ="$OUT_DIR/srt-bonding-relay-linux-x86_64.tar.gz"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "$OUT_DIR"

SRT_BONDING_RELAY_PATH="$STAGE/bin/srt-bonding-relay" \
SRT_BONDING_RELAY_LIB_DIR="$STAGE/lib" \
    bash "$REPO_DIR/scripts/build-local.sh"

tar -C "$STAGE" -czf "$OUT_TGZ" bin lib

echo "Asset: $OUT_TGZ"
echo "SHA256:"
sha256sum "$OUT_TGZ" | awk '{print $1 "  " $2}'


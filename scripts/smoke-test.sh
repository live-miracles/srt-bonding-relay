#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_PATH="$REPO_DIR/srt-bonding-relay.json"
BIN_PATH="${SRT_BONDING_RELAY_PATH:-$REPO_DIR/objs/srt-bonding-relay}"
LIB_DIR="${SRT_BONDING_RELAY_LIB_DIR:-$REPO_DIR/objs/lib}"
STATUS_PORT="$(python3 - <<'PY'
import json
from pathlib import Path
cfg = json.loads(Path("srt-bonding-relay.json").read_text())
print(cfg["status_port"])
PY
)"

bash "$REPO_DIR/scripts/build-local.sh"

relay_env=()
if [[ -d "$LIB_DIR" ]]; then
    relay_env=(env "LD_LIBRARY_PATH=$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}")
fi

usage_out="$(mktemp)"
trap 'rm -f "$usage_out"' EXIT
if "${relay_env[@]}" "$BIN_PATH" >"$usage_out" 2>&1; then
    echo "ERROR: expected usage invocation to fail" >&2
    exit 1
fi
grep -q "Usage:" "$usage_out"

relay_pid=""
cleanup() {
    if [[ -n "$relay_pid" ]]; then
        kill "$relay_pid" >/dev/null 2>&1 || true
        wait "$relay_pid" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

pushd "$REPO_DIR" >/dev/null
"${relay_env[@]}" "$BIN_PATH" "$CONFIG_PATH" > /tmp/srt-bonding-relay-smoke.log 2>&1 &
relay_pid="$!"
popd >/dev/null

for _ in $(seq 1 20); do
    if curl -fsS "http://127.0.0.1:${STATUS_PORT}/status" >/tmp/srt-bonding-relay-status.json; then
        break
    fi
    sleep 0.25
done

grep -q '"streamStates"' /tmp/srt-bonding-relay-status.json

#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_PATH="$REPO_DIR/srt-bonding-relay.json"
BIN_PATH="${SRT_BONDING_RELAY_PATH:-$REPO_DIR/objs/srt-bonding-relay}"
LIB_DIR="${SRT_BONDING_RELAY_LIB_DIR:-$REPO_DIR/objs/lib}"
STATUS_PORT="$(CONFIG_PATH="$CONFIG_PATH" python3 - <<'PY'
import json
import os
from pathlib import Path
cfg = json.loads(Path(os.environ["CONFIG_PATH"]).read_text())
print(cfg["status_port"])
PY
)"

bash "$REPO_DIR/scripts/build-local.sh"

relay_env=()
if [[ -d "$LIB_DIR" ]]; then
    relay_env=(env "LD_LIBRARY_PATH=$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}")
fi

relay_pid=""
usage_out="$(mktemp)"
cleanup() {
    rm -f "$usage_out"
    if [[ -n "$relay_pid" ]]; then
        kill "$relay_pid" >/dev/null 2>&1 || true
        wait "$relay_pid" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if "${relay_env[@]}" "$BIN_PATH" >"$usage_out" 2>&1; then
    echo "ERROR: expected usage invocation to fail" >&2
    exit 1
fi
grep -q "Usage:" "$usage_out"

pushd "$REPO_DIR" >/dev/null
"${relay_env[@]}" "$BIN_PATH" "$CONFIG_PATH" > /tmp/srt-bonding-relay-smoke.log 2>&1 &
relay_pid="$!"
popd >/dev/null

for _ in $(seq 1 20); do
    if ! kill -0 "$relay_pid" >/dev/null 2>&1; then
        echo "ERROR: relay process exited before status became healthy" >&2
        cat /tmp/srt-bonding-relay-smoke.log >&2
        exit 1
    fi
    if curl -fsS "http://127.0.0.1:${STATUS_PORT}/status" >/tmp/srt-bonding-relay-status.json; then
        break
    fi
    sleep 0.25
done

grep -q '"streamStates"' /tmp/srt-bonding-relay-status.json
STATUS_PID="$(python3 - <<'PY'
import json
from pathlib import Path
print(json.loads(Path("/tmp/srt-bonding-relay-status.json").read_text())["pid"])
PY
)"
if [[ "$STATUS_PID" != "$relay_pid" ]]; then
    echo "ERROR: status response came from pid $STATUS_PID, expected $relay_pid" >&2
    cat /tmp/srt-bonding-relay-smoke.log >&2
    exit 1
fi

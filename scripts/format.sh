#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-write}"

if [[ "$MODE" != "write" && "$MODE" != "check" ]]; then
    echo "Usage: $0 [write|check]" >&2
    exit 1
fi

if ! command -v clang-format >/dev/null 2>&1; then
    echo "ERROR: clang-format is required for code formatting." >&2
    exit 1
fi

mapfile -t files < <(find "$REPO_DIR/src" -type f \( -name '*.c' -o -name '*.h' \) | sort)

if [[ "$MODE" == "check" ]]; then
    clang-format --dry-run --Werror "${files[@]}"
else
    clang-format -i "${files[@]}"
fi

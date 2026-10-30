#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: hack/verify.sh

Runs make verify with progress every 15 seconds and concise output.
Retains the complete output in a temporary log and prints its path.
On failure, prints the final 100 log lines and returns make's exit status.
EOF
}

if (($#)); then
    case "$1" in
        --help|-h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
fi

cd "$(dirname "$0")/.."
verify_output="$(mktemp)"
echo "verify: running make verify; full log: $verify_output"
verify_start=$SECONDS
(
    while sleep 15 >/dev/null 2>&1; do
        echo "verify: still running ($((SECONDS - verify_start))s)"
    done
) &
heartbeat_pid=$!
trap 'kill "$heartbeat_pid" 2>/dev/null || true' EXIT

verify_status=0
make verify > "$verify_output" 2>&1 || verify_status=$?
kill "$heartbeat_pid" 2>/dev/null || true
wait "$heartbeat_pid" 2>/dev/null || true
trap - EXIT

if ((verify_status == 0)); then
    echo "verify: passed ($((SECONDS - verify_start))s); full log: $verify_output"
else
    echo "verify: failed with exit status $verify_status ($((SECONDS - verify_start))s); final 100 log lines:" >&2
    tail -n 100 "$verify_output" >&2
    echo "verify: full log: $verify_output" >&2
fi
exit "$verify_status"

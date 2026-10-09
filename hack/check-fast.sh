#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: hack/check-fast.sh [--run REGEX] [--focus REGEX] ./package [./package ...]

Checks formatting, compiles the selected packages and tests, runs Go's default
test vet checks, and executes only the selected tests. Uses -mod=readonly, no tidy.
Requires a non-empty test filter and explicit local packages (no ./... patterns).

Examples:
  hack/check-fast.sh --run '^TestDisruptionPacing' ./pkg/controllers/disruption
  hack/check-fast.sh --focus 'Shared disruption pacing' ./pkg/controllers/disruption

--run selects Go tests, including the parent test when selecting subtests.
--focus selects Ginkgo specs; without --run, ordinary Go tests also run.
Envtest defaults to installed Kubernetes 1.36.2 assets. Install missing assets
separately with: go tool -modfile=go.tools.mod setup-envtest use -p path 1.36.2
EOF
}

cd "$(dirname "$0")/.."
run=''
focus=''
packages=()
while (($#)); do
    case "$1" in
        --run|--focus)
            option="$1"
            if (($# < 2)) || [[ -z "$2" ]]; then
                echo "$option requires a non-empty regex" >&2
                exit 2
            fi
            if [[ "$option" == --run ]]; then run="$2"; else focus="$2"; fi
            shift 2
            ;;
        --help|-h) usage; exit 0 ;;
        ./*)
            if [[ "$1" == *...* || ! -d "$1" ]]; then
                echo "Select an explicit package directory, not a recursive pattern: $1" >&2
                exit 2
            fi
            packages+=("$1")
            shift
            ;;
        *) usage >&2; exit 2 ;;
    esac
done
if ((${#packages[@]} == 0)) || [[ -z "$run$focus" ]]; then
    usage >&2
    exit 2
fi

export GOTOOLCHAIN=auto
export K8S_VERSION="${K8S_VERSION:-1.36.2}"
for override in TEST_ASSET_KUBE_APISERVER TEST_ASSET_ETCD TEST_ASSET_KUBECTL; do
    if [[ -n "${!override:-}" ]]; then
        echo "Unset $override so envtest uses KUBEBUILDER_ASSETS consistently" >&2
        exit 2
    fi
done
case "${USE_EXISTING_CLUSTER:-false}" in
    false|False|FALSE|0) ;;
    *) echo "Unset USE_EXISTING_CLUSTER; tests must start their own control plane" >&2; exit 2 ;;
esac
installed_assets="$(go tool -modfile=go.tools.mod setup-envtest use -i -p path "$K8S_VERSION")"
export KUBEBUILDER_ASSETS="${KUBEBUILDER_ASSETS:-$installed_assets}"
for asset in etcd kube-apiserver kubectl; do
    if [[ ! -x "$KUBEBUILDER_ASSETS/$asset" ]]; then
        echo "Missing executable envtest asset: $KUBEBUILDER_ASSETS/$asset" >&2
        exit 1
    fi
done

echo 'Checking package formatting'
package_dirs="$(go list -mod=readonly -f '{{.Dir}}' "${packages[@]}")"
shopt -s nullglob
while IFS= read -r directory; do
    files=("$directory"/*.go)
    if ((${#files[@]})); then
        unformatted="$(gofmt -l "${files[@]}")"
        if [[ -n "$unformatted" ]]; then
            printf 'Run gofmt on:\n%s\n' "$unformatted" >&2
            exit 1
        fi
    fi
done <<< "$package_dirs"
git diff --check

args=(-mod=readonly -count=1 -timeout=20m -v)
if [[ -n "$run" ]]; then args+=(-run "$run"); fi
if [[ -n "$focus" ]]; then args+=(-ginkgo.focus="$focus" -ginkgo.fail-on-empty -ginkgo.no-color); fi
echo 'Running focused tests (including compilation and default vet checks)'
test_output="$(mktemp)"
trap 'rm -f "$test_output"' EXIT
if ! go test "${packages[@]}" "${args[@]}" > "$test_output" 2>&1; then
    cat "$test_output"
    exit 1
fi
# Detect a filter that ran nothing without paying for a second Go invocation.
if ! grep -q '^=== RUN ' "$test_output"; then
    cat "$test_output"
    echo 'No tests ran; correct the test filter' >&2
    exit 1
fi
if [[ -n "$focus" ]] && ! grep -Eq '^Ran [1-9][0-9]* of [0-9]+ Specs' "$test_output"; then
    cat "$test_output"
    echo 'No Ginkgo specs ran; ensure --run selects the suite entry point' >&2
    exit 1
fi
awk '
    /^=== RUN / { if ($3 ~ /\//) subtests++; else tests++ }
    /^(ok[[:space:]]|Ran |SUCCESS!)/ { print }
    END { printf "Go tests started: %d (+ %d subtests)\n", tests, subtests }
' "$test_output"

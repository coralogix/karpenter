#!/usr/bin/env bash
set -euo pipefail

# Ginkgo suite drivers in this repo (func Test… that calls RunSpecs). --focus runs only these unless --run is set.
ginkgo_suite_run='^(TestAPIs|TestScheduling|TestResources|TestMetrics|TestOperator|TestOptions|TestAtomic|TestCapacityBuffer|TestCloudProvider|TestDynamicResources|TestNodeOverlay|TestReconciles|TestRecorder|TestVirtualPods)$'

usage() {
    cat <<'EOF'
Usage: hack/check-fast.sh [--run REGEX] [--focus REGEX] [--full] ./package

Compiles the package and its tests, runs Go's default test vet checks, and
executes only the selected tests. Does not format or lint; use hack/lint-and-format.sh
separately. Uses -mod=readonly, no tidy.
Requires exactly one explicit package directory (no ./... patterns).

--run and --focus need a non-empty test filter. --full runs every test in the
package (no filter), with the same envtest checks.

Examples:
  hack/check-fast.sh --run '^TestDisruptionPacing' ./pkg/controllers/disruption
  hack/check-fast.sh --focus 'Shared disruption pacing' ./pkg/controllers/disruption
  hack/check-fast.sh --full ./pkg/controllers/disruption

--run selects Go tests, including the parent test when selecting subtests.
--focus selects Ginkgo specs inside the suite test for this package. Without
--run, only the Ginkgo suite driver runs (not other Test* functions in the package).
Use --run to target unit tests or a non-default suite name explicitly.

Focused runs use a 3m test timeout; --full uses 20m. Override with CHECK_FAST_TIMEOUT.
Complete test output is retained in a temporary log whose path is printed.
Failure summaries are limited to 16 KiB, 10 failures, 40 context lines per
failure, and 1 KiB per line. Truncated details remain in the full log.
Progress is printed every 15 seconds while tests run.

Envtest defaults to installed Kubernetes 1.36.2 assets. Install missing assets
separately with: go tool -modfile=go.tools.mod setup-envtest use -p path 1.36.2
EOF
}

summarize_failure() {
    local summary
    echo "check-fast: $1"
    # Keep the full log untouched. Buffer the summary so a formatter failure
    # cannot print a partial report followed by an unbounded fallback.
    if ! summary="$(LC_ALL=C awk '
        function clip(s, limit, marker) {
            marker = " ... [truncated]"
            return length(s) <= limit ? s : substr(s, 1, limit - length(marker)) marker
        }
        function emit(s) {
            s = clip(s, 1023)
            if (bytes + length(s) + 1 > 14000) exhausted = 1
            if (exhausted) { shortened = 1; return }
            print s
            bytes += length(s) + 1
        }
        function failure(name, kind) {
            total++
            if (total > 10) return -1
            names[total] = clip(name, 511)
            kinds[total] = kind
            return total
        }
        function context(id, line) {
            if (id < 1) return
            if (++lines[id] <= 40) details[id] = details[id] clip(line, 1023) "\n"
            else omitted[id] = 1
        }
        {
            gsub(/\033\[[0-?]*[ -\/]*[@-~]/, "")
            line = clip($0, 1023)
            if (NR <= 10) head[NR] = line
            tail[(NR - 1) % 10] = line

            # Crashes and build errors may never produce a completed test.
            if ($0 ~ /^(panic:|fatal error:|# |go: )/ || $0 ~ /^[^[:space:]]+\.go:[0-9]+(:[0-9]+)?:/) diagnostic = 1
            if (diagnostic) {
                if (++diagnostic_lines <= 20) diagnostic_head[diagnostic_lines] = line
                else diagnostic_tail[(diagnostic_lines - 21) % 20] = line
            }

            # Ginkgo reports repeat failures in the final suite summary. Use
            # the detailed blocks, and prefer the final assertion over timelines.
            if ($0 ~ /\[(FAILED|PANICKED|TIMEDOUT|ABORTED|INTERRUPTED)\].*\[[0-9.]+ seconds\]/) {
                ginkgo = failure(line, "Ginkgo")
                suite[active] = 1
                part = message = 0
                next
            }
            if ($0 ~ /^--------------------/) ginkgo = 0
            if (ginkgo) {
                if ($0 !~ /^[[:space:]]*$/ && part < 2) {
                    if (ginkgo > 0) {
                        if (part == 0) names[ginkgo] = clip($0, 511)
                        else locations[ginkgo] = clip($0, 511)
                    }
                    part++
                    next
                }
                if ($0 ~ /^[[:space:]]+\[(FAILED|PANICKED|TIMEDOUT|ABORTED|INTERRUPTED)\]/) {
                    message = 1
                    if (ginkgo > 0) { details[ginkgo] = ""; lines[ginkgo] = omitted[ginkgo] = 0 }
                }
                if (message) {
                    if (ginkgo > 0 && $0 ~ /^[[:space:]]+In .* at:/) locations[ginkgo] = clip($0, 511)
                    context(ginkgo, line)
                }
                next
            }

            # Go assertions precede --- FAIL. Retain a bounded ring for each
            # active test, following ownership switches in verbose output.
            if ($0 ~ /^=== (RUN|CONT|NAME)[[:space:]]/) { active = $3; next }
            if ($0 ~ /^=== PAUSE[[:space:]]/) { active = ""; next }
            if ($0 ~ /^[[:space:]]*--- (PASS|SKIP|FAIL): /) {
                name = $3
                if ($0 ~ /--- FAIL:/) {
                    if (suite[name]) suite_result = line
                    else if (!child_failed[name] || buffered[name] > 0) {
                        id = failure("Go test: " name, "Go")
                        start = buffered[name] > 40 ? buffered[name] - 40 : 0
                        for (j = start; j < buffered[name]; j++) context(id, output[name, j % 40])
                        if (id > 0 && buffered[name] > 40) omitted[id] = 1
                    }
                    parent = name
                    while (sub(/\/[^\/]+$/, "", parent)) child_failed[parent] = 1
                }
                for (j = 0; j < 40; j++) delete output[name, j]
                delete buffered[name]
                if (active == name) active = ""
                next
            }
            if (active != "") output[active, buffered[active]++ % 40] = line
        }
        END {
            selected = total < 10 ? total : 10
            if (total) emit("Failures: " total)
            for (i = 1; i <= selected; i++) {
                emit(i ". " names[i])
                if (locations[i] != "") emit(locations[i])
            }
            if (total > selected) emit((total - selected) " additional failures omitted; see full log.")
            if (suite_result != "") emit(suite_result)
            for (i = 1; i <= selected; i++) {
                if (details[i] == "") continue
                emit("")
                emit("Context " i (kinds[i] == "Go" ? " (Go output; attribution is best effort):" : ":"))
                n = split(details[i], block, "\n")
                for (j = 1; j < n; j++) emit(block[j])
                useful = 1
                if (omitted[i]) shortened = 1
            }
            if (diagnostic_lines) {
                emit("General crash/build diagnostics:")
                for (i = 1; i <= 20 && i <= diagnostic_lines; i++) emit(diagnostic_head[i])
                if (diagnostic_lines > 40) { emit("... diagnostic lines omitted ..."); shortened = 1 }
                start = diagnostic_lines > 40 ? diagnostic_lines - 20 : 20
                for (i = start; i < diagnostic_lines; i++) emit(diagnostic_tail[(i - 20) % 20])
            } else if (!useful) {
                emit("Unrecognized or incomplete failure output; general log excerpts:")
                for (i = 1; i <= 10 && i <= NR; i++) emit(head[i])
                if (NR > 20) { emit("... log lines omitted ..."); shortened = 1 }
                start = NR > 20 ? NR - 9 : 11
                for (i = start; i <= NR; i++) emit(tail[(i - 1) % 10])
            }
            if (shortened) print "[Output truncated by display limits; see full log.]"
        }
    ' "$test_output" 2>/dev/null)"; then
        summary="$(
            printf 'Summary unavailable; general log excerpts:\n'
            { head -c 4096 "$test_output"; printf '\n...\n'; tail -c 4096 "$test_output"; } | LC_ALL=C cut -b 1-1000
        )"
    fi
    printf '%s\n' "$summary"
    # The formatter reserves room for the reason, truncation notice and log path.
    printf 'check-fast: full log: %s\n' "$test_output" | LC_ALL=C cut -b 1-1023
}

cd "$(dirname "$0")/.."
run=''
focus=''
full=false
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
        --full)
            full=true
            shift
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
if ((${#packages[@]} == 0)); then
    usage >&2
    exit 2
fi
if ((${#packages[@]} > 1)); then
    echo 'Pass exactly one package directory per invocation (envtest starts once per package)' >&2
    exit 2
fi
if [[ -z "$run$focus" ]] && ! $full; then
    usage >&2
    exit 2
fi
if $full && [[ -n "$run$focus" ]]; then
    echo '--full cannot be combined with --run or --focus' >&2
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

pkg="${packages[0]}"

if $full; then
    test_timeout="${CHECK_FAST_TIMEOUT:-20m}"
else
    test_timeout="${CHECK_FAST_TIMEOUT:-3m}"
fi

args=(-mod=readonly -count=1 -timeout="$test_timeout" -v)
if [[ -n "$run" ]]; then
    args+=(-run "$run")
elif [[ -n "$focus" ]]; then
    args+=(-run "$ginkgo_suite_run")
fi
if [[ -n "$focus" ]]; then args+=(-ginkgo.focus="$focus" -ginkgo.fail-on-empty -ginkgo.no-color); fi

test_label="$pkg"
if [[ -n "$focus" ]]; then
    test_label+=" (ginkgo.focus: $focus)"
elif [[ -n "$run" ]]; then
    test_label+=" (run: $run)"
else
    test_label+=" (full package tests)"
fi
echo "Running tests in $test_label (timeout: $test_timeout)"
test_output="$(mktemp)"
echo "check-fast: full log: $test_output"
test_start=$SECONDS
(
    while sleep 15 >/dev/null 2>&1; do
        echo "check-fast: $pkg — still running ($((SECONDS - test_start))s)"
    done
) &
heartbeat_pid=$!
trap 'kill "$heartbeat_pid" 2>/dev/null || true' EXIT

test_status=0
go test "$pkg" "${args[@]}" > "$test_output" 2>&1 || test_status=$?
kill "$heartbeat_pid" 2>/dev/null || true
wait "$heartbeat_pid" 2>/dev/null || true
trap - EXIT

if ((test_status != 0)); then
    summarize_failure "tests failed (exit status $test_status)"
    exit "$test_status"
fi
test_elapsed=$((SECONDS - test_start))
parse_output="$(mktemp)"
trap 'rm -f "$parse_output"' EXIT
sed $'s/\033\\[[0-9;]*m//g' "$test_output" > "$parse_output"

# Detect a filter that ran nothing without paying for a second Go invocation.
if ! grep -q '^=== RUN ' "$parse_output"; then
    if $full && grep -Eq '^\?[[:space:]]+[^[:space:]]+[[:space:]]+\[no test files\]$' "$parse_output"; then
        echo "check-fast: $pkg — compiled; no test files (${test_elapsed}s)"
        exit 0
    fi
    summarize_failure 'No tests ran; correct the test filter'
    exit 1
fi
if [[ -n "$focus" ]] && ! grep -Eq '^Ran [1-9][0-9]* of [0-9]+ Specs' "$parse_output"; then
    summarize_failure 'No Ginkgo specs ran; ensure --run selects the suite entry point'
    exit 1
fi
ginkgo_summary="$(grep -E '^Ran [0-9]+ of [0-9]+ Specs' "$parse_output" | tail -1 || true)"
if [[ -n "$ginkgo_summary" ]]; then
    echo "check-fast: $pkg — $ginkgo_summary (${test_elapsed}s)"
else
    echo "check-fast: $pkg — tests finished (${test_elapsed}s)"
fi
awk '
    /^=== RUN / { if ($3 ~ /\//) subtests++; else tests++ }
    /^(ok[[:space:]]|Ran |SUCCESS!)/ { print }
    END { printf "Go tests started: %d (+ %d subtests)\n", tests, subtests }
' "$parse_output"

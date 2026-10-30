#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: hack/lint-and-format.sh [--format-only | --lint-only] PACKAGE...

Formats and/or lints one or more Go packages (explicit directories or go list
patterns such as ./pkg/controllers/disruption or ./pkg/...). Uses -mod=readonly.

Default: format every *.go file in each package directory, git diff --check,
then golangci-lint-kube-api-linter run --fix=false on the listed packages.

Examples:
  hack/lint-and-format.sh ./pkg/controllers/disruption
  hack/lint-and-format.sh --format-only ./pkg/utils/standby
  hack/lint-and-format.sh --lint-only ./pkg/controllers/provisioning/...
EOF
}

cd "$(dirname "$0")/.."

format=true
lint=true
packages=()
while (($#)); do
    case "$1" in
        --format-only)
            format=true
            lint=false
            shift
            ;;
        --lint-only)
            format=false
            lint=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [[ "$1" == *...* && ! -d "$1" ]]; then
                packages+=("$1")
            elif [[ -d "$1" ]]; then
                packages+=("$1")
            else
                echo "Not a package directory or list pattern: $1" >&2
                exit 2
            fi
            shift
            ;;
    esac
done

if ((${#packages[@]} == 0)); then
    usage >&2
    exit 2
fi

export GOTOOLCHAIN=auto
formatter_toolchain="go$(awk '$1 == "go" {print $2; exit}' go.mod)"
package_dirs="$(go list -mod=readonly -f '{{.Dir}}' "${packages[@]}" | sort -u)"

if $format; then
    echo 'Formatting selected packages'
    shopt -s nullglob
    while IFS= read -r directory; do
        files=("$directory"/*.go)
        if ((${#files[@]})); then
            before_format="$(cksum "${files[@]}")"
            GOTOOLCHAIN="$formatter_toolchain" go tool -modfile=go.tools.mod golangci-lint-kube-api-linter fmt "${files[@]}"
            after_format="$(cksum "${files[@]}")"
            if [[ "$before_format" != "$after_format" ]]; then
                awk 'NR == FNR { original[$0] = 1; next } !($0 in original) {
                    sub(/^[0-9]+ [0-9]+ /, ""); print "Formatted: " $0
                }' <(printf '%s\n' "$before_format") <(printf '%s\n' "$after_format")
            fi
        fi
    done <<< "$package_dirs"
    git diff --check
fi

if $lint; then
    echo 'Running configured linters on selected packages'
    GOTOOLCHAIN="$formatter_toolchain" \
        GOLANGCI_LINT_CACHE="$(git rev-parse --show-toplevel)/.cache/golangci-lint" \
        go tool -modfile=go.tools.mod golangci-lint-kube-api-linter run \
        --fix=false --modules-download-mode=readonly "${packages[@]}"
fi

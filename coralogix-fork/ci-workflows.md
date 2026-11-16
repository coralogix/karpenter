# GitHub Actions (fork)

CI on `coralogix/karpenter` is intentionally minimal. Enable Actions under repo **Settings → Actions** before workflows run.

## Active workflow

| Workflow | Trigger | What it runs |
|----------|---------|----------------|
| [presubmit](../.github/workflows/presubmit.yaml) | Every PR, push to `coralogix-fork-v1.14.1`, manual | `make presubmit` once (Kubernetes **1.36.x** envtest): `verify`, full `pkg/...` tests with race, licenses, vulncheck |

This replaces local agent “full validation” once the check is required on merge.

## Removed upstream workflows

These lived under `.github/workflows/` upstream but are **not** kept on the fork (no automatic runs). Reasons:

| Former workflow | Why removed |
|-----------------|-------------|
| `release.yaml` | Publishes GitHub releases and provenance; points at `kubernetes-sigs/karpenter` |
| `kind-e2e.yaml`, `kind-perf-e2e.yaml`, `e2e.yaml` | Heavy Kind + Prometheus/Pyroscope regression/perf; 7× matrix on `main`; not a practical default for fork PRs |
| `scheduling-benchmarking.yaml`, `run-bench-test.yaml` | Bench comparison on PRs; only targeted `main`; uploads artifacts |
| `approval-comment.yaml` | Maintainer `/karpenter perf` trigger for perf e2e |
| `semantic-pr.yaml` | `pull_request_target` title lint; org uses `ticket-id-validator` |
| `codeql-analysis.yaml` | Duplicate of GitHub’s managed CodeQL; `make vulncheck` already runs in presubmit |

Upstream copies remain in git history and on [kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter). To restore a workflow, copy from upstream into `.github/workflows/`.

## Still present but unused by CI

- `.github/actions/install-prometheus` and `install-pyroscope` — only needed if Kind e2e workflows return.
- [dependabot.yaml](../.github/dependabot.yaml) — dependency update PRs (already ran before Actions was enabled).

## Merge protection

After the first green presubmit on a test PR, add **presubmit** as a required check (alongside `ticket-id-validator`).
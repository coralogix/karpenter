#!/usr/bin/env bash
# Dump cluster state for evaluateMoveSet cluster-fixture benchmarks.
#
# Usage:
#   ./coralogix-fork/bench/dump-cluster-fixture.sh [cluster-name] [output-dir]
#
# Prerequisites:
#   - kubectl context pointed at the target cluster (e.g. cx498)
#   - go toolchain for instance catalog generation
set -euo pipefail

CLUSTER_NAME="${1:-cx498}"
OUT_DIR="${2:-testdata/clusterfixtures/${CLUSTER_NAME}}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "${OUT_DIR}"

dump_to() {
  local outfile=$1
  shift
  if "$@" >"${outfile}" 2>/dev/null; then
    echo "wrote ${outfile}"
  else
    printf '%s\n' 'apiVersion: v1' 'kind: List' 'items: []' >"${outfile}"
    echo "wrote empty ${outfile}"
  fi
}

echo "Dumping cluster fixture to ${OUT_DIR}"
echo "kubectl context: $(kubectl config current-context 2>/dev/null || echo unknown)"

dump_to "${OUT_DIR}/nodes.yaml" kubectl get nodes -o yaml
dump_to "${OUT_DIR}/pods.yaml" kubectl get pods -A -o yaml
dump_to "${OUT_DIR}/daemonsets.yaml" kubectl get daemonsets -A -o yaml
dump_to "${OUT_DIR}/pdbs.yaml" kubectl get pdb -A -o yaml
dump_to "${OUT_DIR}/nodepools.yaml" kubectl get nodepools.karpenter.sh -o yaml
dump_to "${OUT_DIR}/nodeclaims.yaml" kubectl get nodeclaims.karpenter.sh -A -o yaml
dump_to "${OUT_DIR}/nodeclasses.yaml" kubectl get ec2nodeclasses.karpenter.k8s.aws -o yaml

NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
POD_COUNT=$(kubectl get pods -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
PDB_COUNT=$(kubectl get pdb -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
NODEPOOL_COUNT=$(kubectl get nodepools.karpenter.sh --no-headers 2>/dev/null | wc -l | tr -d ' ')

cat >"${OUT_DIR}/metadata.json" <<EOF
{
  "cluster": "${CLUSTER_NAME}",
  "context": "$(kubectl config current-context 2>/dev/null || echo "")",
  "dumpedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "nodeCount": ${NODE_COUNT},
  "podCount": ${POD_COUNT},
  "pdbCount": ${PDB_COUNT},
  "nodePoolCount": ${NODEPOOL_COUNT}
}
EOF
echo "wrote ${OUT_DIR}/metadata.json"

(
  cd "${REPO_ROOT}"
  go run ./hack/bench/build-instance-catalog.go \
    --fixture "${OUT_DIR}" \
    --output "${OUT_DIR}/instance-types.json"
)

echo "Done. Fixture ready at ${OUT_DIR}"

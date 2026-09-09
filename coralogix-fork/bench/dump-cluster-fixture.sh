#!/usr/bin/env bash
# Dump cluster state for evaluateMoveSet cluster-fixture benchmarks.
#
# Usage:
#   ./coralogix-fork/bench/dump-cluster-fixture.sh <cluster-name> [output-dir]
#
# Prerequisites:
#   - kubectl context pointed at the target cluster
#   - go toolchain for instance catalog generation
set -euo pipefail

if [ "$#" -lt 1 ] || [ -z "${1}" ]; then
  echo "usage: $0 <cluster-name> [output-dir]" >&2
  exit 2
fi

CLUSTER_NAME="$1"
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
dump_to "${OUT_DIR}/persistentvolumeclaims.yaml" kubectl get persistentvolumeclaims -A -o yaml
dump_to "${OUT_DIR}/persistentvolumes.yaml" kubectl get persistentvolumes -o yaml
dump_to "${OUT_DIR}/storageclasses.yaml" kubectl get storageclasses -o yaml
dump_to "${OUT_DIR}/csinodes.yaml" kubectl get csinodes -o yaml

NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
POD_COUNT=$(kubectl get pods -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
PDB_COUNT=$(kubectl get pdb -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
NODEPOOL_COUNT=$(kubectl get nodepools.karpenter.sh --no-headers 2>/dev/null | wc -l | tr -d ' ')

REGION="${AWS_REGION:-}"
if [ -z "${REGION}" ]; then
  REGION="$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.topology\.kubernetes\.io/region}' 2>/dev/null || true)"
fi
if [ -z "${REGION}" ]; then
  echo "error: could not determine AWS region (set AWS_REGION or configure AWS CLI)" >&2
  exit 1
fi

cat >"${OUT_DIR}/metadata.json" <<EOF
{
  "cluster": "${CLUSTER_NAME}",
  "context": "$(kubectl config current-context 2>/dev/null || echo "")",
  "region": "${REGION}",
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
    --output "${OUT_DIR}/instance-types.json" \
    --region "${REGION}"
)

echo "Done. Fixture ready at ${OUT_DIR}"

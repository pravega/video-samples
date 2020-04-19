#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
${ROOT_DIR}/scripts/uninstall.sh

if [[ "${SKIP_PUBLISH}" != "1" ]]; then
    ${ROOT_DIR}/scripts/publish.sh
fi

for chart in ${CHARTS} ; do
    kubectl wait --for=delete --timeout=900s FlinkCluster/${chart} -n ${NAMESPACE} || true
done

${ROOT_DIR}/scripts/deploy.sh
watch "kubectl get FlinkApplication -n ${NAMESPACE} ; \
kubectl get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName -n ${NAMESPACE}"

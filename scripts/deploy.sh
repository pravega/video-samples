#! /bin/bash
set -e
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
kubectl apply -f ${ROOT_DIR}/GPUTensorflowImage/ClusterFlinkImage.yaml

for chart in ${CHARTS} ; do
    helm upgrade --install --timeout 600s --wait --debug \
        ${chart} \
        --namespace ${NAMESPACE} \
        ${ROOT_DIR}/charts/${chart} \
        $@
done

#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-object-detection}

helm upgrade --install --timeout 600 --wait --debug \
${NAMESPACE} \
--namespace ${NAMESPACE} \
${ROOT_DIR}/charts/videoprocessor \
$@

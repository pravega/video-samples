#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

helm upgrade --install --timeout 600s --wait --debug \
videoprocessor \
--namespace ${NAMESPACE} \
${ROOT_DIR}/charts/videoprocessor \
$@
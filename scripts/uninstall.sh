#! /bin/bash
set -x

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

helm  del --purge \
${NAMESPACE}-videoprocessor

kubectl wait --for=delete --timeout=300s FlinkCluster/video-data-generator -n ${NAMESPACE}
kubectl wait --for=delete --timeout=300s FlinkCluster/multi-video-grid -n ${NAMESPACE}
kubectl wait --for=delete --timeout=300s FlinkCluster/video-reader -n ${NAMESPACE}

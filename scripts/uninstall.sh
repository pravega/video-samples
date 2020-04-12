#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

helm  del \
videoprocessor \
--namespace ${NAMESPACE} || true

kubectl wait --for=delete --timeout=300s FlinkCluster/flink-object-detector -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=300s FlinkCluster/video-data-generator -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=300s FlinkCluster/multi-video-grid -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=300s FlinkCluster/video-reader -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=300s FlinkCluster/memory-eater -n ${NAMESPACE} || true

#! /bin/bash
set -x

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-object-detection}

helm  del --purge \
${NAMESPACE}-videoprocessor

kubectl wait --for=delete --timeout=300s FlinkCluster/flink-object-detector -n ${NAMESPACE}

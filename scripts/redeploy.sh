#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
${ROOT_DIR}/scripts/uninstall.sh
kubectl wait --for=delete --timeout=900s FlinkCluster/flink-object-detector -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=900s FlinkCluster/video-data-generator -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=900s FlinkCluster/multi-video-grid -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=900s FlinkCluster/video-reader -n ${NAMESPACE} || true
kubectl wait --for=delete --timeout=900s FlinkCluster/memory-eater -n ${NAMESPACE} || true
#${ROOT_DIR}/scripts/publish.sh
${ROOT_DIR}/scripts/deploy-k8s-components.sh

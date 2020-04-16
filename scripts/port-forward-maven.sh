#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
kubectl port-forward service/repo 9092:80 --namespace ${NAMESPACE} &

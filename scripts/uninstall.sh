#! /bin/bash
set -e
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh

for chart in ${CHARTS} ; do
    helm del -n ${NAMESPACE} ${chart} $@ || true
done

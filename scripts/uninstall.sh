#! /bin/bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
cat ${ROOT_DIR}/charts/all-charts.txt | xargs -i -P 0 helm del -n ${NAMESPACE} {} || true

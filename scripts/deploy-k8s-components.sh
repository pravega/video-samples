#! /bin/bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
JOBNAME=${JOBNAME:-videoprocessor}
helm upgrade --install --timeout 600s --wait --debug \
  ${JOBNAME} \
  --namespace ${NAMESPACE} \
  ${ROOT_DIR}/charts/${JOBNAME} \
  $@

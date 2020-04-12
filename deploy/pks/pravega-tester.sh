#!/usr/bin/env bash
set -ex

export IMAGE=claudiofahey/pravega-tester:0.7.0

source ./env-local.sh

kubectl delete -n ${PRAVEGA_SCOPE} deployment/pravega-tester || true

kubectl wait --for=delete --timeout=300s deployment/pravega-tester -n ${PRAVEGA_SCOPE} || true

kubectl run -n ${PRAVEGA_SCOPE} \
--serviceaccount ${PRAVEGA_SCOPE}-pravega \
--env="PRAVEGA_CONTROLLER_URI=${PRAVEGA_CONTROLLER_URI}" \
--env="PRAVEGA_SCOPE=${PRAVEGA_SCOPE}" \
--env="EVENT_SIZE=8388608" \
--env="JAVA_OPTS=-Droot.log.level=DEBUG" \
--env="DELETE_STREAM=false" \
--image ${IMAGE} \
pravega-tester

sleep 5s

kubectl logs -n ${PRAVEGA_SCOPE} deployment/pravega-tester --follow

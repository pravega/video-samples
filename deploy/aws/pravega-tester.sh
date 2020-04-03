#!/usr/bin/env bash
#export PRAVEGA_CONTROLLER_URI=tls://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:443
export PRAVEGA_CONTROLLER_URI=tls://pravega-controller.videodemo1.nautilus-lab-sulfur.com:443
export PRAVEGA_SCOPE=examples
export IMAGE=claudiofahey/pravega-tester:0.7.0

kubectl delete -n ${PRAVEGA_SCOPE} deployment/pravega-tester

kubectl run -n ${PRAVEGA_SCOPE} \
--serviceaccount ${PRAVEGA_SCOPE}-pravega \
--env="PRAVEGA_CONTROLLER_URI=${PRAVEGA_CONTROLLER_URI}" \
--env="PRAVEGA_SCOPE=${PRAVEGA_SCOPE}" \
--env="EVENT_SIZE=8388608" \
--env="JAVA_OPTS=-Droot.log.level=DEBUG" \
--image ${IMAGE} \
pravega-tester

kubectl logs -n ${PRAVEGA_SCOPE} deployment/pravega-tester --follow

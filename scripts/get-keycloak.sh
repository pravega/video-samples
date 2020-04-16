#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
kubectl get secret ${NAMESPACE}-pravega -n ${NAMESPACE} -o jsonpath="{.data.keycloak\.json}" | base64 -d > ${HOME}/keycloak.json
chmod go-rw ${HOME}/keycloak.json

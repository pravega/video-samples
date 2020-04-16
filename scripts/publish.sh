#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
export MAVEN_PASSWORD=$(kubectl get secret keycloak-${MAVEN_USERNAME} -n nautilus-system -o jsonpath='{.data.password}' | base64 -d)
cd ${ROOT_DIR}
./gradlew publish

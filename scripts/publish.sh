#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
source ${ROOT_DIR}/scripts/env-local.sh
export MAVEN_PASSWORD=$(kubectl get secret keycloak-${MAVEN_USERNAME} -n nautilus-system -o jsonpath='{.data.password}' | base64 -d)
export MAVEN_URL=http://localhost:9092/maven2
echo Starting kubectl port forward
kubectl port-forward --namespace ${NAMESPACE} service/repo 9092:80 &
sleep 2s
cd ${ROOT_DIR}
./gradlew publish
kill %kubectl

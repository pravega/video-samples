#!/usr/bin/env bash
set -ex
export CLUSTER_NAME=${CLUSTER_NAME:-frightful-four}
scripts/validate-values.py ${CLUSTER_NAME}-values.yaml,scripts/pre-install/values.yaml

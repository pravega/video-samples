#!/usr/bin/env bash
set -ex
source ./env-local.sh
nautilus-dist-master/decks-install config set registry devops-repo.isus.emc.com:8116/nautilus
nautilus-dist-master/scripts/validate-values.py ${CLUSTER_NAME}-values.yaml,nautilus-dist-master/scripts/pre-install/values.yaml

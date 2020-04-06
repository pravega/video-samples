#!/usr/bin/env bash
export CLUSTER_NAME=${CLUSTER_NAME:-frightful-four}
./decks-install config set registry devops-repo.isus.emc.com:8116/nautilus
./decks-install apply --kustomize ./manifests/ --repo ./charts/ --values ${CLUSTER_NAME}-values.yaml,scripts/pre-install/values.yaml

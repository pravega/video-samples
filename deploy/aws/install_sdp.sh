#!/usr/bin/env bash
source ./env-local.sh
nautilus-dist-master/decks-install apply \
  --kustomize nautilus-dist-master/manifests/ \
  --repo nautilus-dist-master/charts/ \
  --values ${CLUSTER_NAME}-values.yaml,nautilus-dist-master/scripts/pre-install/values.yaml

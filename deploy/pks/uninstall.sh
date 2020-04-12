#!/usr/bin/env bash
set -ex
source ./env-local.sh
nautilus-dist-master/decks-install unapply \
  --kustomize nautilus-dist-master/manifests/ \
  --repo nautilus-dist-master/charts/
watch kubectl get ns

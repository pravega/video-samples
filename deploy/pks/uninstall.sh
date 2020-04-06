#!/usr/bin/env bash
set -ex
./decks-install unapply --kustomize ./manifests/ --repo ./charts/
watch kubectl get ns

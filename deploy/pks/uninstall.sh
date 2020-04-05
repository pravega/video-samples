#!/usr/bin/env bash
./decks-install unapply --kustomize ./manifests/ --repo ./charts/
watch kubectl get ns

#!/usr/bin/env bash
./decks-install apply --kustomize ./manifests/ --repo ./charts/ --values praxagora-values.yaml,scripts/pre-install/values.yaml

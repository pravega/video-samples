#!/usr/bin/env bash
set -ex
./decks-install config set registry 024626128611.dkr.ecr.us-east-1.amazonaws.com/desa-lab-sulfur
./decks-install apply --kustomize ./manifests/ --repo ./charts/ --values videodemo1-values.yaml,scripts/pre-install/values.yaml

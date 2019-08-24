#!/usr/bin/env bash
set -x
scripts/uninstall.sh
scripts/deploy-k8s-components.sh -f charts/videoprocessor/values-medium.yaml

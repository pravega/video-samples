#!/usr/bin/env bash
set -ex
./gradlew publish
scripts/uninstall.sh
scripts/deploy-k8s-components.sh

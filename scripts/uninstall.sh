#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..
NAMESPACE=examples

helm  del --purge \
${NAMESPACE}-videoprocessor

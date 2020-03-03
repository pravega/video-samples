#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..

helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/
helm repo update

RELEASE=jupyterhub
NAMESPACE=examples

helm upgrade --install \
$RELEASE \
jupyterhub/jupyterhub \
--namespace $NAMESPACE  \
--version=0.8.0 \
--timeout 600 \
--values secret.yaml \
--values config.yaml

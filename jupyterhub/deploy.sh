#! /bin/bash
set -ex

helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/
helm repo update

RELEASE=jupyterhub
NAMESPACE=${NAMESPACE:-examples}

helm upgrade --install \
$RELEASE \
jupyterhub/jupyterhub \
--namespace $NAMESPACE  \
--version=0.8.0 \
--timeout 600 \
--values secret.yaml \
--values config.yaml

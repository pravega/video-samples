#!/usr/bin/env bash
kubectl get secret keycloak-desdp -n nautilus-system -o jsonpath='{.data.password}' | base64 -d
echo

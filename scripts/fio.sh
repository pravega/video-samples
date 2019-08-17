#!/usr/bin/env bash
kubectl delete pod fio-1
kubectl apply -f fio.yaml
kubectl wait --for=condition=ContainersReady --timeout=60s pod/fio-1
kubectl logs fio-1 --follow

#!/usr/bin/env bash
kubectl delete -f pravega-benchmark.yaml -n examples
kubectl apply -f pravega-benchmark.yaml -n examples
sleep 5s
kubectl logs -f jobs/pravega-benchmark -n examples

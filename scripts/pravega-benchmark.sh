#!/usr/bin/env bash
#kubectl apply -f pravega-benchmark.yaml -n examples
kubectl exec -i --tty bmrk-1 -n examples -- bash
#pravega-benchmark -controller $CONTROLLER_URI -scope examples -stream benchmark1 -segments 1 -producers 1 -size 10000 -throughput 1

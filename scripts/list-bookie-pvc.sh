#!/usr/bin/env bash
kubectl get pvc -n nautilus-pravega -o name | \
grep ledger.*bookie | \
xargs -i -n 1 kubectl get {} -n nautilus-pravega -o jsonpath="{.spec.volumeName}{'|'}"
echo ""

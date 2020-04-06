#!/usr/bin/env bash
set -ex

ROOT_DIR=$(readlink -f -- "$(dirname -- "$0")/..")

docker run \
-d \
-p 8890:8888 \
-e JUPYTER_ENABLE_LAB=yes \
-e PRAVEGA_GRPC_GATEWAY_ADDRESS=${HOST_IP}:54672 \
-v "${ROOT_DIR}":/home/jovyan/video-samples \
--name jupyter-notebook-video-samples \
jupyter/tensorflow-notebook:dc9744740e12 \
jupyter-lab \
--ip=0.0.0.0 \
--no-browser

sleep 5s

docker logs jupyter-notebook-video-samples

#!/usr/bin/env bash
set -ex

export PRAVEGA_GRPC_GATEWAY_ADDRESS=pravega-grpc-gateway.examples.frightful-four.eaglemonk.intranet.nautilus-platform-dev.com:80
export DATA_DIR=${HOME}/nautilus/data
docker stop jupyter-notebook-video-samples
docker rm jupyter-notebook-video-samples
./start_jupyter_docker.sh

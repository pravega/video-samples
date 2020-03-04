#!/usr/bin/env bash
set -ex

ROOT_DIR=$(readlink -f -- "$(dirname -- "$0")/..")

docker run \
-d \
-p 8890:8888 \
-e JUPYTER_ENABLE_LAB=yes \
-v "${ROOT_DIR}":/home/jovyan/video-samples \
-v /mnt:/home/jovyan/mnt \
--name jupyter-notebook-video-samples \
jupyter/all-spark-notebook:f646d2b2a3af \
jupyter-lab \
--ip=0.0.0.0 \
--no-browser

sleep 5s

docker logs jupyter-notebook-video-samples

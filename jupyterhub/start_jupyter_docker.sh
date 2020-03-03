#!/usr/bin/env bash
docker run \
-d \
-p 8890:8888 \
-e JUPYTER_ENABLE_LAB=yes \
-v "$PWD":/home/jovyan/work \
-v /mnt:/home/jovyan/mnt \
--name jupyter-notebook-video-samples \
jupyter/all-spark-notebook:f646d2b2a3af \
jupyter-lab \
--ip=0.0.0.0 \
--no-browser

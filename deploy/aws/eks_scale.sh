#!/usr/bin/env bash
eksctl scale nodegroup --cluster videodemo2 --name standard-workers --nodes 3

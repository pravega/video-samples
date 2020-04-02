#!/usr/bin/env bash
cat docker_repos.txt | xargs -i -P 0 aws ecr create-repository --repository-name --region us-east-1 --profile desa-lab-sulfur {}


# Using Jupyter Notebook with Pravega and Streaming Data Platform

## Installation on SDP

### Install Pravega GRPC Gateway on SDP

```
cd
git clone https://github.com/pravega/pravega-grpc-gateway
cd pravega-grpc-gateway
export DOCKER_REPOSITORY=claudiofahey
export IMAGE_TAG=0.6.0
export NAMESPACE=examples
scripts/build-k8s-components.sh
scripts/deploy-k8s-components.sh
```

### Install Jupyter Hub on SDP

```
export NAMESPACE=examples
./deploy.sh
kubectl get svc/proxy-public -n examples
```

Determine the external IP address displayed from the above command.
Open your browser to this IP address.
This will open Jupyter Hub.
Enter any user name (e.g. "demo"). A session will be created for each user.
Password authentication is disabled and can be any value.

### Run Sample Notebooks

In Jupyter, open a terminal.
```
cd data-project
git clone https://github.com/pravega/video-samples
```

Open the notebook data-project/video-samples/jupyterhub/notebooks/install_dependencies.ipynb.
Click Kernel -> Restart Kernel and Run All Cells.

Open the notebook video_data_generator.ipynb.

Edit the notebook to set the following value:
```
gateway = 'pravega-grpc-gateway:80'
```

Click Kernel -> Restart Kernel and Run All Cells.

Open the notebook video_player.ipynb.




# Reference

- [Pravega GRPC Gateway](https://github.com/pravega/pravega-grpc-gateway)
- https://zero-to-jupyterhub.readthedocs.io
- https://jupyter-docker-stacks.readthedocs.io

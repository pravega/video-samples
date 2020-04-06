
# Using Jupyter Notebook with Pravega and Streaming Data Platform

## Installation on SDP

### Install Pravega GRPC Gateway on SDP

1. Clone the Pravega GRPC Gateway repository.
```
cd
git clone https://github.com/pravega/pravega-grpc-gateway
cd pravega-grpc-gateway
```

2. Edit the file charts/pravega-grpc-gateway/values.yaml as needed.
   You will need to set `pravega.controller` and `service.annotations.external-dns.alpha.kubernetes.io/hostname`
   to match your environment.

3. Build and then deploy using Helm.
```
export DOCKER_REPOSITORY=claudiofahey
export IMAGE_TAG=0.7.0
export NAMESPACE=examples
scripts/build-k8s-components.sh
scripts/deploy-k8s-components.sh
```

### Install Jupyter Hub on SDP

1. Copy secret-example.yaml to secret.yaml.

2. Use `openssl rand -hex 32` to generate two secrets and place them in secret.yaml.

3. Edit the file config.yaml.
   You will need to set `proxy.service.annotations.external-dns.alpha.kubernetes.io/hostname`
   to match your environment.

3. Deploy using Helm.
```
export NAMESPACE=examples
./deploy-k8s-components.sh
```

#### Login to Jupyter Hub

1. Open your browser to the proxy service address you set above (http).   
   Alternatively, you may determine the external IP address with the following command.
```
kubectl get svc/proxy-public -n examples
```

2. Login with any user name ("videodemo" is recommended).
   The password is in the file secret.yaml, field `auth.dummy.password`.

3. Choose Jupyter Tensorflow Notebook.

4. A session will be created for each user name.

### Run Sample Notebooks

1. In Jupyter, open a terminal.
```
cd data-project
git clone https://github.com/pravega/video-samples
```

2. Open the notebook `data-project/video-samples/jupyterhub/notebooks/install_dependencies.ipynb`.
   Click Kernel -> Restart Kernel and Run All Cells.

3. Open the notebook `video_data_generator.ipynb`.
   Click Kernel -> Restart Kernel and Run All Cells.

5. Repeat for `video_player.ipynb` and `video_player_index.ipynb`.

## Reference

- [Pravega GRPC Gateway](https://github.com/pravega/pravega-grpc-gateway)
- <https://zero-to-jupyterhub.readthedocs.io>
- <https://jupyter-docker-stacks.readthedocs.io>

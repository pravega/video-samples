
# Pravega Video Samples

## Overview

![grid-sample](images/video-samples-diagram.png)

This project demonstrates methods to store, process, and read video with Pravega and Flink
which are key components of Dell EMC Streaming Data Platform (SDP).
It also includes an application that performs object detection using TensorFlow and YOLO. 

## Components

- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data. 
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

  Pravega provides dynamic scaling that can increase and decrease parallelism to automatically respond
  to changes in the event rate.

  For more information, see <http://pravega.io>.

- Flink: Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications.
  See <https://flink.apache.org> for more information.

- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.

## Building and Running the Video Samples

In the steps below, only some sections will apply to your environment.
These are identified as follows:

- **(Local)**: A local workstation deployment of Pravega (standalone or Docker).
- **(SDP)**: A deployment of Streaming Data Platform (SDP).
- **(GPU)**: GPUs are available on SDP Kubernetes worker nodes.

### Download this Repository

```
cd
git clone https://github.com/pravega/video-samples
cd video-samples
```

### Install Operating System

Install Ubuntu 18.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java 8

```
apt-get install openjdk-8-jdk
```

### (Optional) Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin.
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors).

### (Local) Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### (Local) Run Pravega

This will run a development instance of Pravega locally.
Note that the default *standalone* Pravega used for development is likely insufficient for testing video because
it stores all data in memory and quickly runs out of memory.

In the command below, replace x.x.x.x with the IP address of a local network interface such as eth0.

```
cd pravega-docker
export HOST_IP=x.x.x.x
docker-compose up -d
```

You must also create the Pravega scope. This can be performed using the REST API.
```
curl -X POST -H "Content-Type: application/json" -d '{"scopeName":"examples"}' http://localhost:10080/v1/scopes
```

You can view the Pravega logs with `docker-compose logs --follow`.

You can view the stream files stored on Tier 2 `ls -h -R /tmp/pravega-tier2`.

#### Alternative for low-memory systems

If your system does not have enough memory to run all of the Pravega containers, you may run
the standalone container using the following commands.

```
export HOST_IP=x.x.x.x
docker run -it --rm -e HOST_IP -p 9090:9090 -p 10080:9091 -p 12345:12345 pravega/pravega:latest standalone
curl -X POST -H "Content-Type: application/json" -d '{"scopeName":"examples"}' http://localhost:10080/v1/scopes
```

### Install Pravega Client and Pravega Flink Connector Libraries (optional)

This step is only required when using pre-release versions of Pravega and/or SDP.
It will install required libraries in the local Maven repository.

```
cd
git clone https://github.com/pravega/pravega
pushd pravega
git checkout r0.7
./gradlew install
popd
git clone https://github.com/pravega/flink-connectors
pushd flink-connectors
git checkout r0.7
./gradlew install
popd
```

### (SDP) Ensure Kubernetes Connectivity

Ensure that the following command works. Refer to the SDP User's Guide for details.
```
kubectl get nodes
```

### (SDP) Configure SDP Authentication

Obtain the Pravega authentication credentials.
This will be needed to allow applications on your local workstations to connect to Pravega.
```
export NAMESPACE=examples
kubectl get secret ${NAMESPACE}-pravega -n ${NAMESPACE} -o jsonpath="{.data.keycloak\.json}" | base64 -d > ${HOME}/keycloak.json
chmod go-rw ${HOME}/keycloak.json
```

**Note:** When running the example applications, you must set the following environment variables.
This can be done by setting the IntelliJ run configurations. If you set this in IntelliJ,
you must manually replace `${HOME}` with your actual home directory.
```
export pravega_client_auth_method=Bearer
export pravega_client_auth_loadDynamic=true
export KEYCLOAK_SERVICE_ACCOUNT_FILE=${HOME}/keycloak.json
```

### Determine the Pravega Controller URL

- Local:
  `tcp://127.0.0.1:9090`

- SDP, TLS enabled:
  The DNS name can be retrieved by running the following command and using the HOST value.
  `kubectl get ingress -n nautilus-pravega pravega-controller`
  The controller will have the form `tls://pravega-controller.example.com:443`.

- SDP, TLS disabled:
  The DNS name can be retrieved by running:
  `kubectl get -n nautilus-pravega svc nautilus-pravega-controller -o go-template=$'{{index .metadata.annotations "external-dns.alpha.kubernetes.io/hostname"}}\n'`
  The controller will have the form `tcp://pravega-controller.example.com:9090`.

### Install Flink Image with TensorFlow

#### Build Flink Image with TensorFlow (optional)

This is only required if you wish to further customize the Flink image.

```
cd GPUTensorflowImage
docker build .
```

Tag the image, push it, and then edit GPUTensorflowImage/ClusterFlinkImage.yaml with the
correct tag.

#### (GPU) Enable TensorFlow GPU Support

If you have GPUs on the SDP worker nodes, then set the following in gradle.properties.
```
enableGPU=true
```

### Running the Examples in IntelliJ (optional)

Run the Flink `VideoDataGeneratorJob` using the following parameters:
```
--controller
tcp://127.0.0.1:9090
--output-minNumSegments
6
--output-stream
examples/video1
```

Next, run a streaming Flink job that reads all video streams and combines them into a single video stream
where each image is composed of the input images in a square grid. 
Run the Flink `MultiVideoGridJob` with the following parameters:
```
--controller
tcp://127.0.0.1:9090
--parallelism
2
--output-minNumSegments
6
--input-stream
examples/video1
--output-stream
examples/grid1
```

Run the Flink `VideoReaderJob` using the following parameters:
```
--jobClass
io.pravega.example.videoprocessor.VideoReaderJob
--controller
tcp://127.0.0.1:9090
--parallelism
2
--input-stream
examples/grid1
```
This will write a subset of images to `/tmp/camera*.png`.

Below shows the example output from 4 camera feeds.
Note that image backgrounds are filled with random bytes to make them incompressible for testing purposes.

![grid-sample](images/grid-sample.png)

### Running the Examples in SDP (optional)

1. Create and edit the file scripts/env-local.sh that defines your environment.
```
cp scripts/env-example.sh scripts/env-local.sh
```

2. Build and deploy the Flink jobs.
```
scripts/redeploy.sh
```

You can edit the file (charts/multi-video-grid-job/values.yaml)[charts/multi-video-grid-job/values.yaml]
to change various parameters such as the image dimensions, frames per second, and
number of cameras.
If you make changes to the source code or values.yaml, you may redeploy your application by repeating step 2.

Note: You may use the script `scripts/uninstall.sh` to delete your Flink application and cluster.
This will also delete any savepoints.

### Viewing Logs in SDP (optional)

To troubleshoot a failed job, begin with the following command.
```
kubectl describe FlinkApplication -n examples multi-video-grid
```

When Flink applications write to stdout, stderr, or use slf4j logging, the output will be
available in one of several locations.

Output written by the driver (e.g. directly called by `main()`) will be available in the job's log
and can be viewed with the following command.
```
kubectl logs jobs/video-data-generator-app-v1-1 -n examples | less
```

Output written by operators (e.g. `map()`) will be available in the Flink task manager log
and can be viewed with the following command.
```
kubectl logs video-data-generator-taskmanager-0 -n examples -c server | less
```

When troubleshooting application issues, you should also review the Flink job manager log,
which can be viewed with the following command.
```
kubectl logs video-data-generator-jobmanager-0 -n examples -c server | less
```

You may want to use the kubectl logs `--follow`, `--tail`, and `--previous` flags.
You may also use the Kubernetes UI to view these logs.

## Camera Recorder Application (optional)

The Camera Recorder application reads images from a USB camera and writes them to
a Pravega stream.
It uses the same video encoding protocol as the Flink applications in this project.
It currently does not support chunking so each image must be less than 1 MB after JSON encoding.

To start it:
```
export PRAVEGA_CONTROLLER_URI=tcp://127.0.0.1:9090
export OUTPUT_STREAM_NAME=video1
./gradlew camera-recorder:run
```

See (AppConfiguration.java)[camera-recorder/src/main/java/io/pravega/example/camerarecorder/AppConfiguration.java]
for more options.

## Video Player Application (optional)

The Video Player application reads images from a Pravega stream and displays
them in a window on the screen.
It uses the same video encoding protocol as the Flink applications in this project.
It currently does not support images split over multiple chunks.

To start it:
```
export PRAVEGA_CONTROLLER_URI=tcp://127.0.0.1:9090
export INPUT_STREAM_NAME=grid1
export CAMERA=1000
./gradlew video-player:run
```

See (AppConfiguration.java)[video-player/src/main/java/io/pravega/example/videoplayer/AppConfiguration.java]
for more options.

# Jupyter Hub

Jupyter Hub provides a Python notebook interface.
See [Jupyter Hub](jupyterhub/README.md).

# Chunking

See [Chunking](chunking.md).

# References

- <http://pravega.io/>

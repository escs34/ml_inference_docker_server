#!/bin/bash

curl -fsSL https://get.docker.com/ | sh

docker build -t tensor_docker ./ml_inference_docker_server/.

docker run -itd --name tensor -p 8078:8078 tensor_docker

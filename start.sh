#!/bin/bash

git clone https://github.com/escs34/ml_inference_docker_server.git


curl -fsSL https://get.docker.com/ | sh

sudo docker build -t tensor_docker ./ml_inference_docker_server/.

sudo docker run -itd -p 8078:8078 tensor_docker

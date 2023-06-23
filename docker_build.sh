#!/bin/bash

docker build --target python -t spark-driver . && \
docker build --target spark -t apache-spark . 
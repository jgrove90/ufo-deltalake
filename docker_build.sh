#!/bin/bash

docker build --target python -t ufo-deltalake . && \
docker build --target spark -t apache-spark .
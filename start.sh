#!/bin/bash

# build docker images
docker build --target python -t ufo-deltalake . && \
docker build --target spark -t apache-spark . && \
# start containers
docker compose up -d && \
# enter spark-driver container and submit job
docker exec -it spark-driver bash -c "spark-submit --properties-file ./src/spark/spark-defaults.conf ./src/start_pipeline.py"

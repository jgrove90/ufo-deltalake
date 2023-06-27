#!/bin/bash

# Stop/remove containers
docker compose down

# Remove images
docker rmi apache-spark ufo-deltalake
#!/bin/bash

spark-submit \
  --properties-file ./src/spark/spark-defaults.conf \
  ./src/start_pipeline.py
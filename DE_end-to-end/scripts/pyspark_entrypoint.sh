#!/bin/bash
set -e

if [ -e "/opt/bitnami/spark/requirements/pyspark_requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install -r /opt/bitnami/spark/requirements/pyspark_requirements.txt
fi

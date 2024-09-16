#!/bin/bash
set -e

if [ -e "/opt/airflow/pyspark_requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install -r pyspark_requirements.txt
fi

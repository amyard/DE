FROM apache/airflow:2.10.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         gcc python3-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Python dependencies
# For fixing problem with installing package we will intall it separately
RUN pip install -U scikit-learn
COPY requirements.txt .
RUN python -m pip install -U pip setuptools && pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy connection.json file into the container
COPY scripts/connection.json /opt/airflow/scripts/connection.json

# Import connections into Airflow
RUN airflow connections import /opt/airflow/scripts/connection.json
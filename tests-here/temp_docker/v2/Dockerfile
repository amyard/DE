# Use the official Apache Airflow image with Python 3.12
FROM apache/airflow:2.10.1-python3.12

# Switch to root user to install dependencies
USER root

# Install dependencies: gcc, Python dev, OpenJDK 17, curl, and Spark dependencies
RUN apt-get update && \
    apt-get install -y \
    gcc \
    python3-dev \
    openjdk-17-jdk \
    curl && \
    apt-get clean

# Set JAVA_HOME environment variable for OpenJDK 17
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Apache Spark
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set SPARK_HOME environment variable
ENV SPARK_HOME /opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Switch back to the airflow user
USER airflow

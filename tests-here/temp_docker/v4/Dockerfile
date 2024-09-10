FROM apache/airflow:2.10.1-python3.12

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable for OpenJDK 17
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

USER airflow
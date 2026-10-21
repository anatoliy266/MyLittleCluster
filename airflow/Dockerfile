# FROM apache/airflow:2.8.0

# USER root

# RUN apt-get update && \
#     apt-get install -y --no-install-recommends openjdk-11-jdk curl && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV SPARK_VERSION=3.0.0
# ENV SPARK_HADOOP_VERSION=3.2
# ENV SPARK_HOME=/opt/spark

# RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
#     | tar -xz -C /opt/ && \
#     mv /opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} ${SPARK_HOME}

# ENV PATH=${SPARK_HOME}/bin:${PATH}
# ENV HADOOP_CONF_DIR=/etc/hadoop
# ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-*-src.zip

# USER airflow

# COPY requirements.txt /requirements.txt

# RUN pip install --no-cache-dir -r /requirements.txt
FROM apache/airflow:3.2.1-python3.14

USER root

# Добавляем флаги, чтобы apt-get не падал, если какие-то старые сервера Debian недоступны
RUN apt-get update --allow-releaseinfo-change || true && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl build-essential libkrb5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=4.2.0
ENV SPARK_HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
# https://downloads.apache.org/spark/spark-4.2.0-preview5/spark-4.2.0-preview5-bin-hadoop3.tgz      
RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} ${SPARK_HOME}

ENV PATH=${SPARK_HOME}/bin:${PATH}
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-*-src.zip

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
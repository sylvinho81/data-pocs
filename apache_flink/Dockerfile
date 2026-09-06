# Flink + PyFlink + Kafka/Iceberg/AWS connector JARs
FROM flink:1.18.1-scala_2.12-java11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
      curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-flink.txt /tmp/requirements-flink.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-flink.txt \
 && rm -f /tmp/requirements-flink.txt

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG HADOOP_UBER_VERSION=2.8.3-10.0

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"

WORKDIR /opt/flink
USER flink

COPY --chown=flink:flink src/flink/ /opt/flink/usrlib/
COPY --chown=flink:flink scripts/submit_job.sh /opt/flink/usrlib/submit_job.sh

ENV FLINK_LIB_DIR=/opt/flink/lib \
    PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

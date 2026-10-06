# CommitReport writer. Compiled against the same Iceberg runtime the Flink image uses.
FROM eclipse-temurin:17-jdk-jammy AS commit-metrics-reporter

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.11.0

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY src/metrics/JsonFileMetricsReporter.java /src/com/datapoc/iceberg/JsonFileMetricsReporter.java

RUN curl -fsSL -o /tmp/iceberg-flink-runtime.jar \
      "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
 && mkdir -p /tmp/classes \
 && javac --release 17 -cp /tmp/iceberg-flink-runtime.jar -d /tmp/classes \
      com/datapoc/iceberg/JsonFileMetricsReporter.java \
 && jar cf /tmp/iceberg-commit-metrics-reporter.jar -C /tmp/classes .

# Flink + PyFlink + Kafka/Iceberg/AWS connector JARs
FROM flink:2.1.3-scala_2.12-java17

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
      python3-venv \
      build-essential \
      curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# apache-flink-libraries is a ~310MB sdist. Keep the unpack on the image disk
# (not a tiny overlay tmpfs) and isolate it from distro Python packages.
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    TMPDIR=/var/tmp/pip-build \
    PYFLINK_VENV=/opt/flink/venv
COPY requirements-flink.txt /tmp/requirements-flink.txt
RUN mkdir -p "${TMPDIR}" \
 && python3 -m venv "${PYFLINK_VENV}" \
 && "${PYFLINK_VENV}/bin/pip" install --upgrade pip setuptools wheel \
 && "${PYFLINK_VENV}/bin/pip" install --no-cache-dir -r /tmp/requirements-flink.txt \
 && rm -rf /tmp/requirements-flink.txt "${TMPDIR}"

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.1
ARG HADOOP_UBER_VERSION=2.8.3-10.0

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"

COPY --from=commit-metrics-reporter /tmp/iceberg-commit-metrics-reporter.jar /opt/flink/lib/iceberg-commit-metrics-reporter.jar

WORKDIR /opt/flink
USER flink

COPY --chown=flink:flink src/flink/ /opt/flink/usrlib/
COPY --chown=flink:flink scripts/submit_job.sh /opt/flink/usrlib/submit_job.sh

ENV FLINK_LIB_DIR=/opt/flink/lib \
    PATH="/opt/flink/venv/bin:${PATH}" \
    PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python

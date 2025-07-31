#!/bin/bash

export HADOOP_HOME=/opt/hadoop
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar
export JAVA_HOME=/usr/local/openjdk-11

# Wait for MySQL
while ! nc -z mariadb 3306; do
  echo "Waiting for MySQL to be ready..."
  sleep 2
done

# Initialize schema
/opt/hive-metastore/bin/schematool -dbType mysql -initSchema --verbose

# Start Metastore
/opt/hive-metastore/bin/start-metastore 
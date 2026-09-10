# Build context is infra/ so this image can pick up both spark/conf and the
# shared hadoop/conf that tells Spark where the namenode lives.

# Spark's built-in Hive client is 2.3.10 and cannot complete a 4.x metastore
# handshake. Spark 4.0 can drive a 4.0.0 client through an isolated classloader,
# but only if it is handed those jars -- so they are lifted out of the Hive
# image here rather than fetched from Maven at job start.
FROM apache/hive:4.0.0 AS hive-client

FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

ENV SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/spark/conf \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

# Pre-stage the Kafka connector and its transitive deps into the image jars dir.
# Resolving these at spark-submit time via --packages needs Maven Central on
# every job start, which makes the Day 5 demo dependent on network weather.
ARG SPARK_VERSION=4.0.4
ARG SCALA_BIN=2.13
ARG KAFKA_CLIENTS_VERSION=3.9.0
ARG COMMONS_POOL_VERSION=2.12.0

RUN cd /opt/spark/jars \
 && M2=https://repo1.maven.org/maven2 \
 && curl -fsSLO "$M2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BIN}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VERSION}.jar" \
 && curl -fsSLO "$M2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BIN}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VERSION}.jar" \
 && curl -fsSLO "$M2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
 && curl -fsSLO "$M2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar"

# Kept in /opt/hive-jars rather than /opt/spark/jars on purpose: these are loaded
# by an isolated classloader and must not collide with Spark's own Hive 2.3.10.
COPY --from=hive-client /opt/hive/lib/ /opt/hive-jars/

COPY hadoop/conf/core-site.xml hadoop/conf/hdfs-site.xml /opt/spark/conf/
COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/finsight /opt/finsight/logs && chmod -R 777 /opt/finsight
WORKDIR /opt/finsight

# Apache publishes no arm64 build of apache/hadoop, so we assemble one from the
# binary tarball. Hadoop itself is pure Java; only the optional native codecs
# (libhadoop.so) are architecture-specific, and HDFS runs fine without them.
FROM eclipse-temurin:17-jre-noble

ARG HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps net-tools less \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
      -o /tmp/hadoop.tar.gz \
 && mkdir -p /opt/hadoop \
 && tar -xzf /tmp/hadoop.tar.gz -C /opt/hadoop --strip-components=1 \
 && rm /tmp/hadoop.tar.gz \
 && rm -rf /opt/hadoop/share/doc

COPY conf/core-site.xml conf/hdfs-site.xml $HADOOP_CONF_DIR/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Silence the "unable to load native-hadoop library" warning on every command.
ENV HADOOP_OPTS="-Djava.library.path=/opt/hadoop/lib/native -Dhadoop.root.logger=WARN,console"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

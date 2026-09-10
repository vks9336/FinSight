# Kafka Connect worker carrying the HDFS 3 Sink (spec 6.3).
#
# Note on licensing: kafka-connect-hdfs3 is a Confluent proprietary connector
# under a 30-day evaluation licence. spark/hdfs_sink.py implements the same
# Parquet landing layout under Apache 2.0 and is the supported fallback.
FROM confluentinc/cp-kafka-connect:7.7.1

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs3:latest \
 && chown -R appuser:appuser /usr/share/confluent-hub-components

# The connector bundles Hadoop client jars but reads cluster coordinates from
# hadoop.conf.dir, which the connector config points at this path.
COPY hadoop/conf/core-site.xml hadoop/conf/hdfs-site.xml /etc/hadoop/

USER appuser

# Build context is infra/.
#
# The stock apache/hive image ships no JDBC driver, so the metastore cannot
# reach Postgres out of the box. Fetch it in a throwaway stage to keep curl and
# its CA bundle out of the final image.
FROM eclipse-temurin:17-jre-noble AS driver
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o /tmp/postgresql.jar

FROM apache/hive:4.0.0

USER root
COPY --from=driver /tmp/postgresql.jar /opt/hive/lib/postgresql.jar

# The image entrypoint copies anything in HIVE_CUSTOM_CONF_DIR over the
# packaged conf before launching the service.
ENV HIVE_CUSTOM_CONF_DIR=/hive_custom_conf
RUN mkdir -p /hive_custom_conf
COPY hive/conf/hive-site.xml /hive_custom_conf/hive-site.xml
COPY hadoop/conf/core-site.xml /hive_custom_conf/core-site.xml
COPY hadoop/conf/hdfs-site.xml /hive_custom_conf/hdfs-site.xml

USER hive

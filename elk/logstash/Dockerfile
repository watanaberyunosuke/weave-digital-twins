# syntax=docker/dockerfile:1
FROM docker.elastic.co/logstash/logstash:9.5.4
# Same driver version as PG_DRIVER in scripts/download-flink-connectors.sh.
# Jars in logstash-core/lib/jars are on the classpath, so the jdbc inputs need
# no jdbc_driver_library path.
ADD --chmod=644 \
    --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /usr/share/logstash/logstash-core/lib/jars/postgresql-42.7.4.jar
COPY --chmod=755 reset-on-target-change.sh /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/reset-on-target-change.sh"]

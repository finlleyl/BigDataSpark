FROM apache/spark:3.5.0

USER root
RUN mkdir -p /opt/spark/jars-extra && \
    curl -L -o /opt/spark/jars-extra/postgresql-42.7.3.jar \
      "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" && \
    curl -L -o /opt/spark/jars-extra/clickhouse-jdbc-0.6.0-all.jar \
      "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0/clickhouse-jdbc-0.6.0-all.jar"
USER spark

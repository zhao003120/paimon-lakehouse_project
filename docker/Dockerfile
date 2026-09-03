FROM flink:1.18.1-scala_2.12-java11

USER root

# Flink S3 filesystem plugin - must go in plugins/ directory
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Install curl + MySQL client (for StarRocks queries)
RUN apt-get update && apt-get install -y curl default-mysql-client && rm -rf /var/lib/apt/lists/*

# ================================================================
# Required JARs for Paimon + S3 (MinIO)
# ================================================================

# 1. Paimon Flink connector (read/write Paimon tables)
RUN curl -L -o /opt/flink/lib/paimon-flink-1.18-0.8.2.jar \
    https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.2/paimon-flink-1.18-0.8.2.jar

# 2. Paimon S3 filesystem plugin (Paimon's own S3 implementation)
RUN curl -L -o /opt/flink/lib/paimon-s3-0.8.2.jar \
    https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/0.8.2/paimon-s3-0.8.2.jar

# 3. Hadoop uber JAR (provides hadoop-common, hadoop-hdfs, hadoop-aws)
#    Required by Paimon S3 internally (uses Hadoop S3AFileSystem)
RUN curl -L -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Verify all JARs are present
RUN ls -la /opt/flink/lib/paimon-*.jar /opt/flink/lib/flink-shaded-hadoop-*.jar

WORKDIR /app
COPY . /app/

CMD ["bash"]

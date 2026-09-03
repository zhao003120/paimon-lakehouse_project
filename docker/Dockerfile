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
RUN curl --fail -L -o /opt/flink/lib/paimon-flink-1.18-0.8.2.jar \
    https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.2/paimon-flink-1.18-0.8.2.jar

# 2. Paimon S3 filesystem plugin (Paimon's own S3 implementation)
RUN curl --fail -L -o /opt/flink/lib/paimon-s3-0.8.2.jar \
    https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/0.8.2/paimon-s3-0.8.2.jar

# 3. Hadoop client API (provides org.apache.hadoop.conf.Configuration)
RUN curl --fail -L -o /opt/flink/lib/hadoop-client-api-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar

# 4. Hadoop client runtime (provides runtime implementation, includes transitive deps)
RUN curl --fail -L -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# 5. Hadoop AWS (provides S3AFileSystem for Paimon S3)
RUN curl --fail -L -o /opt/flink/lib/hadoop-aws-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

# 6. AWS Java SDK bundle (required by hadoop-aws)
RUN curl --fail -L -o /opt/flink/lib/aws-java-sdk-bundle-1.12.267.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.267/aws-java-sdk-bundle-1.12.267.jar

# Verify all JARs are present and non-empty (>1000 bytes)
RUN for f in /opt/flink/lib/paimon-flink-1.18-0.8.2.jar \
             /opt/flink/lib/paimon-s3-0.8.2.jar \
             /opt/flink/lib/hadoop-client-api-3.3.6.jar \
             /opt/flink/lib/hadoop-client-runtime-3.3.6.jar \
             /opt/flink/lib/hadoop-aws-3.3.6.jar \
             /opt/flink/lib/aws-java-sdk-bundle-1.12.267.jar; do \
      size=$(stat -c%s "$f"); \
      if [ "$size" -lt 1000 ]; then \
        echo "ERROR: $f is too small ($size bytes) - download may have failed"; \
        exit 1; \
      fi; \
      echo "OK: $f ($size bytes)"; \
    done

WORKDIR /app
COPY . /app/

CMD ["bash"]

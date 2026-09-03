FROM flink:1.18.1-scala_2.12-java11

USER root

# Flink S3 filesystem plugin - must go in plugins/ directory (not lib/)
# This registers the s3:// filesystem scheme
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Install curl + MySQL client (for StarRocks queries)
RUN apt-get update && apt-get install -y curl default-mysql-client && rm -rf /var/lib/apt/lists/*

# Download Paimon connector JAR (Flink 1.18 + Paimon 0.8)
RUN curl -L -o /opt/flink/lib/paimon-flink-1.18-0.8.2.jar \
    https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.2/paimon-flink-1.18-0.8.2.jar

WORKDIR /app
COPY . /app/

CMD ["bash"]

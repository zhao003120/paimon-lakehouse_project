FROM flink:1.18.1-scala_2.12-java11

USER root

# Flink S3 filesystem plugin (for MinIO)
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/lib/

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app/

CMD ["bash"]

FROM flink:1.18.1-scala_2.12-java11

USER root

# Install Maven
ARG MAVEN_VERSION=3.9.6
RUN cd /opt && \
    curl -fsSL https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz -o maven.tar.gz && \
    tar xzf maven.tar.gz && \
    rm maven.tar.gz && \
    ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/maven

ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

# Install Python (for MinIO client scripts)
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Install mc (MinIO Client)
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Flink S3 filesystem plugin
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/lib/

# Set working directory
WORKDIR /app

# Copy project
COPY . /app/

# Default command
CMD ["bash"]

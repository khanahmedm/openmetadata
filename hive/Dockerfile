# Use OpenJDK base image
FROM openjdk:8-jre-slim

# Set Hive version
ENV HIVE_VERSION=3.1.2
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

# Install dependencies (curl, tar, bash, etc.)
RUN apt-get update && apt-get install -y curl tar bash postgresql-client && rm -rf /var/lib/apt/lists/*

# Download and extract Apache Hive
RUN curl -fsSL https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz -o /tmp/hive.tar.gz && \
    tar -xzf /tmp/hive.tar.gz -C /opt && \
    mv /opt/apache-hive-$HIVE_VERSION-bin $HIVE_HOME && \
    rm /tmp/hive.tar.gz

ENV HADOOP_VERSION=3.3.5
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN curl -fsSL https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm /tmp/hadoop.tar.gz



# Copy PostgreSQL JDBC driver (required for Hive to connect to Postgres)
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -o $HIVE_HOME/lib/postgresql.jar

# Create conf directory explicitly
RUN mkdir -p $HIVE_HOME/conf

# Add hive-site.xml config file (you will mount or customize it later)
COPY hive-site.xml $HIVE_HOME/conf/hive-site.xml

# Expose Hive Metastore Thrift port
EXPOSE 9083

# Entry point: Initialize schema if needed, then start metastore service
CMD ["bash", "-c", "\
    schematool -dbType postgres -initSchema && \
    hive --service metastore"]

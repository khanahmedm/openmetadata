FROM openjdk:17-jdk-slim

ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3

RUN apt-get update && apt-get install -y curl python3 python3-pip bash wget && rm -rf /var/lib/apt/lists/*

# Download and extract Spark binaries
RUN curl -sSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt

ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=$PATH:$SPARK_HOME/bin

RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar

RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar
# Install Python dependencies
RUN pip3 install pyspark==3.3.2 delta-spark==2.2.0

RUN pip3 install openmetadata-ingestion

CMD [ "bash" ]

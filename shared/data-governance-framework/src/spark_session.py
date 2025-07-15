import os
import logging
from pyspark.sql import SparkSession

def start_spark_session(app_name="Delta + Hive + MinIO", logger: logging.Logger = None) -> SparkSession:
    if logger is None:
        raise ValueError("Logger instance must be provided to start_spark_session")

    try:
        jar_dir = "/home/jovyan/jars"
        all_jars = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
        jars_csv = ",".join(all_jars)

        logger.info(f"Found {len(all_jars)} JARs in {jar_dir}")
        logger.debug(f"JARs: {all_jars}")

        logger.info("Starting Spark session...")
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jars_csv) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "4") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session started successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to start Spark session: {e}", exc_info=True)
        raise

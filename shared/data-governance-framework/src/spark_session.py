import os
import logging
from pyspark.sql import SparkSession


def start_spark_session(app_name: str = "Delta + Hive + MinIO", logger: logging.Logger = None) -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Delta Lake, Hive Metastore, and MinIO (S3-compatible).

    Args:
        app_name (str): Name of the Spark application.
        logger (logging.Logger): Logger instance for capturing logs.

    Returns:
        SparkSession: Configured Spark session with Hive and Delta support.

    Raises:
        ValueError: If no logger is provided.
        Exception: If session creation fails or required jars are missing.
    """
    if logger is None:
        raise ValueError("Logger instance must be provided to start_spark_session")

    try:
        jar_dir = "/home/jovyan/jars"

        try:
            all_jars = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
            if not all_jars:
                raise FileNotFoundError(f"No JARs found in {jar_dir}")
        except Exception as e:
            logger.error(f"Error reading JAR directory '{jar_dir}': {e}", exc_info=True)
            raise

        jars_csv = ",".join(all_jars)
        logger.info(f"Found {len(all_jars)} JAR(s) in {jar_dir}")
        logger.debug(f"JARs: {all_jars}")

        try:
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

            logger.info("✅ Spark session started successfully")
            return spark

        except Exception as e:
            logger.error("❌ Failed to initialize Spark session", exc_info=True)
            raise

    except Exception as final_error:
        logger.critical("🚨 Unable to start Spark session. See error logs for details.")
        raise final_error

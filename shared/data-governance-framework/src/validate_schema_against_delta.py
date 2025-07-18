import boto3
import yaml
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

def validate_schema_against_delta(config_loader, spark: SparkSession, logger: logging.Logger) -> Dict[str, Any]:
    logger.info("Starting schema vs. Delta table column validation")

    # Load schema YAML from MinIO
    schema_path = config_loader.get_schema_path()
    schema_path_clean = schema_path.replace("s3a://", "")
    schema_bucket = schema_path_clean.split("/")[0]
    schema_key = "/".join(schema_path_clean.split("/")[1:])

    logger.info(f"Schema file: {schema_path}")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # adjust if different
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    try:
        schema_obj = s3.get_object(Bucket=schema_bucket, Key=schema_key)
        schema_yaml = yaml.safe_load(schema_obj["Body"].read().decode("utf-8"))

        # Derive class name from target_table
        target_table = config_loader.get_target_table()
        class_name = target_table.split(".")[-1]

        logger.info(f"Derived schema class name: {class_name}")

        schema_class = schema_yaml.get("classes", {}).get(class_name, {})
        if not schema_class:
            raise ValueError(f"Class '{class_name}' not found in schema YAML.")

        schema_columns = list(schema_class.get("slots", []))

        logger.info("Schema loaded and class found successfully")
        logger.debug(f"Schema columns: {schema_columns}")
    except Exception as e:
        logger.error(f"Failed to load or parse schema YAML: {e}", exc_info=True)
        raise

    # Construct Delta table path
    delta_path = f"s3a://cdm-lake/silver/{target_table.replace('.', '/')}"
    logger.info(f"Reading Delta table from path: {delta_path}")

    try:
        df = spark.read.format("delta").load(delta_path)
        delta_columns = df.columns

        logger.info("Delta table loaded successfully")
        logger.debug(f"Delta columns: {delta_columns}")
    except Exception as e:
        logger.error(f"Failed to read Delta table: {e}", exc_info=True)
        raise

    # Compare columns
    missing_in_table = [col for col in schema_columns if col not in delta_columns]
    extra_in_table = [col for col in delta_columns if col not in schema_columns]

    logger.info("Comparison between schema and Delta table columns completed")
    if missing_in_table:
        logger.warning(f"Missing columns in Delta table: {missing_in_table}")
    else:
        logger.info("All schema columns are present in the Delta table")

    if extra_in_table:
        logger.warning(f"Extra columns in Delta table not in schema: {extra_in_table}")
    else:
        logger.info("No extra columns in Delta table")

    print(f"📂 Delta table columns: {delta_columns}")
    print(f"📄 Schema columns: {schema_columns}")
    print("🔍 Comparison results:")
    if missing_in_table:
        print(f"❌ Missing in Delta table: {missing_in_table}")
    else:
        print("✅ All schema columns are present in Delta table")
    if extra_in_table:
        print(f"⚠️ Extra in Delta table not in schema: {extra_in_table}")
    else:
        print("✅ No extra columns in Delta table")

    return {
        "delta_columns": delta_columns,
        "schema_columns": schema_columns,
        "missing_in_table": missing_in_table,
        "extra_in_table": extra_in_table,
    }

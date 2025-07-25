import boto3
import yaml
import logging
from typing import Dict, Any
import io

def validate_schema_against_file(config_loader, logger: logging.Logger) -> Dict[str, Any]:
    """
    Validate that the output file's header columns match the expected schema defined in a LinkML YAML file.

    Args:
        config_loader: Instance of ConfigLoader to fetch configuration and schema path.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, Any]: Dictionary with keys:
            - file_columns: List of columns from file header
            - schema_columns: List of expected columns from LinkML schema
            - missing_in_file: Columns in schema but missing in file
            - extra_in_file: Columns in file but not in schema
    """
    logger.info("Starting schema vs. file validation")

    # Step 1: Setup MinIO client
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}", exc_info=True)
        return {}

    # Step 2: Extract header columns from output file
    try:
        output_file_config = config_loader.get_output_file()
        output_file_path = output_file_config["output_file_path"]
        delimiter = output_file_config.get("delimiter", "\t")
        ignore_first_line = output_file_config.get("ignore_first_line", "no").strip().lower() == "yes"

        logger.info(f"Output file: {output_file_path}")
        logger.debug(f"Delimiter: {delimiter}, Ignore first line: {ignore_first_line}")

        path_clean = output_file_path.replace("s3a://", "")
        bucket = path_clean.split("/")[0]
        key = "/".join(path_clean.split("/")[1:])

        response = s3.get_object(Bucket=bucket, Key=key)
        body_stream = io.TextIOWrapper(response["Body"], encoding="utf-8")
        if ignore_first_line:
            next(body_stream)
        header_line = next(body_stream).strip()
        header_columns = [col.strip() for col in header_line.split(delimiter)]

        logger.info("Header columns successfully extracted from output file")
        logger.debug(f"Header columns: {header_columns}")

    except Exception as e:
        logger.error(f"❌ Failed to read header from output file: {e}", exc_info=True)
        return {
            "file_columns": [],
            "schema_columns": [],
            "missing_in_file": [],
            "extra_in_file": [],
            "error": f"Failed to extract file header: {e}"
        }

    # Step 3: Load and parse schema YAML
    try:
        schema_path = config_loader.get_schema_path()
        schema_path_clean = schema_path.replace("s3a://", "")
        schema_bucket = schema_path_clean.split("/")[0]
        schema_key = "/".join(schema_path_clean.split("/")[1:])

        logger.info(f"Schema file: {schema_path}")

        schema_obj = s3.get_object(Bucket=schema_bucket, Key=schema_key)
        schema_yaml = yaml.safe_load(schema_obj["Body"].read().decode("utf-8"))

        target_table = config_loader.get_target_table()
        class_name = target_table.split(".")[-1]

        logger.info(f"Derived schema class name: {class_name}")

        schema_class = schema_yaml.get("classes", {}).get(class_name, {})
        if not schema_class:
            raise ValueError(f"Class '{class_name}' not found in schema YAML")

        schema_columns = list(schema_class.get("slots", []))
        logger.info("Schema loaded and class found successfully")
        logger.debug(f"Schema columns: {schema_columns}")

    except Exception as e:
        logger.error(f"❌ Failed to load or parse schema YAML: {e}", exc_info=True)
        return {
            "file_columns": header_columns,
            "schema_columns": [],
            "missing_in_file": [],
            "extra_in_file": [],
            "error": f"Schema load error: {e}"
        }

    # Step 4: Compare schema vs. file columns
    try:
        missing_in_file = [col for col in schema_columns if col not in header_columns]
        extra_in_file = [col for col in header_columns if col not in schema_columns]

        logger.info("Comparison between schema and file header completed")
        if missing_in_file:
            logger.warning(f"Missing columns in file: {missing_in_file}")
        else:
            logger.info("All schema columns are present in the file")

        if extra_in_file:
            logger.warning(f"Extra columns in file not in schema: {extra_in_file}")
        else:
            logger.info("No extra columns in the file")

        # For console/debug display
        print(f"🔹 Header columns from file:\n{header_columns}\n")
        print(f"✅ Columns in schema class '{class_name}':\n{schema_columns}\n")
        print("🔍 Comparison results:")
        if missing_in_file:
            print(f"❌ Missing in file: {missing_in_file}")
        else:
            print("✅ All schema columns are present in file")
        if extra_in_file:
            print(f"⚠️ Extra columns in file not in schema: {extra_in_file}")
        else:
            print("✅ No extra columns in file")

        return {
            "file_columns": header_columns,
            "schema_columns": schema_columns,
            "missing_in_file": missing_in_file,
            "extra_in_file": extra_in_file
        }

    except Exception as e:
        logger.error(f"❌ Error during schema vs. file comparison: {e}", exc_info=True)
        return {
            "file_columns": header_columns,
            "schema_columns": schema_columns,
            "missing_in_file": [],
            "extra_in_file": [],
            "error": f"Comparison error: {e}"
        }

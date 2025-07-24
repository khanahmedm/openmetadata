from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel
import logging
from typing import Dict, Any
import json

def run_validations_from_config_spark(spark: SparkSession, config_loader, logger: logging.Logger) -> Dict[str, Any]:
    """
    Perform column-level validations on a CSV file using PySpark.

    Validation rules are dynamically loaded from the config and include:
    - not_null
    - regex_match
    - numeric_range
    - allowed_values

    Args:
        spark (SparkSession): Active Spark session.
        config_loader: Instance of ConfigLoader.
        logger (logging.Logger): Logger for structured output.

    Returns:
        Dict[str, Any]: Dictionary with validation summary:
            - file_path (str): Path to the validated file
            - validation_errors (list): List of error messages
            - success (bool): True if all validations passed
    """
    try:
        output_config = config_loader.get_output_file()
        output_file_path = output_config["output_file_path"]
        delimiter = output_config.get("delimiter", "\t")

        logger.info(f"📄 Starting validation on file: {output_file_path}")
        logger.debug(f"Delimiter: {delimiter}")

        validations = config_loader.get_validations()
        logger.debug(f"Validation rules: {validations}")
    except Exception as e:
        logger.error(f"❌ Failed to load validation config: {e}", exc_info=True)
        return {"file_path": None, "validation_errors": [str(e)], "success": False}


    # Narrow columns and avoid caching in RAM
    try:
        columns_to_validate = list(set(rule["column"] for rule in validations))
        df = spark.read.option("header", "true").option("delimiter", delimiter).csv(output_file_path)
        df = df.select(*columns_to_validate).persist(StorageLevel.DISK_ONLY)
        df.count()  # Trigger persistence
    except Exception as e:
        logger.error(f"❌ Failed to load or parse file '{output_file_path}': {e}", exc_info=True)
        return {"file_path": output_file_path, "validation_errors": [str(e)], "success": False}

    validation_errors = []
    passed_validations = set()

    # Apply validations
    for rule in validations:
        column = rule["column"]
        vtype = rule["validation_type"]
        error_msg = rule.get("error_message", f"Validation failed for {column}")
        rule_key = f"{column} - {vtype}"

        if column not in df.columns:
            msg = f"⚠️ Skipping unknown column: {column}"
            logger.warning(msg)
            validation_errors.append(msg)
            continue

        try:
            if vtype == "not_null":
                invalid_df = df.filter(col(column).isNull() | (col(column) == ""))
            elif vtype == "regex_match":
                pattern = rule.get("pattern")
                if not pattern:
                    raise ValueError(f"Missing pattern for regex_match on column {column}")
                invalid_df = df.filter(~col(column).rlike(pattern))
            elif vtype == "numeric_range":
                min_val = rule.get("min")
                max_val = rule.get("max")
                invalid_df = df.filter(
                    (min_val is not None and col(column).cast("double") < min_val) |
                    (max_val is not None and col(column).cast("double") > max_val)
                )
            elif vtype == "allowed_values":
                allowed = rule.get("values", [])
                invalid_df = df.filter(~col(column).isin(allowed))
            else:
                msg = f"⚠️ Unknown validation type: {vtype}"
                logger.warning(msg)
                validation_errors.append(msg)
                continue
        except Exception as e:
            msg = f"❌ Failed to apply validation '{rule_key}': {e}"
            logger.error(msg, exc_info=True)
            validation_errors.append(msg)
            continue

        # Collect sample violations
        try:
            sample = invalid_df.select(column).limit(5).collect()
        except Exception as e:
            msg = f"❌ Failed to collect sample violations for rule '{rule_key}': {e}"
            logger.warning(msg, exc_info=True)
            validation_errors.append(msg)
            continue

        if sample:
            msg = f"❌ Violations found for rule: {rule_key} → {error_msg}"
            logger.error(msg)
            sample_dict = [row.asDict() for row in sample]
            logger.error("Sample violations (up to 5 rows):")
            for row in sample_dict:
                logger.error(json.dumps(row, ensure_ascii=False))
            validation_errors.append(msg)
        else:
            passed_validations.add(rule_key)


    logger.info("Validation Summary:")
    if validation_errors:
        logger.warning(f"{len(validation_errors)} validation issue(s) found.")
    else:
        logger.info("All validation rules passed.")

    return {
        "file_path": output_file_path,
        "validation_errors": validation_errors,
        "success": len(validation_errors) == 0
    }

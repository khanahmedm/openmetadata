from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel
import logging
from typing import Dict, Any

def run_validations_from_config_spark(spark: SparkSession, config_loader, logger: logging.Logger) -> Dict[str, Any]:
    output_config = config_loader.get_output_file()
    output_file_path = output_config["output_file_path"]
    delimiter = output_config.get("delimiter", "\t")

    logger.info(f"Starting validation on file: {output_file_path}")
    logger.debug(f"Delimiter: {delimiter}")

    validations = config_loader.get_validations()
    logger.debug(f"Validation rules: {validations}")

    # Narrow columns and avoid caching in RAM
    columns_to_validate = list(set(rule["column"] for rule in validations))
    df = spark.read.option("header", "true").option("delimiter", delimiter).csv(output_file_path)
    df = df.select(*columns_to_validate).persist(StorageLevel.DISK_ONLY)
    df.count()  # Materialize persisted DF

    validation_errors = []
    passed_validations = set()

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

        if vtype == "not_null":
            invalid_df = df.filter(col(column).isNull() | (col(column) == ""))
        elif vtype == "regex_match":
            pattern = rule.get("pattern")
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

        sample = invalid_df.select(column).limit(5).collect()
        if sample:
            msg = f"❌ Violations found for rule: {rule_key} → {error_msg}"
            logger.error(msg)
            sample_dict = [row.asDict() for row in sample]
            logger.error("Sample violations (up to 5 rows):")
            for row in sample_dict:
                #logger.error(row)
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

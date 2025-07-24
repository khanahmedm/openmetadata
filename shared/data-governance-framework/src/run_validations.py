import boto3
import csv
import io
import logging
import re
from typing import Dict, Any


def run_validations_from_config(config_loader, logger: logging.Logger) -> Dict[str, Any]:
    """
    Perform file-based validations defined in the config on a tabular file stored in MinIO (S3).

    Supported validation types:
        - not_null
        - regex_match
        - numeric_range

    Args:
        config_loader: ConfigLoader instance with the loaded config JSON.
        logger (logging.Logger): Logger instance with pipeline context.

    Returns:
        Dict[str, Any]: Validation results including:
            - file_path (str)
            - validation_errors (List[str])
            - success (bool)
    """
    try:
        output_config = config_loader.get_output_file()
        output_file_path = output_config["output_file_path"]
        delimiter = output_config.get("delimiter", "\t")
        ignore_first_line = output_config.get("ignore_first_line", "no").strip().lower() == "yes"

        logger.info(f"📄 Starting validation on file: {output_file_path}")
        logger.debug(f"Delimiter: {delimiter}, Ignore first line: {ignore_first_line}")

        validations = config_loader.get_validations()
        logger.debug(f"Validation rules: {validations}")
    except Exception as e:
        logger.error(f"❌ Failed to read config: {e}", exc_info=True)
        return {"file_path": None, "validation_errors": [str(e)], "success": False}

    # Parse S3 path
    try:
        path_clean = output_file_path.replace("s3a://", "")
        bucket = path_clean.split("/")[0]
        key = "/".join(path_clean.split("/")[1:])

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        response = s3.get_object(Bucket=bucket, Key=key)
        stream = io.TextIOWrapper(response["Body"], encoding="utf-8")
        reader = csv.reader(stream, delimiter=delimiter)

        header = next(reader)
        col_index = {name: idx for idx, name in enumerate(header)}
        logger.debug(f"Header columns: {header}")

    except Exception as e:
        logger.error(f"❌ Failed to read file from MinIO or parse header: {e}", exc_info=True)
        return {"file_path": output_file_path, "validation_errors": [str(e)], "success": False}

    validation_errors = []
    passed_validations = []
    row_num = 1

    print("\n🔍 Running validations:\n")
    logger.info("Running validations...")

    for row in reader:
        row_num += 1

        if ignore_first_line and row_num == 2:
            continue

        for rule in validations:
            col = rule["column"]
            vtype = rule["validation_type"]
            error_msg = rule.get("error_message", f"Validation failed for {col}")
            rule_key = f"{col} - {vtype}"

            if col not in col_index:
                msg = f"⚠️ Skipping unknown column: {col}"
                print(msg)
                logger.warning(msg)
                validation_errors.append(msg)
                continue

            try:
                value = row[col_index[col]].strip()
            except IndexError as e:
                msg = f"❌ Row {row_num}: Missing value for column '{col}'"
                logger.error(msg)
                validation_errors.append(msg)
                continue

            try:
                if vtype == "not_null":
                    if value == "":
                        msg = f"❌ Row {row_num}, Column '{col}' is NULL → {error_msg}"
                        print(msg)
                        logger.error(msg)
                        validation_errors.append(msg)
                    else:
                        passed_validations.append(rule_key)

                elif vtype == "regex_match":
                    pattern = rule.get("pattern")
                    if pattern and not re.match(pattern, value):
                        msg = f"❌ Row {row_num}, Column '{col}'='{value}' fails regex {pattern} → {error_msg}"
                        print(msg)
                        logger.error(msg)
                        validation_errors.append(msg)
                    else:
                        passed_validations.append(rule_key)

                elif vtype == "numeric_range":
                    try:
                        num_value = float(value)
                        min_val = rule.get("min")
                        max_val = rule.get("max")

                        if (min_val is not None and num_value < min_val) or \
                           (max_val is not None and num_value > max_val):
                            msg = f"❌ Row {row_num}, Column '{col}'={value} out of range ({min_val} to {max_val}) → {error_msg}"
                            print(msg)
                            logger.error(msg)
                            validation_errors.append(msg)
                        else:
                            passed_validations.append(rule_key)
                    except ValueError:
                        msg = f"❌ Row {row_num}, Column '{col}'='{value}' is not a number → {error_msg}"
                        print(msg)
                        logger.error(msg)
                        validation_errors.append(msg)

                else:
                    msg = f"⚠️ Unknown validation type '{vtype}' for column '{col}'"
                    print(msg)
                    logger.warning(msg)
                    validation_errors.append(msg)

            except Exception as e:
                msg = f"❌ Error validating row {row_num}, column '{col}': {e}"
                print(msg)
                logger.error(msg, exc_info=True)
                validation_errors.append(msg)

    # Summary
    passed_rules_summary = sorted(set(passed_validations))

    print("\n📋 Validation Summary:")
    logger.info("Validation summary:")
    if not validation_errors:
        print("✅ All validation rules ran successfully. No issues found.")
        logger.info("All validation rules passed. No issues found.")
    else:
        print(f"⚠️ Validation completed with {len(validation_errors)} issue(s). See above for details.")
        logger.warning(f"Validation completed with {len(validation_errors)} issue(s).")

    print("\n✅ Passed Rules:")
    for rule in passed_rules_summary:
        print(f"✔️ {rule}")
        logger.info(f"Passed: {rule}")

    return {
        "file_path": output_file_path,
        "validation_errors": validation_errors,
        "success": len(validation_errors) == 0
    }

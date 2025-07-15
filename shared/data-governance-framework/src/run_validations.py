import boto3
import csv
import io
import logging
import re
from typing import Dict, Any


def run_validations_from_config(config_loader, logger: logging.Logger) -> Dict[str, Any]:
    """
    Run validations defined in the config JSON on the output file.

    Args:
        config_loader: Instance of ConfigLoader.
        logger: Logger object with pipeline context.

    Returns:
        Dictionary containing success flag and list of validation errors.
    """
    output_config = config_loader.get_output_file()
    output_file_path = output_config["output_file_path"]
    delimiter = output_config.get("delimiter", "\t")
    ignore_first_line = output_config.get("ignore_first_line", "no").strip().lower() == "yes"

    logger.info(f"Starting validation on file: {output_file_path}")
    logger.debug(f"Delimiter: {delimiter}, Ignore first line: {ignore_first_line}")

    # Parse S3 bucket and key
    path_clean = output_file_path.replace("s3a://", "")
    bucket = path_clean.split("/")[0]
    key = "/".join(path_clean.split("/")[1:])

    # Load validation rules
    validations = config_loader.get_validations()
    logger.debug(f"Validation rules: {validations}")

    # MinIO client setup
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    # Read file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    #content = response["Body"].read().decode("utf-8")

    # Parse with CSV reader
    #reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    #rows = list(reader)

    stream = io.TextIOWrapper(response["Body"], encoding="utf-8")
    reader = csv.reader(stream, delimiter=delimiter)
    header = next(reader)
    col_index = {name: idx for idx, name in enumerate(header)}


    # Extract header and data
    #header = rows[0]
    #data_rows = rows[1:] if ignore_first_line else rows[1:]
    #col_index = {name: idx for idx, name in enumerate(header)}
    validation_errors = []

    passed_validations = []

    print("\n🔍 Running validations:\n")
    logger.info("Running validations...")
    logger.debug(f"Header columns: {header}")

    #for row_num, row in enumerate(data_rows, start=2 if ignore_first_line else 2):

    row_num = 1
    for row in reader:
        row_num += 1

        # Skip line if ignore_first_line is true and we're still on the first data row
        if ignore_first_line and row_num == 2:
            continue

        for rule in validations:
            col = rule["column"]
            if col not in col_index:
                warning_msg = f"⚠️ Skipping unknown column: {col}"
                print(warning_msg)
                logger.warning(warning_msg)
                validation_errors.append(warning_msg)
                continue

            value = row[col_index[col]].strip()
            vtype = rule["validation_type"]
            error_msg = rule.get("error_message", f"Validation failed for {col}")
            rule_key = f"{col} - {vtype}"

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

                    if (min_val is not None and num_value < min_val) or (max_val is not None and num_value > max_val):
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

        # (rest of the validation code)
        # row_num += 1

    # Prepare unique passed validations
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

# src/linkml_schema_validator.py

import logging
from typing import Dict, Any, List


def validate_config_columns_against_schema(
    config: Dict[str, Any],
    schema_columns: List[str],
    logger: logging.Logger
) -> Dict[str, List[str]]:
    """
    Validate that columns used in config sections exist in the LinkML schema.

    Args:
        config: The loaded config JSON (as dict).
        schema_columns: List of columns (slots) from the LinkML class.
        logger: Logger instance.

    Returns:
        A dict summarizing invalid columns per section.
    """
    logger.info("Starting validation of config columns against LinkML schema")

    invalid_columns = {
        "validations": [],
        "referential_integrity": [],
        "great_expectations_validations": [],
        "transformations": []
    }

    # Validations
    for i, rule in enumerate(config.get("validations", [])):
        col = rule.get("column")
        if col and col not in schema_columns:
            logger.warning(f"[validations[{i}]] Column '{col}' not found in schema")
            invalid_columns["validations"].append(col)

    # Referential Integrity
    for i, rule in enumerate(config.get("referential_integrity", [])):
        fk = rule.get("foreign_key")
        if fk and fk not in schema_columns:
            logger.warning(f"[referential_integrity[{i}]] Foreign key '{fk}' not in schema")
            invalid_columns["referential_integrity"].append(fk)

    # Great Expectations Validations
    for i, rule in enumerate(config.get("great_expectations_validations", [])):
        params = rule.get("params", {})
        col = params.get("column")
        if col and col not in schema_columns:
            logger.warning(f"[great_expectations_validations[{i}]] Column '{col}' not in schema")
            invalid_columns["great_expectations_validations"].append(col)

    # Transformations
    for i, rule in enumerate(config.get("transformations", [])):
        col = rule.get("column_name")
        if col and col not in schema_columns:
            logger.warning(f"[transformations[{i}]] Column '{col}' not in schema")
            invalid_columns["transformations"].append(col)

    logger.info("LinkML schema column validation complete")
    return invalid_columns

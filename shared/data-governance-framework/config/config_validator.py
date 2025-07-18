import logging
from .config_loader import ConfigLoader  # <-- Your class goes here
from .config_validator_core import ConfigValidator  # Separate file containing the original validator class
from .json_schema_validator import validate_against_schema
from jsonschema import ValidationError
from typing import Optional
from .linkml_schema_validator import validate_config_columns_against_schema
from src.validate_schema import validate_schema_against_file 

def run_config_validation(target_table: str, dry_run: bool = True, logger: Optional[logging.Logger] = None) -> bool:
    """
    Load and validate config from MinIO for a given target_table.

    Args:
        target_table (str): Target table name (used to resolve JSON filename).
        dry_run (bool): Whether to run in dry-run mode.
        logger (Logger): Optional external logger to reuse across modules.

    Returns:
        bool: True if valid, False if not.
    """
    # Setup logger
    #logger = logging.getLogger("config-validator")
    #logger.setLevel(logging.INFO)
    #logger.addHandler(logging.StreamHandler())
    if logger is None:
        # fallback to internal logger setup if not provided
        logger = setup_logger(
            log_dir="/tmp",
            logger_name="config_validator",
            pipeline_name="config_validation",
            target_table=target_table,
            schema="pangenome"
        )

    # Load config from MinIO
    loader = ConfigLoader(target_table_name=target_table, logger=logger)
    loader.load_config()
    config = loader.get_full_config()

    # Validate config
    #validator = ConfigValidator(config=config, dry_run=dry_run)

    # Validate against JSON schema before running custom checks
    try:
        #validate_against_schema(config, "config_schema.json", logger)
        validate_against_schema(config, "s3a://cdm-lake/config-json/config_schema.json", logger)
    except ValidationError as e:
        logger.error(f"JSON schema validation failed: {e}")
        return False

    # Validate LinkML schema slots
    schema_result = validate_schema_against_file(loader, logger)
    schema_columns = schema_result["schema_columns"]
    invalid_columns = validate_config_columns_against_schema(config, schema_columns, logger)

    if any(invalid_columns.values()):
        logger.error(f"Config contains columns not defined in schema: {invalid_columns}")
        if dry_run:
            print("❌ LinkML schema validation failed:")
            for section, cols in invalid_columns.items():
                if cols:
                    print(f" - {section}: {cols}")
        # return False


    print("✅ About to run ConfigValidator...")

    # Proceed with custom validation
    validator = ConfigValidator(config=config, dry_run=dry_run, logger=logger)

    return validator.validate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--target-table", required=True, help="Name of the target table (used to locate JSON)")
    parser.add_argument("--dry-run", action="store_true", help="Enable dry-run mode")

    args = parser.parse_args()
    result = run_config_validation(args.target_table, dry_run=args.dry_run)
    exit(0 if result else 1)

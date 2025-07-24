"""
Run Great Expectations validations dynamically using Spark DataFrames.

This module supports:
- Loading a Delta table into a DataFrame
- Registering a Spark runtime data source with GE
- Dynamically applying expectations from a config
- Saving the expectation suite
- Running a checkpoint and generating Data Docs
"""

import json
import logging
from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint


def run_great_expectations_validation(spark, loader, logger: logging.Logger, suite_name: str = "default_suite") -> None:
    """
    Run Great Expectations validations on a Spark Delta table using rules from config.

    Args:
        spark (SparkSession): Active Spark session to read Delta tables.
        loader: An instance of ConfigLoader with loaded config.
        logger (logging.Logger): Logger instance for structured logging.
        suite_name (str): Name of the expectation suite (default = 'default_suite').

    Behavior:
        - Loads the Spark table defined in the config.
        - Initializes GE context and registers Spark runtime datasource.
        - Loads dynamic expectations from config and applies them to the data.
        - Saves the expectation suite and validates data.
        - Builds Data Docs and executes a checkpoint.
    """
    target_table = loader.get_target_table()
    table_name = target_table

    logger.info(f"Starting Great Expectations validation for table: {table_name} with suite: {suite_name}")

    # Load table into DataFrame
    try:
        df = spark.table(target_table)
        logger.info(f"Loaded Spark table: {target_table}")
    except Exception as e:
        logger.error(f"Failed to load table {target_table}: {e}", exc_info=True)
        return

    # Initialize GE context and register Spark datasource
    try:
        context = get_context()
        logger.info("Great Expectations context initialized.")

        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        logger.info(f"Expectation suite '{suite_name}' added or updated.")

        context.add_datasource(
            name="my_spark_datasource",
            class_name="Datasource",
            execution_engine={"class_name": "SparkDFExecutionEngine"},
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_id"]
                }
            }
        )
        logger.info("Spark datasource registered in GE context.")
    except Exception as e:
        logger.error("Failed during GE context or datasource setup.", exc_info=True)
        return

    # Define a runtime batch request to link DataFrame with GE
    try:
        batch_request = RuntimeBatchRequest(
            datasource_name="my_spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=table_name.replace(".", "_"),
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_id": "batch_001"},
        )
        logger.debug(f"Batch request created for data asset: {table_name.replace('.', '_')}")
    except Exception as e:
        logger.error("Failed to create RuntimeBatchRequest.", exc_info=True)
        return

    # Apply expectations from config
    try:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        logger.info("Validator initialized for batch request.")

        #validator.expect_column_values_to_not_be_null("genome_id")
        #validator.expect_column_values_to_match_regex("genome_id", "^[A-Za-z0-9_.-]+$")
        #validator.expect_column_values_to_not_be_null("gtdb_species_clade_id")
        #validator.expect_column_values_to_not_be_null("gtdb_taxonomy_id")
        
        # Get GE expectations from config
        expectations = loader.config.get("great_expectations_validations", [])
        logger.debug(f"Found {len(expectations)} expectations to apply.")

        logger.debug(f"Loaded GE validations config: {expectations}")


        # Apply dynamic expectations
        for exp in expectations:
            try:
                expectation_type = exp["expectation_type"]
                kwargs = exp["params"]

                # Check if the validator has this expectation method
                if not hasattr(validator, expectation_type):
                    logger.error(f"Unknown expectation type: {expectation_type}")
                    continue

                expectation_func = getattr(validator, expectation_type)
                result = expectation_func(**kwargs)
                logger.info(f"Applied GE expectation: {expectation_type} with args {kwargs} → result: {result.success}")

            except Exception as e:
                logger.error(f"Failed to apply expectation {exp}: {e}", exc_info=True)


        validator.save_expectation_suite(discard_failed_expectations=False)
        logger.info("Expectations saved to suite.")
    except Exception as e:
        logger.error("Error defining expectations or saving suite.", exc_info=True)
        return

    # Validate the batch and log results
    try:
        result = validator.validate()
        logger.info("Validation run completed.")
        #logger.debug("Validation result:\n" + json.dumps(result.to_json_dict(), indent=2))
        result_dict = result.to_json_dict()
        summary = {
            "success": result_dict["success"],
            "successful_expectations": result_dict["statistics"]["successful_expectations"],
            "unsuccessful_expectations": result_dict["statistics"]["unsuccessful_expectations"],
            "success_percent": result_dict["statistics"]["success_percent"],
        }
        logger.debug(f"Validation summary: {summary}")

    except Exception as e:
        logger.error("Validation failed.", exc_info=True)
        return

    # Build Data Docs and execute checkpoint
    try:
        context.build_data_docs()
        logger.info("Data Docs built after validation.")

        checkpoint = SimpleCheckpoint(name=f"{suite_name}_checkpoint", data_context=context)
        checkpoint_result = checkpoint.run(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )

        logger.info("Checkpoint executed.")
        context.build_data_docs()
        logger.info("Data Docs rebuilt with checkpoint results.")

        print("✅ GE validation and checkpoint complete. Data Docs generated.")
    except Exception as e:
        logger.error("Checkpoint or Data Docs generation failed.", exc_info=True)

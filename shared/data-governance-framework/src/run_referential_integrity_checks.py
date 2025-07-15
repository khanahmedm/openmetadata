from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from functools import reduce
import logging

def run_referential_integrity_checks(spark: SparkSession, config_loader, logger: logging.Logger) -> bool:
    """
    Perform referential integrity checks based on a config JSON file.

    Parameters:
        spark (SparkSession): Active Spark session.
        config_loader: Instance of ConfigLoader.
        logger: Logger instance with pipeline context.

    Returns:
        bool: True if all checks pass, False if any violation is found.
    """
    loader = config_loader
    loader.load_and_validate()

    ri_rules = loader.get_referential_integrity_rules()
    target_table = loader.get_target_table()
    target_table_short = target_table.split(".")[-1]

    if not ri_rules:
        logger.warning(f"No referential integrity rules defined for table: {target_table}")
        print(f"\nℹ️ No referential integrity rules defined for table: {target_table}")
        return False

    target_df = spark.table(target_table)

    logger.info(f"Starting referential integrity checks for table: {target_table}")
    all_passed = True
    violation_dfs = []

    for rule in ri_rules:
        fk_col = rule["foreign_key"]
        ref_table = f"{rule['database']}.{rule['reference_table']}"
        ref_col = rule["reference_column"]
        action = rule.get("action", "log")

        logger.info(f"Checking foreign key: {fk_col} in {target_table} → {ref_col} in {ref_table}")
        print(f"\n🔍 Checking FK `{fk_col}` in `{target_table}` → `{ref_col}` in `{ref_table}`")

        # Check if columns exist
        if fk_col not in target_df.columns:
            logger.error(f"Column '{fk_col}' not found in target table '{target_table}'")
            print(f"❌ Column '{fk_col}' not found in target table '{target_table}'")
            all_passed = False
            continue

        try:
            ref_df = spark.table(ref_table)
        except Exception as e:
            logger.error(f"Reference table '{ref_table}' not found or unreadable: {str(e)}")
            print(f"❌ Reference table '{ref_table}' not found or unreadable")
            all_passed = False
            continue

        if ref_col not in ref_df.columns:
            logger.error(f"Column '{ref_col}' not found in reference table '{ref_table}'")
            print(f"❌ Column '{ref_col}' not found in reference table '{ref_table}'")
            all_passed = False
            continue

        ref_df = ref_df.select(ref_col).distinct()
        violations_df = target_df.join(ref_df, target_df[fk_col] == ref_df[ref_col], how="left_anti")

        count = violations_df.count()
        logger.info(f"Violations found: {count}")
        print(f" → Violations found: {count}")

        if count > 0:
            all_passed = False
            print(" → Sample violations:")
            logger.warning(f"{count} violations found in referential integrity check for FK: {fk_col} → {ref_col} in {ref_table}")

            try:
                sample_violations = violations_df.select(fk_col).limit(5).toPandas().to_dict(orient="records")
                logger.error(f"Sample violations for FK '{fk_col}': {sample_violations}")
            except Exception as e:
                logger.warning(f"Unable to convert sample violations for FK '{fk_col}' to JSON: {str(e)}")

            if action == "log":
                violations_df = violations_df.withColumn("failed_fk", lit(fk_col))
                violation_dfs.append(violations_df)


    # If there are any violations collected, write them once
    if violation_dfs:
        all_violations_df = reduce(DataFrame.unionByName, violation_dfs)

        log_config = loader.get_logging_config()
        log_table = log_config.get("error_table", f"{target_table_short}_errors")
        log_path = log_config.get("output_delta_path", f"s3a://cdm-lake/logs/errors/{target_table_short}")

        all_violations_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(log_path)

        spark.sql(f"CREATE TABLE IF NOT EXISTS pangenome.{log_table} USING DELTA LOCATION '{log_path}'")

        logger.error(f"Referential integrity violations written to Delta table: pangenome.{log_table} at {log_path}")
        print(f"\n🚨 All violations logged to: pangenome.{log_table}")

    if all_passed:
        logger.info("All referential integrity checks passed.")
        print("\n✅ All referential integrity checks passed.")
    else:
        logger.warning("Referential integrity check(s) failed.")
        print("\n❌ Some referential integrity checks failed.")

    return all_passed

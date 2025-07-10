from src.config_loader import ConfigLoader
from src.spark_session import start_spark_session
from src.validate_header_schema import validate_schema_columns
from src.validate_file_format import validate_tsv_format
from src.validate_content_rules import validate_content
from src.validate_referential_integrity import check_referential_integrity
from src.validate_with_ge import run_great_expectations_validation

config_path = "s3a://cdm-lake/config-json/genome.json"
loader = ConfigLoader(config_path)
loader.load_and_validate()

spark = start_spark_session()

# Run validations
validate_schema_columns(loader, spark)
validate_tsv_format(loader)
validate_content(loader)
check_referential_integrity(loader, spark)
run_great_expectations_validation(spark, table_name="pangenome.genome", suite_name="genome_suite")

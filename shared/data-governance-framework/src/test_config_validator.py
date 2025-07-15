import pytest
from config_validator import ConfigValidator, ConfigValidationError


@pytest.fixture
def valid_config():
    return {
        "target_table": "pangenome.genome",
        "schema_file_path": "s3a://schemas/schema.yaml",
        "input_files": [
            {
                "source": "GTDB",
                "file_path": "s3a://bronze/file.tsv",
                "file_type": "tsv",
                "delimiter": "\t",
                "ignore_first_line": "no"
            }
        ],
        "output_file": {
            "output_file_path": "s3a://silver/output.tsv",
            "file_type": "tsv",
            "delimiter": "\t",
            "ignore_first_line": "no"
        },
        "transformations": [
            {"column_name": "genome_id", "operation": "trim_whitespace"}
        ],
        "validations": [
            {"column": "genome_id", "validation_type": "not_null", "error_message": "Missing genome_id"}
        ],
        "referential_integrity": [
            {
                "foreign_key": "genome_id",
                "reference_table": "gtdb_metadata",
                "reference_column": "accession",
                "database": "pangenome",
                "action": "log"
            }
        ],
        "great_expectations_validations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "params": {"column": "genome_id"}
            }
        ],
        "schema_drift_handling": {
            "action": "log",
            "log_table": "drift_log",
            "log_path": "s3a://logs/drift"
        },
        "logging": {
            "error_table": "error_table",
            "output_delta_path": "s3a://logs/errors"
        },
        "versioning": {
            "version_tag": "v1",
            "created_by": "tester",
            "created_at": "2025-07-15T12:00:00"
        }
    }


def test_valid_config_passes(valid_config):
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is True


def test_missing_required_field_raises_error(valid_config):
    del valid_config["target_table"]
    validator = ConfigValidator(config=valid_config, dry_run=False)
    with pytest.raises(ConfigValidationError) as exc_info:
        validator.validate()
    assert "target_table" in str(exc_info.value)


def test_invalid_input_files_type(valid_config):
    valid_config["input_files"] = "should_be_list"
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("input_files must be a list" in e["message"] for e in validator.errors)


def test_missing_transformation_fields(valid_config):
    valid_config["transformations"] = [{"operation": "trim_whitespace"}]
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("Each transformation must include" in e["message"] for e in validator.errors)


def test_invalid_created_at_format(valid_config):
    valid_config["versioning"]["created_at"] = "not-a-date"
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("not a valid ISO timestamp" in e["message"] for e in validator.errors)


def test_missing_referential_integrity_key(valid_config):
    del valid_config["referential_integrity"][0]["database"]
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("Missing field: database" in e["message"] for e in validator.errors)


def test_missing_output_file_field(valid_config):
    del valid_config["output_file"]["file_type"]
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("Missing field: file_type" in e["message"] for e in validator.errors)


def test_missing_ge_expectation_fields(valid_config):
    valid_config["great_expectations_validations"] = [{"params": {"column": "genome_id"}}]
    validator = ConfigValidator(config=valid_config, dry_run=True)
    assert validator.validate() is False
    assert any("Each entry must have 'expectation_type' and 'params'" in e["message"]
               for e in validator.errors)

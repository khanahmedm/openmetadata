import json
import boto3
from typing import Any, Dict, List, Optional

#from src.logger import setup_logger
import logging

#logger = setup_logger()
#logger.info("Logger initialized for config_loader")

class ConfigLoader:
    def __init__(self, target_table_name: str, logger: logging.Logger):
        self.logger = logger
        self.target_table_name = target_table_name
        self.config_path = f"s3a://cdm-lake/config-json/{target_table_name}.json"
        self.config: Optional[Dict[str, Any]] = None
        self.logger.info(f"ConfigLoader initialized for target table: {self.target_table_name}")
        self.logger.info(f"Resolved config path: {self.config_path}")

    def load_config(self) -> None:
        try:
            # Parse bucket and key
            path = self.config_path.replace("s3a://", "")
            bucket = path.split("/")[0]
            key = "/".join(path.split("/")[1:])
            self.logger.info(f"Loading config from MinIO: bucket={bucket}, key={key}")

            s3 = boto3.client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            response = s3.get_object(Bucket=bucket, Key=key)
            self.config = json.loads(response["Body"].read())
            self.logger.info("Config loaded successfully from MinIO")
        except Exception as e:
            self.logger.error(f"Failed to load config from MinIO: {e}", exc_info=True)
            raise

    def validate_required_fields(self) -> None:
        required_fields = [
            "target_table", "schema_file_path", "input_files", "output_file",
            "bronze", "silver", "transformations",
            "validations", "referential_integrity",
            "schema_drift_handling", "great_expectations_validations", "logging", "versioning"
        ]
        missing = [field for field in required_fields if field not in self.config]
        if missing:
            self.logger.error(f"Missing required fields in config: {missing}")
            raise ValueError(f"Missing required fields in config: {missing}")
        self.logger.info("All required fields are present in the config")

    def load_and_validate(self) -> None:
        self.load_config()
        self.validate_required_fields()

    def get_target_table(self) -> str:
        value = self.config.get("target_table", "")
        self.logger.debug(f"Target table: {value}")
        return value

    def get_schema_path(self) -> str:
        value = self.config.get("schema_file_path", "")
        self.logger.debug(f"Schema file path: {value}")
        return value

    def get_input_files(self) -> List[Dict[str, Any]]:
        value = self.config.get("input_files", [])
        self.logger.debug(f"Input files: {value}")
        return value

    def get_output_file(self) -> Dict[str, Any]:
        value = self.config.get("output_file", {})
        self.logger.debug(f"Output file: {value}")
        return value

    def get_bronze_config(self) -> Dict[str, str]:
        value = self.config.get("bronze", {})
        self.logger.debug(f"Bronze config: {value}")
        return value

    def get_silver_config(self) -> Dict[str, str]:
        value = self.config.get("silver", {})
        self.logger.debug(f"Silver config: {value}")
        return value

    def get_transformations(self) -> List[Dict[str, Any]]:
        value = self.config.get("transformations", [])
        self.logger.debug(f"Transformations: {value}")
        return value

    def get_validations(self) -> List[Dict[str, Any]]:
        value = self.config.get("validations", [])
        self.logger.debug(f"Validations: {value}")
        return value

    def get_referential_integrity_rules(self) -> List[Dict[str, Any]]:
        value = self.config.get("referential_integrity", [])
        self.logger.debug(f"Referential integrity rules: {value}")
        return value

    def get_schema_drift_config(self) -> Dict[str, Any]:
        value = self.config.get("schema_drift_handling", {})
        self.logger.debug(f"Schema drift config: {value}")
        return value

    def get_logging_config(self) -> Dict[str, Any]:
        value = self.config.get("logging", {})
        self.logger.debug(f"Logging config: {value}")
        return value

    def get_versioning_info(self) -> Dict[str, Any]:
        value = self.config.get("versioning", {})
        self.logger.debug(f"Versioning info: {value}")
        return value

    def get_full_config(self) -> Dict[str, Any]:
        self.logger.debug("Full config returned")
        return self.config

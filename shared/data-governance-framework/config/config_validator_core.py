import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
import logging


class ConfigValidationError(Exception):
    """Raised when config validation fails."""


class ConfigValidator:
    def __init__(self, config: Dict[str, Any], dry_run: bool = False, logger: Optional[logging.Logger] = None):
        self.config = config
        self.dry_run = dry_run
        self.logger = logger or logging.getLogger("pipeline_logger")
        self.errors: List[Dict[str, str]] = []

        print("✅ ConfigValidator logger:", self.logger, flush=True)
        print("✅ Logger name:", self.logger.name, flush=True)
        print("✅ Logger handlers:", self.logger.handlers, flush=True)


    def validate(self) -> bool:
        if self.logger:
            self.logger.info("🔍 Starting config validation...")

        self.errors.clear()

        print("✅ Inside ConfigValidator.validate")

        self._run_section_validation("required_fields", self._validate_required_fields)
        self._run_section_validation("input_files", self._validate_input_files)
        self._run_section_validation("output_file", self._validate_output_file)
        self._run_section_validation("transformations", self._validate_transformations)
        self._run_section_validation("validations", self._validate_validations)
        self._run_section_validation("referential_integrity", self._validate_referential_integrity)
        self._run_section_validation("great_expectations_validations", self._validate_great_expectations)
        self._run_section_validation("schema_drift_handling", self._validate_schema_drift_handling)
        self._run_section_validation("logging", self._validate_logging)
        self._run_section_validation("versioning", self._validate_versioning)

        if self.errors:
            if self.dry_run:
                print("⚠️ Config validation failed with errors:")
                for err in self.errors:
                    print(f"- [{err['section']}] {err['message']}")
                    if self.logger:
                        self.logger.warning(f"[{err['section']}] {err['message']}")
                return False

            if self.logger:
                for err in self.errors:
                    self.logger.error(f"[{err['section']}] {err['message']}")
                self.logger.error("❌ Config validation failed.")
            raise ConfigValidationError(json.dumps(self.errors, indent=2))

        if self.dry_run:
            print("✅ Config validation passed.")
            if self.logger:
                self.logger.info("✅ Config validation passed.")
        else:
            if self.logger:
                self.logger.info("✅ Config validation completed with no errors.")

        return True

    def _run_section_validation(self, section: str, func: Callable[[], None]):
        self._log_section_start(section)
        func()
        self._log_success_if_no_errors(section)

    def _log_section_start(self, section: str):
        if self.logger:
            self.logger.info(f"Validating section: {section}")

    def _log_success_if_no_errors(self, section: str):
        if self.logger:
            if not any(err["section"].startswith(section) for err in self.errors):
                self.logger.info(f"✅ Section '{section}' validated successfully.")

    def _validate_required_fields(self):
        required_fields = ["target_table", "schema_file_path", "input_files", "output_file"]
        for field in required_fields:
            if field not in self.config:
                self.errors.append({"section": "required_fields", "message": f"Missing required field: '{field}'"})

    def _validate_input_files(self):
        input_files = self.config.get("input_files", [])
        if not isinstance(input_files, list):
            self.errors.append({"section": "input_files", "message": "input_files must be a list"})
            return
        for idx, item in enumerate(input_files):
            for key in ["source", "file_path", "file_type", "delimiter", "ignore_first_line"]:
                if key not in item:
                    self.errors.append({
                        "section": f"input_files[{idx}]",
                        "message": f"Missing field: {key}"
                    })

    def _validate_output_file(self):
        output = self.config.get("output_file", {})
        for key in ["output_file_path", "file_type", "delimiter", "ignore_first_line"]:
            if key not in output:
                self.errors.append({"section": "output_file", "message": f"Missing field: {key}"})

    def _validate_transformations(self):
        for i, t in enumerate(self.config.get("transformations", [])):
            if "column_name" not in t or "operation" not in t:
                self.errors.append({
                    "section": f"transformations[{i}]",
                    "message": "Each transformation must include 'column_name' and 'operation'"
                })

    def _validate_validations(self):
        for i, v in enumerate(self.config.get("validations", [])):
            required = ["column", "validation_type", "error_message"]
            for key in required:
                if key not in v:
                    self.errors.append({
                        "section": f"validations[{i}]",
                        "message": f"Missing field: {key}"
                    })

    def _validate_referential_integrity(self):
        for i, r in enumerate(self.config.get("referential_integrity", [])):
            required = ["foreign_key", "reference_table", "reference_column", "database", "action"]
            for key in required:
                if key not in r:
                    self.errors.append({
                        "section": f"referential_integrity[{i}]",
                        "message": f"Missing field: {key}"
                    })

    def _validate_great_expectations(self):
        for i, g in enumerate(self.config.get("great_expectations_validations", [])):
            if "expectation_type" not in g or "params" not in g:
                self.errors.append({
                    "section": f"great_expectations_validations[{i}]",
                    "message": "Each entry must have 'expectation_type' and 'params'"
                })

    def _validate_schema_drift_handling(self):
        sdh = self.config.get("schema_drift_handling", {})
        if sdh:
            required = ["action", "log_table", "log_path"]
            for key in required:
                if key not in sdh:
                    self.errors.append({
                        "section": "schema_drift_handling",
                        "message": f"Missing field: {key}"
                    })

    def _validate_logging(self):
        log = self.config.get("logging", {})
        if log:
            for key in ["error_table", "output_delta_path"]:
                if key not in log:
                    self.errors.append({
                        "section": "logging",
                        "message": f"Missing field: {key}"
                    })

    def _validate_versioning(self):
        version = self.config.get("versioning", {})
        if version:
            for key in ["version_tag", "created_by", "created_at"]:
                if key not in version:
                    self.errors.append({
                        "section": "versioning",
                        "message": f"Missing field: {key}"
                    })
            else:
                try:
                    datetime.fromisoformat(version.get("created_at", ""))
                except ValueError:
                    self.errors.append({
                        "section": "versioning",
                        "message": "'created_at' is not a valid ISO timestamp"
                    })

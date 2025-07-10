# src/logger.py
import logging
import os
import sys
from datetime import datetime

_logger_instance = None

class PipelineContextFilter(logging.Filter):
    def __init__(self, pipeline_name: str, target_table: str, schema: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.target_table = target_table
        self.schema = schema

    def filter(self, record):
        record.pipeline_name = self.pipeline_name
        record.target_table = self.target_table
        record.schema = self.schema
        return True

def setup_logger(
    log_dir="local_logs",
    logger_name="pipeline_logger",
    pipeline_name="unknown_pipeline",
    target_table="unknown_table",
    schema="unknown_schema"
):
    global _logger_instance
    if _logger_instance:
        return _logger_instance

    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_run_{timestamp}.log")

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode="w")
        ch = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            '{"time": "%(asctime)s", "pipeline": "%(pipeline_name)s", "schema": "%(schema)s", "table": "%(target_table)s", "level": "%(levelname)s", "module": "%(module)s", "msg": "%(message)s"}'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        context_filter = PipelineContextFilter(pipeline_name, target_table, schema)
        logger.addFilter(context_filter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    logger.log_file_path = log_file
    _logger_instance = logger
    return logger


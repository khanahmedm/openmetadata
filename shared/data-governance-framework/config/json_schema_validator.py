import json
import logging
from jsonschema import validate, ValidationError, Draft7Validator
import boto3

def load_schema_from_minio(s3_uri: str) -> dict:
    path = s3_uri.replace("s3a://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # NOT 9001 (admin UI), use the S3 API port
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())

def validate_against_schema(config: dict, schema_path: str, logger: logging.Logger) -> None:
    """
    Validate config JSON against JSON Schema.

    Raises:
        ValidationError if config is invalid.
    """
    #with open(schema_path, "r") as f:
    #    schema = json.load(f)

    schema = load_schema_from_minio(schema_path)

    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(config), key=lambda e: e.path)

    if errors:
        for error in errors:
            logger.error(f"Schema validation error: {error.message} at path: {list(error.path)}")
        raise ValidationError("Config failed JSON schema validation.")
    else:
        logger.info("Config passed JSON schema validation.")

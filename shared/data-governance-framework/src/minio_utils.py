import boto3
from botocore.exceptions import BotoCoreError, ClientError
from typing import List, Optional

def get_s3_client():
    """
    Initializes and returns a Boto3 S3 client configured for MinIO.

    Returns:
        boto3.client: Configured S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # update if needed
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

def list_minio_files(bucket: str, prefix: str) -> List[str]:
    """
    List .json files in a given MinIO bucket and prefix.

    Args:
        bucket (str): Name of the MinIO bucket.
        prefix (str): Folder path prefix inside the bucket.

    Returns:
        List[str]: List of file names (not full keys) ending in '.json'.

    Raises:
        RuntimeError: If listing files fails due to S3/MinIO errors.
    """
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        return [obj["Key"].split("/")[-1] for obj in contents if obj["Key"].endswith(".json")]
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to list files in MinIO bucket '{bucket}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error listing files: {e}") from e

def get_file_from_minio(bucket: str, key: str) -> str:
    """
    Fetch a file from MinIO and return its content as a string.

    Args:
        bucket (str): Name of the MinIO bucket.
        key (str): Full object key (path) inside the bucket.

    Returns:
        str: File content decoded as UTF-8 text.

    Raises:
        RuntimeError: If file fetch or decoding fails.
    """
    s3 = get_s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to fetch file from MinIO (s3://{bucket}/{key}): {e}") from e
    except UnicodeDecodeError as e:
        raise RuntimeError(f"Failed to decode file content from MinIO (s3://{bucket}/{key}): {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error retrieving file from MinIO: {e}") from e

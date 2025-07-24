# src/minio_uploader.py
import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
import os

def upload_to_minio(local_path, bucket="cdm-lake", object_key=None):
    """
    Uploads a local file to a MinIO (S3-compatible) bucket.

    Args:
        local_path (str): Full path to the local file to upload.
        bucket (str): Target bucket name in MinIO.
        object_key (str, optional): Object key (path inside the bucket). If None, uses 'logs/pangenome/{filename}'.

    Raises:
        RuntimeError: If upload fails due to connection or file access issues.
    """
    try:
        # Initialize S3 client
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version='s3v4'),
            region_name="us-east-1",
        )
    except BotoCoreError as e:
        raise RuntimeError(f"❌ Failed to connect to MinIO: {e}") from e

    # Derive object key if not provided
    if object_key is None:
        filename = os.path.basename(local_path)
        object_key = f"logs/pangenome/{filename}"

    # Upload file
    try:
        s3.upload_file(local_path, bucket, object_key)
        print(f"✅ Uploaded log to MinIO at: s3://{bucket}/{object_key}")
    except FileNotFoundError:
        raise RuntimeError(f"❌ File not found: {local_path}")
    except ClientError as e:
        raise RuntimeError(f"❌ Upload failed: {e}") from e
    except Exception as e:
        raise RuntimeError(f"❌ Unexpected error during upload: {e}") from e

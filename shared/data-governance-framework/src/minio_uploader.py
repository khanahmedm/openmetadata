# src/minio_uploader.py
import boto3
from botocore.client import Config
import os

def upload_to_minio(local_path, bucket="cdm-lake", object_key=None):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # MinIO endpoint
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version='s3v4'),
        region_name="us-east-1",
    )

    if object_key is None:
        filename = os.path.basename(local_path)
        object_key = f"logs/pangenome/{filename}"

    s3.upload_file(local_path, bucket, object_key)
    print(f"✅ Uploaded log to MinIO at: s3://{bucket}/{object_key}")

import boto3

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # update if needed
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

def list_minio_files(bucket: str, prefix: str) -> list:
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"].split("/")[-1] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]
    except Exception as e:
        return [f"Error: {str(e)}"]

def get_file_from_minio(bucket: str, key: str) -> str:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

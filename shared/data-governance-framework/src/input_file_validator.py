import boto3
import logging

def validate_input_files(config_loader, logger: logging.Logger):
    """
    Validates that all input files specified in the config exist in MinIO and, if they are TSV,
    contain the expected delimiter in their first few lines.

    Args:
        config_loader: Instance of ConfigLoader, used to fetch input file metadata.
        logger (logging.Logger): Logger for capturing validation results.

    Behavior:
        - Logs and prints whether each input file exists in MinIO.
        - For TSV files, reads the first few lines and checks for the presence of the delimiter.
    """
    input_files = config_loader.get_input_files()

    logger.info("Starting input file validation...")
    print("Validating input files...\n")

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
    except BotoCoreError as e:
        logger.error(f"❌ Failed to connect to MinIO: {e}", exc_info=True)
        print(f"❌ Failed to connect to MinIO: {e}")
        return

    for file_info in input_files:
        s3_path = file_info["file_path"]
        file_type = file_info.get("file_type", "").lower()
        delimiter = file_info.get("delimiter", "\t")
        ignore_first_line = file_info.get("ignore_first_line", "no").lower() == "yes"

        path_clean = s3_path.replace("s3a://", "")
        bucket = path_clean.split("/")[0]
        key = "/".join(path_clean.split("/")[1:])

        logger.info(f"Checking file: {s3_path}")
        print(f"🔍 Checking: {s3_path}")

        
        try:
            # Check file existence
            s3.head_object(Bucket=bucket, Key=key)
            logger.info(f"File exists: s3://{bucket}/{key}")
            print("✅ File exists")

            # Content validation (only for TSV)
            if file_type == "tsv":
                try:
                    response = s3.get_object(Bucket=bucket, Key=key)
                    content = response["Body"].read().decode("utf-8")
                    lines = content.strip().split("\n")

                    if ignore_first_line:
                        lines_to_check = lines[1:6]
                        line_offset = 2
                    else:
                        lines_to_check = lines[:5]
                        line_offset = 1

                    for idx, line in enumerate(lines_to_check):
                        line_number = idx + line_offset
                        if delimiter in line:
                            logger.debug(f"   ✅ Line {line_number}: Delimiter '{delimiter}' found")
                            print(f"   ✅ Line {line_number}: Delimiter '{delimiter}' found")
                        else:
                            logger.warning(f"   ❌ Line {line_number}: Missing delimiter → {line}")
                            print(f"   ❌ Line {line_number}: Missing delimiter → {line}")

                except Exception as e:
                    logger.error(f"⚠️ Failed to read/parse TSV file: {e}", exc_info=True)
                    print(f"⚠️ Failed to read/parse TSV file: {e}")
            else:
                logger.warning(f"⚠️ Skipped format check (file_type is '{file_type}')")
                print(f"⚠️ Skipped format check (file_type is '{file_type}')")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.error(f"❌ File not found: s3://{bucket}/{key}")
                print("❌ File not found")
            else:
                logger.error(f"⚠️ Error checking file: {e}", exc_info=True)
                print(f"⚠️ Error checking file: {e}")

        except Exception as e:
            logger.error(f"⚠️ Unexpected error while validating input file: {e}", exc_info=True)
            print(f"⚠️ Unexpected error: {e}")

        print()

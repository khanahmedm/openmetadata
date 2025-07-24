import boto3
from typing import Dict, Any


def validate_tsv_format(config_loader) -> Dict[str, Any]:
    """
    Validate whether the output file is properly tab-delimited by inspecting a few lines.

    Args:
        config_loader: Instance of ConfigLoader to retrieve output file path and delimiter info.

    Returns:
        Dict[str, Any]: A dictionary containing:
            - 'file_path': the path to the file validated,
            - 'checked_lines': number of lines checked,
            - 'invalid_lines': list of problematic lines with line number and content,
            - 'is_valid': overall success flag (True if all checked lines are valid).
    """
    try:
        # Load output config
        output_file_config = config_loader.get_output_file()
        output_file_path = output_file_config["output_file_path"]
        delimiter = output_file_config.get("delimiter", "\t")
        ignore_first_line = output_file_config.get("ignore_first_line", "no").strip().lower() == "yes"
    except Exception as e:
        print(f"❌ Failed to load output config: {e}")
        return {
            "file_path": None,
            "checked_lines": 0,
            "invalid_lines": [],
            "is_valid": False,
            "error": str(e)
        }

    try:
        # Extract bucket and key
        path_clean = output_file_path.replace("s3a://", "")
        bucket = path_clean.split("/")[0]
        key = "/".join(path_clean.split("/")[1:])

        # MinIO client setup
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin"
        )

        # Download file content
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response["Body"].read().decode("utf-8").splitlines()
    except Exception as e:
        print(f"❌ Failed to read file from MinIO: {e}")
        return {
            "file_path": output_file_path,
            "checked_lines": 0,
            "invalid_lines": [],
            "is_valid": False,
            "error": str(e)
        }

    try:
        # Select lines to validate
        lines_to_check = lines[1:6] if ignore_first_line else lines[:5]
        offset = 2 if ignore_first_line else 1

        print(f"\n🔍 Verifying tab-delimited format in file: {output_file_path}")
        invalid_lines = []

        for idx, line in enumerate(lines_to_check):
            line_number = idx + offset
            if delimiter in line:
                print(f"✅ Line {line_number} is tab-delimited.")
            else:
                print(f"❌ Line {line_number} is NOT tab-delimited: {line}")
                invalid_lines.append({
                    "line_number": line_number,
                    "content": line
                })

        return {
            "file_path": output_file_path,
            "checked_lines": len(lines_to_check),
            "invalid_lines": invalid_lines,
            "is_valid": len(invalid_lines) == 0
        }

    except Exception as e:
        print(f"❌ Error while validating lines: {e}")
        return {
            "file_path": output_file_path,
            "checked_lines": 0,
            "invalid_lines": [],
            "is_valid": False,
            "error": str(e)
        }

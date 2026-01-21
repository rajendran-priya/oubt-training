import boto3

s3 = boto3.client("s3")

# ---- CONFIG (YOUR RAW FILE) ----
EXPECTED_BUCKET = "day20-demo-oubt"
EXPECTED_KEY = "raw/yellow_tripdata_2025-08.parquet"

MIN_SIZE_BYTES = 1024 * 10  # 10 KB minimum (safe for parquet)

def lambda_handler(event, context):
    """
    This Lambda validates a single raw Parquet file before Glue runs.
    Step Functions can call this with an empty payload: {}
    """

    bucket = EXPECTED_BUCKET
    key = EXPECTED_KEY

    # 1) Extension check
    if not key.lower().endswith(".parquet"):
        raise ValueError(f"Invalid file extension. Expected .parquet, got {key}")

    # 2) Check file exists + size
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        raise ValueError(f"Raw file not found: s3://{bucket}/{key}. Error: {str(e)}")

    size = int(head.get("ContentLength", 0))
    if size < MIN_SIZE_BYTES:
        raise ValueError(f"File too small ({size} bytes). Minimum required: {MIN_SIZE_BYTES}")

    # 3) Parquet signature check (PAR1)
    # Parquet files must end with magic bytes PAR1
    try:
        resp = s3.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes={size-4}-{size-1}"
        )
        tail = resp["Body"].read()
        if tail != b"PAR1":
            raise ValueError(f"Invalid Parquet file. Expected PAR1 footer, got {tail}")
    except Exception as e:
        raise ValueError(f"Parquet validation failed for s3://{bucket}/{key}. Error: {str(e)}")

    # ✅ Passed all checks
    return {
        "is_valid": True,
        "bucket": bucket,
        "key": key,
        "file_size_bytes": size,
        "message": "Raw Parquet validation passed"
    }

import boto3
import time
import datetime
from loguru import logger

s3 = boto3.client("s3")

def delete_daily_tmp_directories(bucket_name, start_pattern = "tmp_k"):
    
    logger.info(f"Running cleanup job...")
    
    paginator = s3.get_paginator("list_objects_v2")

    # list only top-level keys
    for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
        if "CommonPrefixes" not in page:
            continue

        for prefix_info in page["CommonPrefixes"]:
            folder_name = prefix_info["Prefix"]  # e.g. "tmp_ksqj601_zslmvl/"

            # Check top-level directory name
            if folder_name.startswith(start_pattern):
                logger.info(f"Deleting directory: {folder_name}")

                # Delete every object inside this "directory"
                delete_all_under_prefix(bucket_name, folder_name)

    logger.info(f"Cleanup finished.")



def delete_weekend_tmp_directories(bucket_name, start_pattern = "tmp_k"):
    today = datetime.datetime.today().weekday()  # Monday=0 ... Sunday=6

    # Only delete on Saturday (5) or Sunday (6)
    if today not in (5, 6):
    # if today not in (0, 1):
        logger.info(f"Not weekend — skipping cleanup.")
        
        return
    
    logger.info("Weekend detected — starting cleanup...")

    paginator = s3.get_paginator("list_objects_v2")

    # list only top-level keys
    for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
        if "CommonPrefixes" not in page:
            continue

        for prefix_info in page["CommonPrefixes"]:
            folder_name = prefix_info["Prefix"]  # e.g. "tmp_ksqj601_zslmvl/"

            # Check top-level directory name
            if folder_name.startswith(start_pattern):
                logger.info(f"Deleting directory: {folder_name}")

                # Delete every object inside this "directory"
                delete_all_under_prefix(bucket_name, folder_name)

    print("Cleanup finished.")

def delete_all_under_prefix(bucket_name, prefix):
    """Deletes all objects under a given S3 prefix (directory)."""
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            logger.info(f"  - deleting {key}")
            s3.delete_object(Bucket=bucket_name, Key=key)

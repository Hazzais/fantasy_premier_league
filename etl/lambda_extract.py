import os
import json
import logging

import extract

s3_bucket = os.environ.get('AWS_S3_BUCKET')
s3_key_root = os.environ.get('AWS_S3_BUCKET_KEY_ROOT')
# e.g. etl_staging/raw/season-202021/

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        extract_details = extract.extract(s3_bucket, s3_key_root)
        return {
            'statusCode': 200,
            'body': json.dumps(extract_details)
        }
    except Exception:
        raise

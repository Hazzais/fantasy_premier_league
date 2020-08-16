import io
import json
from datetime import datetime

import boto3


class Datetime:

    def __init__(self):
        self._now = datetime.now()
        self.timestamp = self._now.timestamp()

    def get_datetime(self):
        return round(self._now)

    def get_datetime_string(self):
        return str(self.get_datetime())

    def get_datetime_string_f(self, ftime="%Y%m%d-%H%M%S"):
        return self._now.strftime(ftime)


def write_s3_json(json_obj, bucket, key):
    s3 = boto3.client('s3')
    return s3.put_object(Body=str(json.dumps(json_obj)),
                         Bucket=bucket,
                         Key=key)

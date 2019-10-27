import os
import logging

import boto3
from botocore.exceptions import ClientError

from fpltools.utils import get_datetime_string_f

logging.basicConfig(level=logging.INFO,
                    filename=f'logs/s3_upload_{get_datetime_string_f()}.log',
                    filemode='w',
                    format='%(levelname)s - %(asctime)s - %(message)s')


class AwsS3:
    """Upload one or more files to a given S3 bucket"""

    def __init__(self, bucket: str, bucket_folder):
        self.__s3_client = boto3.client('s3')
        self._bucket = bucket
        self._bucket_folder = bucket_folder

    def _upload_file(self, file, out_object):
        try:
            resp = self.__s3_client.upload_file(file, self._bucket, out_object)
        except FileNotFoundError as e1:
            logging.error(e1)
        except ClientError as e2:
            logging.error(e2)
        else:
            logging.info(f'Successfully uploaded {file} as {out_object}')
            return resp

    def _generate_out_name(self, filename):
        extension = filename.split('.')[-1]
        data_out_name = filename.replace(f".{extension}", f"_{get_datetime_string_f()}.{extension}")
        if self._bucket_folder is not None:
            data_out_object = f'{self._bucket_folder}/{data_out_name}'
        else:
            data_out_object = f'{data_out_name}'
        return data_out_object

    def upload(self, data_files):
        if isinstance(data_files, str):
            data_files = [data_files]
        elif isinstance(data_files, list):
            data_files = data_files
        else:
            logging.error("Incorrect set of data file(s) to be uploaded supplied")
            raise TypeError("data_files must be a str or a list")

        logging.info(f'Uploading {len(data_files)} files to {self._bucket}/{self._bucket_folder}')

        for fl in data_files:
            logging.info(f'Uploading {fl}')
            filepath, filename = os.path.split(fl)
            out_object = self._generate_out_name(filename)
            resp = self._upload_file(fl, out_object)

dfiles = ['data/fixtures.json', 'main.json']

ec2 = AwsS3('fpl-alldata', 'etl_staging')

ec2.upload(dfiles)

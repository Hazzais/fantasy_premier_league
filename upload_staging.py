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

    def __init__(self):
        self.__s3_client = boto3.client('s3')

    def _upload_file(self, file, bucket, out_object):
        """Call to s3 client which uploads the file, naming it as specified to a given bucket """
        try:
            resp = self.__s3_client.upload_file(file, bucket, out_object)
        except FileNotFoundError as e1:
            logging.error(e1)
        except ClientError as e2:
            logging.error(e2)
        else:
            logging.info(f'Successfully uploaded {file} as {out_object}')
            return resp

    @staticmethod
    def _generate_out_name(filename, bucket_folder):
        """Generate the output object name for a file. This is the original filename with any S3 folder specified as a
        prefix, appending the current datetime before the extension"""
        split_file = filename.split('.')

        # Currently require a file to have an extension - no problem extending this to all files but minor refactoring
        # would be needed
        if len(split_file) <= 1:
            raise RuntimeError(f"File to be uploaded must have an extension")
        extension = split_file[-1]

        data_out_name = filename.replace(f".{extension}", f"_{get_datetime_string_f()}.{extension}")
        if bucket_folder is not None:
            data_out_object = f'{bucket_folder}/{data_out_name}'
        else:
            data_out_object = f'{data_out_name}'
        return data_out_object

    def upload(self, data_files, bucket, bucket_folder):
        """Upload one or more files to S3

        datafiles: str or list
            One or more files (including pre-pending path and extension)
        bucket: str
            The S3 bucket to upload to
        bucket_folder: str or None
            If specified, the folder(s) within the S3 bucket to upload to
        """
        if isinstance(data_files, str):
            data_files = [data_files]
        elif isinstance(data_files, list):
            data_files = data_files
        else:
            logging.error("Incorrect set of data file(s) to be uploaded supplied")
            raise TypeError("data_files must be a str or a list")

        logging.info(f'Uploading {len(data_files)} files to S3 ({bucket})')

        for fl in data_files:
            logging.info(f'Uploading {fl}')
            filepath, filename = os.path.split(fl)
            out_object = self._generate_out_name(filename, bucket_folder)
            _ = self._upload_file(fl, bucket, out_object)


if __name__ == '__main__':
    dfiles = ['data/fixtures.json', 'data/main.json', 'data/players.json']
    ec2 = AwsS3()
    ec2.upload(dfiles, 'fpl-alldata', 'etl_staging')

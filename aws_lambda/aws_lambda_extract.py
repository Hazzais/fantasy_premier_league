import os
import json
import logging

from fpltools.extract import (retrieve_data, retrieve_player_details,
                              save_intermediate_data)
from fpltools.constants import API_URLS
from fpltools.utils import AwsS3

DATA_LOC = '/tmp/'
FILE_STRING_FIXTURES = 'fixtures'
FILE_STRING_PLAYERS = 'players'
FILE_STRING_MAIN = 'main'
# LOG_FILE = '/tmp/extract.log'

s3_bucket = os.environ.get('AWS_S3_BUCKET')
s3_folder = os.environ.get('AWS_S3_BUCKET_FOLDER')
s3_log_output = os.environ.get('AWS_S3_LOG_OUTPUT')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        # logging.basicConfig(level=logging.INFO,
        #                     filename=LOG_FILE,
        #                     filemode='w',
        #                     format='%(levelname)s - %(asctime)s - %(message)s')

        main_data = retrieve_data(API_URLS['static'])
        fixtures_data = retrieve_data(API_URLS['fixtures'])
        player_data = retrieve_player_details(API_URLS['player'],
                                              main_data['elements'],
                                              verbose=True)

        save_intermediate_data(main_data, FILE_STRING_MAIN, DATA_LOC)
        save_intermediate_data(fixtures_data, FILE_STRING_FIXTURES, DATA_LOC)
        save_intermediate_data(player_data, FILE_STRING_PLAYERS, DATA_LOC)

        dfiles = [
            f'{DATA_LOC}/{FILE_STRING_FIXTURES}.json',
            f'{DATA_LOC}/{FILE_STRING_MAIN}.json',
            f'{DATA_LOC}/{FILE_STRING_PLAYERS}.json'
        ]

        s3 = AwsS3()
        s3.upload(dfiles, s3_bucket, s3_folder)
        logging.info('================Extract complete================')
        # lfiles = [LOG_FILE]
        # logging.info(f'Uploading {LOG_FILE} to S3')
        # s3_l = AwsS3()
        # s3_l.upload(lfiles, s3_bucket, s3_log_output)
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    except:
        logging.info('================Extract Failed================')
        # lfiles = [LOG_FILE]
        # logging.info(f'Uploading {LOG_FILE} to S3')
        # s3_l = AwsS3()
        # s3_l.upload(lfiles, s3_bucket, s3_log_output)
        raise

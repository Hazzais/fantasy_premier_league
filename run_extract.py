import logging
import argparse

from fpltools.extract import (retrieve_data, retrieve_player_details,
                              save_intermediate_data)
from fpltools.constants import API_URLS
from fpltools.utils import get_datetime, AwsS3

FILE_STRING_FIXTURES = 'fixtures'
FILE_STRING_PLAYERS = 'players'
FILE_STRING_MAIN = 'main'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch download from FPL'
                                                 'website endpoints and save'
                                                 'as JSON')

    parser.add_argument('--data_location',
                        type=str,
                        default='data/',
                        help='path in which to store data')
    parser.add_argument('-s',
                        '--skip-s3-upload',
                        action='store_true',
                        help='Do not attempt any upload to AWS S3')
    parser.add_argument('-b',
                        '--s3_bucket',
                        type=str,
                        default='fpl-alldata',
                        help='S3 bucket to upload to')
    parser.add_argument('-f',
                        '--s3-folder',
                        type=str,
                        default='etl_staging',
                        help='Folder within the S3 bucket to upload to')
    args = parser.parse_args()

    DATA_LOC = args.data_location

    logging.basicConfig(level=logging.INFO,
                        filename=f'logs/extract_{get_datetime()}.log',
                        filemode='w',
                        format='%(levelname)s - %(asctime)s - %(message)s')

    main_data = retrieve_data(API_URLS['static'])
    fixtures_data = retrieve_data(API_URLS['fixtures'])
    player_data = retrieve_player_details(API_URLS['player'],
                                          main_data['elements'],
                                          verbose=True)

    save_intermediate_data(main_data, FILE_STRING_MAIN, DATA_LOC)
    save_intermediate_data(fixtures_data, FILE_STRING_FIXTURES, DATA_LOC)
    save_intermediate_data(player_data, FILE_STRING_PLAYERS, DATA_LOC)

    if not args.skip_s3_upload:
        dfiles = [f'{DATA_LOC}/{FILE_STRING_FIXTURES}.json',
                  f'{DATA_LOC}/{FILE_STRING_MAIN}.json',
                  f'{DATA_LOC}/{FILE_STRING_PLAYERS}.json']

        ec2 = AwsS3()
        ec2.upload(dfiles, args.s3_bucket, args.s3_bucket)

    logging.info('================Extract complete================')

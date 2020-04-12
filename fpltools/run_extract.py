import logging
import argparse

from fpltools.extract import (retrieve_data, retrieve_player_details,
                              save_intermediate_data)
from fpltools.constants import API_URLS
from fpltools.utils import AwsS3

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
                        '--s3-bucket',
                        type=str,
                        default='fpl-alldata',
                        help='S3 bucket to upload to')
    parser.add_argument('-f',
                        '--s3-folder',
                        type=str,
                        default='etl_staging/raw',
                        help='Folder within the S3 bucket to upload to')
    parser.add_argument('-l',
                        '--s3-log-output',
                        type=str,
                        default='etl_staging/logs',
                        help='Folder within the S3 bucket to upload log to')
    parser.add_argument('--log-file',
                        type=str,
                        default='logs/extract.log',
                        help='Location to save logs locally')
    args = parser.parse_args()

    DATA_LOC = args.data_location

    logging.basicConfig(level=logging.INFO,
                        filename=args.log_file,
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

        s3 = AwsS3()
        s3.upload(dfiles, args.s3_bucket, args.s3_folder)

    logging.info('================Extract complete================')

    if not args.skip_s3_upload:
        lfiles = [LOG_FILE]
        logging.info(f'Uploading {LOG_FILE} to S3')
        s3_l = AwsS3()
        s3_l.upload(lfiles, args.s3_bucket, args.s3_log_output)

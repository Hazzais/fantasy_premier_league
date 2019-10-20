import logging
import argparse

from fpltools.extract import (retrieve_data, retrieve_player_details,
                              save_intermediate_data)
from fpltools.constants import API_URLS
from fpltools.utils import get_datetime


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch download from FPL'
                                                 'website endpoints and save'
                                                 'as JSON')

    parser.add_argument('--data_location',
                        type=str,
                        default='data/',
                        help='path in which to store data')
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

    save_intermediate_data(main_data, 'main', DATA_LOC)
    save_intermediate_data(fixtures_data, 'fixtures', DATA_LOC)
    save_intermediate_data(player_data, 'players', DATA_LOC)

    logging.info('================Extract complete================')

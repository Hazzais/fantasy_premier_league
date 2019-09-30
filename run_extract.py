import os
import requests
import json
import logging
import argparse

from fpltools.constants import API_URLS
from fpltools.utils import get_datetime


def retrieve_player_details(link, player_ids, verbose=False):
    """For each player - retrieve a dictionary of their data by cycling through
    their player_ids (derived from main data set). Set verbose=True to print
    output for every one in ten players."""
    players_full = {}
    for i, pl in enumerate(player_ids):
        if verbose and i % 10 == 0:
            logging.info(f"Player number: {str(i)} of {str(len(player_ids))}")

        player_id = pl['id']
        players_full[player_id] = retrieve_data(link.format(player_id))

    return players_full


def retrieve_data(link):
    """Retrieve JSON formatted data from an API endpoint (link)"""
    logging.info(f'Reading data from link ({link}')
    try:
        response = requests.get(link)
        response.raise_for_status()
    except Exception as err:
        logging.warning(
            f'Could not load from link {link} with error: {err}')
    else:
        logging.info(f'Link {link} successfully accessed')
        return json.loads(response.text)


def save_intermediate_data(data, data_name, data_loc):
    """Save unedited data as JSON files"""
    logging.info(f'Saving {data_name} as JSON in {data_loc}')
    try:
        with open(os.path.join(data_loc, f'{data_name}.json'), 'w') as f:
            json.dump(data, f)
    except FileNotFoundError as e:
        logging.exception('Unable to find save location')
    else:
        logging.info(f'Successfully saved {data_name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch download from FPL'
                                                 'website endpoints and save as'
                                                 'JSON')

    parser.add_argument('data_location',
                        type=str,
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

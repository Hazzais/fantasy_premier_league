import os
import requests
import json
import logging

import keyring

from fpltools.constants import LOGIN_URL, API_URLS
from fpltools.utils import get_datetime_string, get_datetime

# TODO: ARGUMENTS:
# user
# (opt) fpl_service
# (opt) API URLs to use
# (opt) data location

# FUTURE: may use endpoints requiring credentials in future
# import keyring
# SERVICE = 'fpl'
# USER = 'harryafirth@gmail.com'
# user_pw = keyring.get_password(SERVICE, USER)

# TODO: arg or from yaml
DATA_LOC = 'data'


def retrieve_player_details(link, player_ids, verbose=False):
    # More complicated - for each player - retrieve a dictionary of their data
    players_full = {}
    for i, pl in enumerate(player_ids):
        if verbose and i % 10 == 0:
            logging.info(f"Player number: {str(i)} of {str(len(player_ids))}")

        player_id = pl['id']
        players_full[player_id] = retrieve_data(link.format(player_id))

    return players_full


def retrieve_data(link):
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

    parser.add_argument('d',
                        '--data_location',
                        type=str,
                        help='path in which to store data',
                        required=True)
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


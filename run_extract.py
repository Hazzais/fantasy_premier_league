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

SERVICE = 'fpl'
USER = 'harryafirth@gmail.com'

# TODO: arg or from yaml
DATA_LOC = 'data'

logging.basicConfig(level=logging.INFO,
                    filename=f'logs/extract_{get_datetime()}.log',
                    filemode='w',
                    format='%(asctime)s - %(message)s')


def retrieve_player_details(link, player_ids, verbose=False):
    # More complicated - for each player - retrieve a dictionary of their data
    players_full = {}
    for i, pl in enumerate(player_ids):
        # if verbose and i % 10 == 0:
        #     logging.log(f"Player number: {str(i)} of {str(len(player_ids))}")

        player_id = pl['id']
        players_full[player_id] = retrieve_data(link.format(player_id))

    return players_full


def retrieve_data(link):
    try:
        response = requests.get(link)
        response.raise_for_status()
    except Exception as err:
        logging.warning(
            f'Could not load from link {link} with error: {err}')
    else:
        logging.info(f'Link {link} successfully accessed')
        return json.loads(response.text)

# TODO: reference data (before this script)

if __name__ == '__main__':
    user_pw = keyring.get_password(SERVICE, USER)

    # TODO: automate below (with other possible save files
    main_data = retrieve_data(API_URLS['static'])
    fixtures_data = retrieve_data(API_URLS['fixtures'])
    player_data = retrieve_player_details(API_URLS['player'],
                                          main_data['elements'],
                                          verbose=True)

    # TODO: validation checks


    with open(os.path.join(DATA_LOC,
                           f'main_{get_datetime()}.json'), 'w') as f:
        json.dump(main_data, f)

    with open(os.path.join(DATA_LOC,
                           f'fixtures_{get_datetime()}.json'), 'w') as f:
        json.dump(fixtures_data, f)

    with open(os.path.join(DATA_LOC,
                           f'players_{get_datetime()}.json'), 'w') as f:
        json.dump(player_data, f)

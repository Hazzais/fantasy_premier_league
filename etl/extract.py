import os
import logging
import json

import requests


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

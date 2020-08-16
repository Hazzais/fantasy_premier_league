import sys
import logging
import json
import argparse

import requests

import urls
import utils


def retrieve_data_list(ids, func, verbose=False):
    """For a list of ids, get data from an endpoint corresponding to that
    returned by a function with its only argument as that id

    ids: list of strings to use to construct endpoint
    func:
    verbose: optional int or False. If not False, will log every n requests,
    where n is the value of verbose.
    """
    players_full = {}
    for i, pl in enumerate(ids):
        if verbose and i % verbose == 0:
            logging.info(f"Player number: {str(i)} of {str(len(ids))}")

        this_id = pl['id']
        players_full[this_id] = retrieve_data(func(this_id))

    return players_full


def retrieve_data(link):
    """Retrieve JSON formatted data from an API endpoint

    link: endpoint
    """
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


def extract(bucket, key_root):
    """Extracts key FPL data and saves as JSON to S3
    """
    # Main and fixture data can be extracted with one request each
    main_data = retrieve_data(urls.main)
    fixtures_data = retrieve_data(urls.fixtures)

    # Detailed player data must be retrieved by making a request per player
    player_ids = main_data['elements']
    player_data = retrieve_data_list(player_ids, urls.player_url, verbose=True)

    # Want outputs idompotent - label keys with current datetime
    datetime = utils.Datetime()
    datetime_string = datetime.get_datetime_string()

    # Outputs are stored on S3
    main_key = f'{key_root}main_{datetime_string}.json'
    utils.write_s3_json(main_data,
                        bucket=bucket,
                        key=main_key)

    fixture_key = f'{key_root}fixtures_{datetime_string}.json'
    utils.write_s3_json(fixtures_data,
                        bucket=bucket,
                        key=fixture_key)

    player_key = f'{key_root}players_{datetime_string}.json'
    utils.write_s3_json(player_data,
                        bucket=bucket,
                        key=player_key)
    return {'keys':
                {'main': main_key,
                 'fixtures': fixture_key,
                 'players': player_key},
            'datestamp': datetime_string}


def _get_args(args):
    parser = argparse.ArgumentParser(description="Extract data from official "
                                                 "FPL API endpoints, saving "
                                                 "to S3 as JSON")
    parser.add_argument('-b',
                        '--bucket',
                        type=str,
                        required=True,
                        help='S3 bucket in which to store outputs')
    parser.add_argument('-k',
                        '--key-root',
                        type=str,
                        required=True,
                        help='S3 key root within specified bucket with which '
                             'to use as start of keys to store outputs')
    return vars(parser.parse_args())


def main(args):
    pargs = _get_args(args)
    return extract(bucket=pargs['bucket'], key_root=pargs['key_root'])


if __name__ == "__main__":
    main(sys.argv[1:])

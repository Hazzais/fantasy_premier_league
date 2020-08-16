import json
import sys
import argparse

import requests
import pandas as pd

import extract



# Logged in:
# - https://fantasy.premierleague.com/api/leagues-classic/967/standings/
# - https://fantasy.premierleague.com/api/leagues-h2h-matches/league/946125/
# - https://fantasy.premierleague.com/api/my-team/91928/
# - https://fantasy.premierleague.com/api/me/
# Maybe logged in:
# - https://fantasy.premierleague.com/api/entry/91928/
# - https://fantasy.premierleague.com/api/entry/91928/event/1/picks/
# - https://fantasy.premierleague.com/api/entry/91928/history
# - https://fantasy.premierleague.com/api/entry/91928/cup/
# - https://fantasy.premierleague.com/api/entry/91928/transfers-latest/
# - https://fantasy.premierleague.com/api/entry/91928/transfers/



with open('./data/main.json', 'r') as f:
    main = json.load(f)


events = main['events']
game_settings = main['game_settings']
phases = main['phases']
teams = main['teams']


def run_etl(args):
    extract_details = extract.extract(bucket=args['bucket'],
                                      key_root=args['key_root'])

    # More steps here...

    return extract_details


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
    run_etl(pargs)


if __name__ == "__main__":
    main(sys.argv[1:])

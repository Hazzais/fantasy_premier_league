import json
import sys
import argparse

import requests
import pandas as pd
import numpy as np

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



with open('./data/main_20200823-125112.json', 'r') as f:
    main = json.load(f)

with open('./data/fixtures_20200823-125112.json', 'r') as f:
    fixtures = json.load(f)

with open('./data/players_20200823-125112.json', 'r') as f:
    players = json.load(f)

with open('./data/main_old.json', 'r') as f:
    main_old = json.load(f)

with open('./data/fixtures_old.json', 'r') as f:
    fixtures_old = json.load(f)

with open('./data/players_old.json', 'r') as f:
    players_old = json.load(f)


events = main['events']
game_settings = main['game_settings']
phases = main['phases']
teams = main['teams']
positions = main['element_types']


def season_name_to_id(season):
    return season.str.replace('/', '').astype(int)


def get_players_level(players_data, level, with_id=False):
    players_extract = []
    for k, v in players_data.items():
        if with_id:
            temp = pd.DataFrame(v[level])
            temp[with_id] = k
            players_extract.append(temp)
        else:
            players_extract.append(pd.DataFrame(v[level]))

    return pd.concat(players_extract, sort=True)


def get_players_future(player_data):
    players_future = get_players_level(player_data, 'fixtures',
                                       with_id='player_id')
    players_future_rename = {'code': 'fixture_id_long',
                            'event': 'gameweek_id',
                            'finished': 'fixture_finished',
                            'id': 'fixture_id',
                            'minutes': 'fixture_minutes',
                            'started': 'fixture_started',
                            'team_a': 'away_team_id',
                            'team_a_score': 'away_team_score',
                            'team_h': 'home_team_id',
                            'team_h_score': 'home_team_score',}
    players_future.rename(columns=players_future_rename, inplace=True)
    players_future['kickoff_time'] = pd.to_datetime(
        players_future['kickoff_time'],
        errors='coerce')
    players_future.set_index(['player_id', 'fixture_id'], inplace=True)
    players_future.sort_index(inplace=True)
    return players_future


def get_players_previous_seasons(player_data):
    players_prev_seasons = get_players_level(player_data, 'history_past',
                                             with_id='player_id')
    players_prev_seasons_rename = {'element_code': 'player_id_long'}
    players_prev_seasons.rename(columns=players_prev_seasons_rename,
                                inplace=True)
    players_prev_seasons['season_id'] = season_name_to_id(
        players_prev_seasons['season_name'])
    players_prev_seasons.set_index(['player_id', 'season_id'], inplace=True)
    players_prev_seasons.sort_index(inplace=True)
    return players_prev_seasons


def get_players_previous_fixtures(player_data):
    players_previous = get_players_level(player_data, 'history', with_id=False)
    if len(players_previous) == 0:
        return pd.DataFrame()
    players_past_rename = {'element': 'player_id',
                            'fixture': 'fixture_id',
                            'team_h_score': 'home_team_score',
                            'team_a_score': 'away_team_score',
                            'round': 'gameweek_id',
                            'was_home': 'fixture_home'}
    players_previous.rename(columns=players_past_rename, inplace=True)
    players_previous['kickoff_datetime'] = pd.to_datetime(
        players_previous['kickoff_time'],
        errors='coerce')
    players_previous.set_index(['player_id', 'fixture_id'], inplace=True)
    players_previous.sort_index(inplace=True)
    return players_previous


def get_players_single(player_data):
    players = pd.DataFrame(player_data)
    players['news_added_datetime'] = pd.to_datetime(players['news_added'],
                                                    errors='coerce')
    players_rename = {'code': 'player_id_long',
                        'element_type': 'position_id',
                        'event_points': 'gameweek_points',
                        'id': 'player_id',
                        'team': 'team_id',
                        'team_code': 'team_id_long'
                        }
    players.rename(columns=players_rename, inplace=True)
    players.set_index('player_id', inplace=True)
    players.sort_index(inplace=True)
    return players


def get_positions(positions):
    positions_df = pd.DataFrame(positions)
    positions_drop = ['plural_name', 'plural_name_short', 'ui_shirt_specific',
                        'sub_positions_locked']
    positions_df.drop(columns=positions_drop, inplace=True)
    positions_rename = {'id': 'position_id',
                        'singular_name': 'position_name_long',
                        'singular_name_short': 'position_name'}
    positions_df.rename(columns=positions_rename, inplace=True)
    positions_df.set_index('position_id', inplace=True)
    positions_df.sort_index(inplace=True)
    return positions_df


def get_teams(team_data):
    teams_df = pd.DataFrame(team_data)
    teams_rename = {'code': 'team_id_long',
                    'id': 'team_id',
                    'name': 'team_name_long',
                    'short_name': 'team_name',
                    'strength': 'team_strength',
                    'strength_overall_home': 'team_strength_overall_home',
                    'strength_overall_away': 'team_strength_overall_away',
                    'strength_attack_home': 'team_strength_attack_home',
                    'strength_attack_away': 'team_strength_attack_away',
                    'strength_defence_home': 'team_strength_defence_home',
                    'strength_defence_away': 'team_strength_defence_away',
                    }
    teams_df.rename(columns=teams_rename, inplace=True)
    teams_df.set_index('team_id', inplace=True)
    teams_df.sort_index(inplace=True)
    return teams_df


def get_gameweeks(events_data):
    gameweeks = pd.DataFrame(events_data)
    gameweeks['deadline_time'] =\
        pd.to_datetime(gameweeks['deadline_time'], errors='coerce')
    gameweeks.drop(columns=['chip_plays'], inplace=True)
    gameweek_rename = {'id': 'gameweek_id',
                    'name': 'gameweek_name',
                    'finished': 'gameweek_finished',
                    'data_checked': 'gameweek_data_checked',
                    'highest_score': 'highest_scoring_entry_score',
                    'is_previous': 'gameweek_previous',
                    'is_current': 'gameweek_current',
                    'is_next': 'gameweek_next',
                    'most_selected': 'player_id_most_selected',
                    'most_transferred_in': 'player_id_most_transferred_in',
                    'top_element': 'player_id_highest_score',
                    'most_captained': 'player_id_most_captained',
                    'most_vice_captained': 'player_id_most_vice_captained'
                    }
    gameweeks.rename(columns=gameweek_rename, inplace=True)
    gameweeks.set_index('gameweek_id', inplace=True)
    gameweeks.sort_index(inplace=True)
    return gameweeks


def get_fixtures(fixture_data):
    fixtures_df = pd.DataFrame(fixture_data)
    fixtures_df.drop(columns=['stats'], inplace=True)
    fixtures_df['kickoff_time'] = pd.to_datetime(fixtures_df['kickoff_time'],
                                                 errors='coerce')
    fixtures_rename = {'code': 'fixture_id_long',
                       'event': 'gameweek_id',
                       'finished': 'fixture_finished',
                       'finished_provisional': 'fixture_finished_provisional',
                       'id': 'fixture_id',
                       'minutes': 'fixture_minutes',
                       'started': 'fixture_started',
                       'team_a': 'away_team_id',
                       'team_a_score': 'away_team_score',
                       'team_h': 'home_team_id',
                       'team_h_score': 'home_team_score',
                       'team_h_difficulty': 'home_team_fixture_difficulty',
                       'team_a_difficulty': 'away_team_fixture_difficulty'
                       }
    fixtures_df.rename(columns=fixtures_rename, inplace=True)
    fixtures_df.set_index('fixture_id', inplace=True)
    fixtures_df.sort_index(inplace=True)
    return fixtures_df


gameweeks_df = get_gameweeks(events)
fixtures_df = get_fixtures(fixtures)
teams_df = get_teams(teams)
positions_df = get_positions(main['element_types'])
players_single_df = get_players_single(main_old['elements'])
players_previous_seasons = get_players_previous_seasons(players)
players_previous_fixtures = get_players_previous_fixtures(players)
players_future_fixtures = get_players_future(players)


players_next = players_single_df.copy()
subset = players_future_fixtures.reset_index()
subset = subset.loc[~subset['fixture_finished']]
subset.sort_values(['player_id', 'kickoff_datetime'], inplace=True)
subset = subset.groupby('player_id').head(1)
subset['opponent_team_id'] = np.nan
subset.loc[subset['is_home'], 'opponent_team_id'] = subset['away_team_id']
subset.loc[~subset['is_home'], 'opponent_team_id'] = subset['home_team_id']

subset_small = subset[['player_id', 'fixture_id', 'fixture_id_long',
                       'opponent_team_id', 'difficulty', 'is_home',
                       'kickoff_time']].copy()

# TODO: make indexes all categorical
subset_small['player_id'] = subset_small['player_id'].astype('int64')
players_next = players_next.reset_index().merge(subset_small,
                                  how='left',
                                  on='player_id',
                                  validate='one_to_one')
players_next.set_index('player_id', inplace=True)

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

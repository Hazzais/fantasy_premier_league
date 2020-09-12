import json
import sys
import argparse

import requests
import pandas as pd
import numpy as np

import extract
import predict
import utils
import additional_fifa as afifa
import load



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

# TODO: functions to determine latest and download files

dt_string = '20200912-123823'

with open(f'./data/main_{dt_string}.json', 'r') as f:
    main = json.load(f)

with open(f'./data/fixtures_{dt_string}.json', 'r') as f:
    fixtures = json.load(f)

with open(f'./data/players_{dt_string}.json', 'r') as f:
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
                            'team_h_score': 'home_team_score',
                            'is_home': 'fixture_home'}
    players_future.rename(columns=players_future_rename, inplace=True)
    players_future['kickoff_datetime'] = pd.to_datetime(
        players_future['kickoff_time'],
        errors='coerce')
    players_future.drop(columns=['kickoff_time'], inplace=True)
    cat_cols = ['player_id', 'fixture_id', 'fixture_id_long', 'gameweek_id',
                'away_team_id', 'home_team_id']
    players_future[cat_cols] = players_future[cat_cols].astype('category')
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
    cat_cols = ['player_id', 'player_id_long']
    players_prev_seasons[cat_cols] = \
        players_prev_seasons[cat_cols].astype('category')
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
    players_previous.drop(columns=['kickoff_time'], inplace=True)
    cat_cols = ['fixture_id', 'player_id', 'gameweek_id']
    players_previous[cat_cols] = players_previous[cat_cols].astype('category')
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
    cat_cols = ['player_id_long', 'player_id', 'position_id', 'team_id',
                'team_id_long']
    players[cat_cols] = players[cat_cols].astype('category')
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
    cat_cols = ['position_id']
    positions_df[cat_cols] = positions_df[cat_cols].astype('category')
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
    cat_cols = ['team_id_long', 'team_id']
    teams_df[cat_cols] = teams_df[cat_cols].astype('category')
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
    cat_cols = ['gameweek_id']
    gameweeks[cat_cols] = gameweeks[cat_cols].astype('category')
    gameweeks.set_index('gameweek_id', inplace=True)
    gameweeks.sort_index(inplace=True)
    return gameweeks


def get_fixtures(fixture_data):
    fixtures_df = pd.DataFrame(fixture_data)
    fixtures_df.drop(columns=['stats'], inplace=True)
    fixtures_df['kickoff_datetime'] = pd.to_datetime(fixtures_df['kickoff_time'],
                                                 errors='coerce')
    fixtures_df.drop(columns=['kickoff_time'], inplace=True)
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

    cat_cols = ['fixture_id_long', 'fixture_id', 'gameweek_id', 'away_team_id',
                'home_team_id']
    fixtures_df[cat_cols] = fixtures_df[cat_cols].astype('category')
    fixtures_df.set_index('fixture_id', inplace=True)
    fixtures_df.sort_index(inplace=True)
    return fixtures_df

# New. TODO: make players_single_df new
gameweeks_df = get_gameweeks(events)
fixtures_df = get_fixtures(fixtures)
teams_df = get_teams(teams)
positions_df = get_positions(main['element_types'])
players_single_df = get_players_single(main['elements'])
players_previous_seasons = get_players_previous_seasons(players)
players_previous_fixtures = get_players_previous_fixtures(players)
players_future_fixtures = get_players_future(players)


dim_gameweeks = gameweeks_df
dim_gameweeks.to_csv('./data/dim_gameweeks.csv', index=True)

dim_fixtures = fixtures_df
dim_fixtures.to_csv('./data/dim_fixtures.csv', index=True)

dim_teams = teams_df
dim_teams.to_csv('./data/dim_teams.csv', index=True)

dim_positions = positions_df
dim_positions.to_csv('./data/dim_positions.csv', index=True)



def home_away_stats(data,
                    new_col,
                    home_col,
                    away_col,
                    ha_type,
                    homeaway_marker_col='fixture_home'):
    data[new_col] = np.nan
    if ha_type == 'team':
        home_mask = data[homeaway_marker_col]
        away_mask = ~data[homeaway_marker_col]
    elif ha_type == 'opponent':
        home_mask = ~data[homeaway_marker_col]
        away_mask = data[homeaway_marker_col]
    else:
        raise ValueError(f'ha_type {ha_type} should be either team or opponent')

    data.loc[home_mask, new_col] = data[home_col]
    data.loc[away_mask, new_col] = data[away_col]
    return data


# player-fixtures
x = set(players_previous_fixtures.columns)
y = set(players_future_fixtures.columns)
list(x)
list(y)

# In both
list(x & y)

# In previous (add team_id, fixture_finished, 'difficulty',
#   'home_team_id', 'away_team_id', 'fixture_minutes',
# '') - merge on fixtures
list(x - y)
p_old = players_previous_fixtures.copy()
orig_index = p_old.index
p_old.reset_index(inplace=True)

fixture_cols = ['fixture_finished',
       'fixture_minutes',
       'away_team_id',
       'home_team_id',
       'home_team_fixture_difficulty',
       'away_team_fixture_difficulty']
add_fixtures = fixtures_df[fixture_cols].reset_index()
p_old = p_old.merge(add_fixtures,
                    how='left',
                    on='fixture_id',
                    validate='many_to_one')
p_old = home_away_stats(p_old,
                        'team_id',
                        'home_team_id',
                        'away_team_id',
                        'team')
p_old = home_away_stats(p_old,
                        'team_score',
                        'home_team_score',
                        'away_team_score',
                        'team')
p_old = home_away_stats(p_old,
                        'opponent_score',
                        'home_team_score',
                        'away_team_score',
                        'opponent')
p_old = home_away_stats(p_old,
                        'difficulty',
                        'home_team_fixture_difficulty',
                        'away_team_fixture_difficulty',
                        'team')
p_old.drop(columns=['away_team_id', 'home_team_id',
                    'home_team_fixture_difficulty',
                    'away_team_fixture_difficulty'], inplace=True)
p_old[['player_id', 'fixture_id', 'gameweek_id']] = \
    p_old[['player_id', 'fixture_id', 'gameweek_id']].astype('int64')

# In future (add team_id, opponent_team|
# remove fixture_id_long, provisional_start_time, event_name)
list(y - x)
p_new = players_future_fixtures.copy()
p_new = home_away_stats(p_new,
                        'team_id',
                        'home_team_id',
                        'away_team_id',
                        'team')
p_new = home_away_stats(p_new,
                        'opponent_team',
                        'home_team_id',
                        'away_team_id',
                        'opponent')
p_new = home_away_stats(p_new,
                        'team_score',
                        'home_team_score',
                        'away_team_score',
                        'team')
p_new = home_away_stats(p_new,
                        'opponent_score',
                        'home_team_score',
                        'away_team_score',
                        'opponent')
p_new.drop(columns=['fixture_id_long', 'provisional_start_time', 'event_name',
                    'home_team_id', 'away_team_id',
                    'home_team_score', 'away_team_score'], inplace=True)
orig_index = p_new.index
p_new.reset_index(inplace=True)
p_new[['player_id', 'fixture_id', 'gameweek_id']] = \
    p_new[['player_id', 'fixture_id', 'gameweek_id']].astype('int64')

p_combined = pd.concat((p_new, p_old), ignore_index=True, sort=False)
p_combined.sort_values(['player_id', 'gameweek_id', 'kickoff_datetime'], inplace=True)

p_combined.drop_duplicates(subset=['player_id', 'fixture_id'], keep='last', inplace=True)

gw_count = p_combined.groupby('player_id').size()
assert gw_count.max() == 38

p_combined.set_index(['player_id', 'fixture_id'], inplace=True)
p_combined.to_csv('./data/player_result_fixtures.csv', index=True)

# dim_players
player_specific = ['player_id_long',
                   'dreamteam_count',
                   'position_id',
                   'first_name',
                   'form',
                   'photo',
                   'points_per_game',
                   'second_name',
                   'special',
                   'squad_number',
                   'team_id',
                   'total_points',
                   'transfers_in',
                   'transfers_out',
                   'value_form',
                   'value_season',
                   'web_name',
                   'minutes',
                   'goals_scored',
                   'assists',
                   'clean_sheets',
                   'goals_conceded',
                   'own_goals',
                   'penalties_saved',
                   'penalties_missed',
                   'yellow_cards',
                   'red_cards',
                   'saves',
                   'bonus',
                   'bps',
                   'influence',
                   'creativity',
                   'threat',
                   'ict_index',
                   'influence_rank',
                   'influence_rank_type',
                   'creativity_rank',
                   'creativity_rank_type',
                   'threat_rank',
                   'threat_rank_type',
                   'ict_index_rank',
                   'ict_index_rank_type']
players_base = players_single_df[player_specific].copy()
players_base.rename(columns={'team_id': 'team_id_current'}, inplace=True)


# Old. TODO: remove
# events_old = main_old['events']
# game_settings_old = main_old['game_settings']
# phases_old = main_old['phases']
# teams_old = main_old['teams']
# positions_old = main_old['element_types']
# gameweeks_df_old = get_gameweeks(events_old)
# fixtures_df_old = get_fixtures(fixtures_old)
# teams_df_old = get_teams(teams_old)
# positions_df_old = get_positions(main_old['element_types'])
# players_single_df_old = get_players_single(main_old['elements'])
# players_previous_seasons_old = get_players_previous_seasons(players_old)
# players_previous_fixtures_old = get_players_previous_fixtures(players_old)
# players_future_fixtures_old = get_players_future(players_old)


# FIFA mapping
from configparser import ConfigParser, NoSectionError

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import keyring

def get_config_section(section, path='db_config.ini'):
    """"Retrieve general config data from config file"""
    parser = ConfigParser()
    parser.read(path)

    try:
        config_section = dict(parser.items(section))
    except NoSectionError:
        raise NoSectionError(f'Check ini file for [{section}]')
    else:
        return config_section


def get_db_config(path='db_config.ini', section='db'):
    """Get main config for database"""
    db_config = get_config_section(section, path=path)

    db_config.update(
        {'password': keyring.get_password(db_config['keyring_key'],
                                          db_config['user'])}
        )
    return db_config


def set_db(db_config):
    db_url = f"postgresql://"\
        f"{db_config['user']}:{db_config['password']}@"\
        f"{db_config['host']}:{db_config['port']}/"\
        f"{db_config['db']}"
    return create_engine(db_url)


def clear_table(engine, table):
    """Drop lookup table"""
    with engine.connect() as conn:
        conn.execute(f"""DROP TABLE {table}""")


pargs = _get_args(args)

db_config = get_db_config()
task_config = get_config_section('fpl_fifa_mapping')
table_name = task_config['table_name']
source_data = task_config['source_csv']

engine = set_db(db_config)

if pargs['rebuild']:
    # If requested, delete table to rebuild
    clear_table(engine, table_name)

fpl_data = players_base[['first_name', 'second_name', 'position_id',
                         'team_id_current']].copy().reset_index()
fpl_data['fpl_player_name'] = fpl_data['first_name'] + ' ' + fpl_data['second_name']
fpl_data = fpl_data.merge(positions_df['position_name'],
                             left_on='position_id',
                             right_index=True,
                             validate='many_to_one')
fpl_data = fpl_data.merge(teams_df['team_name_long'],
                             left_on='team_id_current',
                             right_index=True,
                             validate='many_to_one')
fifa_data = afifa.BuildFifaData().load_fifa_data(filepath=source_data)

MATCH_LOOKUP_DEF = """CREATE TABLE {} (
    player_id VARCHAR(3) NOT NULL UNIQUE REFERENCES players_summary(player_id),
    sofifa_id VARCHAR(20),
    match_best INT,
    fpl_player_name VARCHAR(550),
    fifa_name_short VARCHAR(550),
    fifa_name_long VARCHAR(550)
    )
    """

# Only need to, by default, match new players (as it can take long time for
# all players)
# TODO: this should actually check dim_players
# TODO: if table not exists, look to match all
subset_data = afifa.get_new_player_frame(fpl_data, engine,
                                         lookup_table=table_name)

# TODO: temp
# subset_data = fpl_data.sample(40).copy()

if len(subset_data):
    # Players split into batches depending on config value
    n_batches = afifa.get_batches_needed(subset_data, fifa_data, task_config)
    batched_data = afifa.batch_preparation(subset_data, n_batches)

    # Perform matching, can take a while for lots of players
    matched_data = afifa.batch_matching(batched_data, fifa_data)

    # Recombine individual batches
    matched_data_full = afifa.combine_batch_matches(matched_data)

    # Any final cleaning of matched data
    matched_data_full = afifa.final_preparation(matched_data_full)

    # TODO: instead, all ids should be in dim_players
    # bsu = load.BatchSQLUpdate(matched_data_full, engine, MATCH_LOOKUP_DEF,
    #                           'lkp_fpl_fifa')
    # bsu.batch_append()

# TODO: temp
import pickle
with open('data/temp_matches.pkl', 'wb') as f:
    pickle.dump(matched_data, f)


with open('data/temp_matches.pkl', 'rb') as f:
    matched_data = pickle.load(f)


# TODO: this should instead get existing lookup (i.e. any mapping in dim_player table
# without those in update - concat)
name_dim_players = 'dim_players'
from sqlalchemy.exc import ProgrammingError
try:
    existing_mapping = pd.read_sql(f"""SELECT DISTINCT player_id, sofifa_id
                        FROM {name_dim_players} WHERE sofifa_id != 'nan'""", engine)
except ProgrammingError:
    full_lookup = matched_data_full.loc[(matched_data_full['sofifa_id'].notna()) &
                                          (matched_data_full['sofifa_id'] != 'nan'),
                                         ['player_id', 'sofifa_id']]
else:
    full_lookup = pd.concat((existing_mapping,
                             matched_data_full[['player_id', 'sofifa_id']]))
finally:
    full_lookup['sofifa_id'] = full_lookup['sofifa_id'].astype('int64')

# existing_mapping = pd.read_sql(f"""SELECT DISTINCT player_id, sofifa_id
#                         FROM {table_name} WHERE sofifa_id != 'nan'""", engine)

# # TEMP:
# existing_mapping = matched_data_full.loc[(matched_data_full['sofifa_id'].notna()) &
#                                           (matched_data_full['sofifa_id'] != 'nan'),
#                                          ['player_id', 'sofifa_id']]

# existing_mapping['sofifa_id'] = existing_mapping['sofifa_id'].astype('int64')

fifa_data_full = pd.read_csv(source_data)
fifa_data_full.drop(columns=['player_url', 'short_name', 'long_name', 'age',
                             'club'], inplace=True)
fifa_data_full = fifa_data_full.merge(existing_mapping,
                                      on='sofifa_id',
                                      how='inner',
                                      validate='one_to_many')
fifa_data_full['player_id'] = fifa_data_full['player_id'].astype('int64')
dim_players = players_base.merge(fifa_data_full,
                                  on='player_id',
                                  how='left',
                                  validate='one_to_one')
dim_players.set_index('player_id', inplace=True)
dim_players.to_csv('./data/dim_players.csv', index=True)


# fact_players_current
include_fact_current = ['chance_of_playing_next_round',
                       'chance_of_playing_this_round',
                       'cost_change_event',
                       'cost_change_event_fall',
                       'position_id',
                       'ep_next',
                       'ep_this',
                       'gameweek_points',
                       'form',
                       'points_per_game',
                       'in_dreamteam',
                       'news',
                       'news_added',
                       'now_cost',
                       'status',
                       'team_id',
                       'total_points',
                       'transfers_in',
                       'transfers_in_event',
                       'transfers_out',
                       'transfers_out_event',
                       'value_form',
                       'value_season',
                       'web_name',
                       'minutes',
                       'goals_scored',
                       'assists',
                       'clean_sheets',
                       'goals_conceded',
                       'own_goals',
                       'penalties_saved',
                       'penalties_missed',
                       'yellow_cards',
                       'red_cards',
                       'saves',
                       'bonus',
                       'bps',
                       'influence',
                       'creativity',
                       'threat',
                       'ict_index',
                       'influence_rank',
                       'influence_rank_type',
                       'creativity_rank',
                       'creativity_rank_type',
                       'threat_rank',
                       'threat_rank_type',
                       'ict_index_rank',
                       'ict_index_rank_type',
                       'news_added_datetime']

fact_players_current = players_single_df[include_fact_current].reset_index().copy()
subset = players_future_fixtures.reset_index().copy()
subset = subset.loc[~subset['fixture_finished']]
subset.sort_values(['player_id', 'kickoff_datetime'], inplace=True)
subset = subset.groupby('player_id').head(1)
subset['opponent_team_id'] = np.nan
subset.loc[subset['fixture_home'], 'opponent_team_id'] = subset['away_team_id']
subset.loc[~subset['fixture_home'], 'opponent_team_id'] = subset['home_team_id']

subset_small = subset[['player_id', 'fixture_id', 'fixture_id_long',
                       'opponent_team_id', 'difficulty', 'fixture_home',
                       'kickoff_datetime']].copy()

subset_small['player_id'] = subset_small['player_id'].astype('int64')
fact_players_current['player_id'] = fact_players_current['player_id'].astype('int64')
fact_players_current = fact_players_current.merge(subset_small,
                                                  how='left',
                                                  on='player_id',
                                                  validate='one_to_one')

# Apply predictive model. TODO: actual one - update predict module when ready
fact_players_current_predict = predict.apply_models(fact_players_current)

fact_players_current_predict.to_csv('./data/fact_players_current_predict.csv',
                                    index=False)


# Load

table = ''
dataframe = ''
method = 'overwrite'  # overwrite/append/update

engine = set_db(db_config)



from collections import namedtuple




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
    parser.add_argument('-r', '--rebuild',
                        help='Delete and rebuild lookup',
                        action='store_true')
    return vars(parser.parse_args())


def main(args):
    pargs = _get_args(args)
    run_etl(pargs)


if __name__ == "__main__":
    main(sys.argv[1:])

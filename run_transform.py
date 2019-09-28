import os
import json
import re

import pandas as pd
import numpy as np

# TODO: ARGS
DATA_LOC = 'data/'


def dval_unique_index(df):
    return len(df.index.unique()) == len(df.index)


def pandas_integerstr_to_int(x):
    if np.isnan(x):
        return np.nan
    else:
        return re.sub(r'(\.\d+)', '', str(x))


# TODO: Automate or get rid of temporary files
DT1 = '1569351282'
DT2 = '1569351282'
DT3 = '1569351272'
with open(os.path.join(DATA_LOC, f'fixtures_{DT1}.json'), 'r') as f:
    fixtures_data = json.load(f)

with open(os.path.join(DATA_LOC, f'players_{DT2}.json'), 'r') as f:
    player_data = json.load(f)

with open(os.path.join(DATA_LOC, f'main_{DT3}.json'), 'r') as f:
    main_data = json.load(f)

# Data: fixtures
fixtures_rename = {'code': 'fixture_id_long',
                   'event': 'gameweek',
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

fixtures_drop = ['kickoff_time', 'provisional_start_time']
fixtures_str_cols = ['fixture_id',
                     'fixture_id_long',
                     'gameweek',
                     'away_team_id',
                     'home_team_id']
fixtures_index = ['fixture_id']

df_fixtures = pd.DataFrame(fixtures_data)
df_fixtures['fixture_kickoff_datetime'] =\
    pd.to_datetime(df_fixtures['kickoff_time'], errors='coerce')
df_fixtures.drop(columns=['stats'] + fixtures_drop, inplace=True)
df_fixtures.rename(columns=fixtures_rename, inplace=True)
df_fixtures[fixtures_str_cols] = df_fixtures[fixtures_str_cols].astype(str)
df_fixtures.sort_values(fixtures_index, inplace=True)

# Data: gameweeks
gameweek_rename = {'id': 'gameweek',
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
gameweek_drop = ['deadline_time',
                 'deadline_time_epoch',
                 'deadline_time_game_offset',
                 'chip_plays',
                 'top_element_info']
gameweek_index = ['gameweek']
gameweek_str_cols = ['gameweek',
                     'highest_scoring_entry',
                     'player_id_most_selected',
                     'player_id_most_transferred_in',
                     'player_id_highest_score',
                     'player_id_most_captained',
                     'player_id_most_vice_captained']

df_gameweeks = pd.DataFrame(main_data['events'])
df_gameweeks['gameweek_deadline_time'] =\
    pd.to_datetime(df_gameweeks['deadline_time'], errors='coerce')
df_gameweeks.drop(columns=gameweek_drop, inplace=True)
df_gameweeks.rename(columns=gameweek_rename, inplace=True)
df_gameweeks[gameweek_str_cols] = df_gameweeks[gameweek_str_cols]\
    .applymap(pandas_integerstr_to_int)
df_gameweeks.sort_values(gameweek_index, inplace=True)

# Data: teams
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
teams_drop = ['draw', 'form', 'loss', 'played', 'points', 'position',
              'team_division', 'unavailable', 'win']
teams_str_cols = ['team_id_long', 'team_id']
teams_index = ['team_id']

df_teams = pd.DataFrame(main_data['teams'])
df_teams.rename(columns=teams_rename, inplace=True)
df_teams.drop(columns=teams_drop, inplace=True)
df_teams[teams_str_cols] = df_teams[teams_str_cols].astype(str)
df_teams.sort_values(teams_index, inplace=True)

# Reference data: positions
positions_rename = {'id': 'position_id',
                    'singular_name': 'position_name_long',
                    'singular_name_short': 'position_name'}
positions_drop = ['plural_name', 'plural_name_short', 'ui_shirt_specific',
                  'sub_positions_locked']
positions_str_cols = ['position_id']
positions_index = ['position_id']

df_positions = pd.DataFrame(main_data['element_types'])
df_positions.rename(columns=positions_rename, inplace=True)
df_positions.drop(columns=positions_drop, inplace=True)
df_positions[positions_str_cols] = df_positions[positions_str_cols].astype(str)
df_positions.sort_values(positions_index, inplace=True)

# Data: players - single row per player with current stats for this point and
# aggregated up to this point
players_sum_rename = {'code': 'player_id_long',
                      'element_type': 'position_id',
                      'event_points': 'gameweek_points',
                      'id': 'player_id',
                      'team': 'team_id',
                      'team_code': 'team_id_long'}
players_sum_drop = ['squad_number', 'web_name']
players_sum_str_cols = ['player_id_long', 'position_id', 'player_id']
players_sum_index = ['player_id']

df_players_sum = pd.DataFrame(main_data['elements'])
df_players_sum['news_added_datetime'] = pd.to_datetime(
    df_players_sum['news_added'],
    errors='coerce')
df_players_sum.rename(columns=players_sum_rename, inplace=True)
df_players_sum.drop(columns=players_sum_drop, inplace=True)
df_players_sum[players_sum_str_cols] =\
    df_players_sum[players_sum_str_cols].astype(str)
df_players_sum.sort_values(players_sum_index, inplace=True)

# Data: players - one row per player per fixture with stats for that fixture
# only
df_players_past = []
df_players_future = []
df_players_prev_seasons = []
for k, p in player_data.items():
    df_players_past.append(pd.DataFrame(p['history']))
    fut = pd.DataFrame(p['fixtures'])
    fut['player_id'] = k
    df_players_future.append(fut)
    df_players_prev_seasons.append(pd.DataFrame(p['history_past']))

# Data: player performance in previous seasons
players_prev_seasons_rename = {'element_code': 'player_id_long'}
players_prev_seasons_drop = []
players_prev_seasons_index = ['player_id_long', 'season_name']
players_prev_seasons_str_cols = ['player_id_long']

df_players_prev_seasons = pd.concat(df_players_prev_seasons)
df_players_prev_seasons.rename(columns=players_prev_seasons_rename,
                               inplace=True)
df_players_prev_seasons.drop(columns=players_prev_seasons_drop, inplace=True)
df_players_prev_seasons[players_prev_seasons_str_cols] =\
    df_players_prev_seasons[players_prev_seasons_str_cols].astype(str)
df_players_prev_seasons.sort_values(players_prev_seasons_index, inplace=True)

# Data: players in previous fixtures this season
players_past_rename = {'element': 'player_id',
                       'fixture': 'fixture_id',
                       'round': 'gameweek',
                       'was_home': 'fixture_home'}
players_past_drop = ['kickoff_time', 'opponent_team']
players_past_str_cols = ['player_id', 'fixture_id', 'gameweek']
players_past_index = ['player_id', 'fixture_id']
df_players_past = pd.concat(df_players_past)

df_players_past.rename(columns=players_past_rename, inplace=True)
df_players_past['kickoff_datetime'] = pd.to_datetime(
    df_players_past['kickoff_time'],
    errors='coerce')
df_players_past.drop(columns=players_past_drop, inplace=True)
df_players_past[players_past_str_cols] =\
    df_players_past[players_past_str_cols].astype(str)
df_players_past = pd.merge(df_players_past, df_fixtures[
    ['fixture_id', 'fixture_id_long', 'away_team_id', 'home_team_id']],
                           how='inner',
                           right_on='fixture_id',
                           left_on='fixture_id'
                           )
df_players_past.sort_values(players_past_index, inplace=True)

# Data: players' remaining fixtures
player_future_rename = {'event': 'gameweek',
                        'code': 'fixture_id_long',
                        'team_h': 'home_team_id',
                        'team_a': 'away_team_id',
                        'is_home': 'fixture_home'}
players_future_drop = ['kickoff_time', 'event_name']
players_future_str_cols = ['fixture_id_long',
                           'gameweek',
                           'home_team_id',
                           'away_team_id']
players_future_index = ['player_id', 'fixture_id_long']
df_players_future = pd.concat(df_players_future)
df_players_future.rename(columns=player_future_rename, inplace=True)
df_players_future['kickoff_datetime'] =\
    pd.to_datetime(df_players_future['kickoff_time'], errors='coerce')
df_players_future.drop(columns=players_future_drop, inplace=True)
df_players_future[players_future_str_cols] =\
    df_players_future[players_future_str_cols].astype(str)
df_players_future = pd.merge(df_players_future,
                             df_fixtures[['fixture_id', 'fixture_id_long']],
                             how='inner',
                             on='fixture_id_long'
                             )
df_players_future.sort_values(players_future_index, inplace=True)

# Data: players - one row per fixture for this season's previous and remaining
# fixtures
df_players_full = pd.concat((df_players_past, df_players_future), sort=False)

df_players_full.sort_values(['player_id', 'kickoff_datetime'], inplace=True)
df_players_full['team_id'] = np.where(df_players_full['fixture_home'],
                                      df_players_full['home_team_id'],
                                      df_players_full['away_team_id'])
df_players_full = pd.merge(df_players_full,
                           df_players_sum[['player_id', 'position_id']],
                           how='left',
                           on='player_id')

df_fixtures.set_index(fixtures_index, inplace=True)
df_gameweeks.set_index(gameweek_index, inplace=True)
df_teams.set_index(teams_index, inplace=True)
df_positions.set_index(positions_index, inplace=True)
df_players_sum.set_index(players_sum_index, inplace=True)
df_players_prev_seasons.set_index(players_prev_seasons_index, inplace=True)
df_players_past.set_index(players_past_index, inplace=True)
df_players_future.set_index(players_future_index, inplace=True)

assert dval_unique_index(df_fixtures)
assert dval_unique_index(df_gameweeks)
assert dval_unique_index(df_teams)
assert dval_unique_index(df_positions)
assert dval_unique_index(df_players_sum)
assert dval_unique_index(df_players_prev_seasons)
assert dval_unique_index(df_players_past)
assert dval_unique_index(df_players_future)

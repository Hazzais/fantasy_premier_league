import logging
import argparse
import warnings

import pandas as pd
import numpy as np

from transform import (load_json, check_unique_index,
                       check_not_null_index, pickle_data,
                       pandas_integerstr_to_int)
from fpltools.utils import AwsS3

IN_FIXTURES = 'fixtures.json'
IN_PLAYERS = 'players.json'
IN_MAIN = 'main.json'

OUT_FIXTURES = 'fixtures'
OUT_GAMEWEEKS = 'gameweeks'
OUT_TEAMS = 'teams'
OUT_POSITIONS = 'positions'
OUT_PLAYERS_SUM = 'players_summary'
OUT_PLAYERS_PREVIOUS_SEASONS = 'players_previous_seasons'
OUT_PLAYERS_PAST = 'players_past'
OUT_PLAYERS_FUTURE = 'players_future'
OUT_PLAYERS_FULL = 'players_full'
OUT_TEAM_RESULTS = 'team_results'
OUT_LEAGUE_TABLE = 'league_table'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transformations of '
                                                 'previously batch downloaded '
                                                 'JSON files')

    parser.add_argument('--data_input',
                        type=str,
                        default='data/',
                        help='path from which to load data')
    parser.add_argument('--data_output',
                        type=str,
                        default='data/',
                        help='path in which to store data')
    parser.add_argument('-r',
                        '--raise-errors',
                        action='store_false',
                        help='stop on data validation exception')
    parser.add_argument('-s',
                        '--skip-s3-upload',
                        action='store_true',
                        help='Do not attempt any upload to AWS S3')
    parser.add_argument('-b',
                        '--s3-bucket',
                        type=str,
                        default='fpl-alldata',
                        help='S3 bucket to upload to')
    parser.add_argument('-f',
                        '--s3-folder',
                        type=str,
                        default='etl_staging/transformed',
                        help='Folder within the S3 bucket to upload to')
    parser.add_argument('-l',
                        '--s3-log-output',
                        type=str,
                        default='etl_staging/logs',
                        help='Folder within the S3 bucket to upload log to')
    parser.add_argument('--log-file',
                        type=str,
                        default='logs/extract.log',
                        help='Location to save logs locally')
    args = parser.parse_args()

    DATA_LOC = args.data_input
    DATA_LOC_OUT = args.data_output
    RAISE_ERRORS = args.raise_errors

    logging.basicConfig(level=logging.INFO,
                        filename=args.log_file,
                        filemode='w',
                        format='%(levelname)s - %(asctime)s - %(message)s')

    fixtures_data = load_json(IN_FIXTURES, DATA_LOC)
    player_data = load_json(IN_PLAYERS, DATA_LOC)
    main_data = load_json(IN_MAIN, DATA_LOC)

    # Data: fixtures
    logging.info('Beginning transform of fixtures data')
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

    fixtures_drop = ['kickoff_time', 'provisional_start_time']
    fixtures_str_cols = ['fixture_id',
                         'fixture_id_long',
                         'gameweek_id',
                         'away_team_id',
                         'home_team_id']
    fixtures_index = ['fixture_id']

    df_fixtures = pd.DataFrame(fixtures_data)
    df_fixtures['fixture_kickoff_datetime'] =\
        pd.to_datetime(df_fixtures['kickoff_time'], errors='coerce')
    df_fixtures.drop(columns=['stats'] + fixtures_drop, inplace=True)
    df_fixtures.rename(columns=fixtures_rename, inplace=True)
    df_fixtures[fixtures_str_cols] = df_fixtures[fixtures_str_cols] \
        .applymap(pandas_integerstr_to_int)
    df_fixtures.sort_values(fixtures_index, inplace=True)

    # Case exists where if a fixture has been postponed without being rescheduled,
    # the gameweek will be null in certain tables (it won't appear in gameweeks).
    # This is a non-fatal error, but it requires rows to be dropped from later
    # table(s) (df_players_future) so a warning must be displayed.
    if sum(df_fixtures.gameweek_id.isna()):
        missing_gameweeks = True
        warn_msg = "At least one fixture does not have an assigned gameweek. " \
                   "Records may be dropped in player tables to accommodate this."
        warnings.warn(warn_msg)
    else:
        missing_gameweeks = False

    logging.info('Completed transform of fixtures data')

    # Data: gameweeks
    logging.info('Beginning gameweek of fixtures data')
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
    gameweek_drop = ['deadline_time',
                     'deadline_time_epoch',
                     'deadline_time_game_offset',
                     'chip_plays',
                     'top_element_info']
    gameweek_index = ['gameweek_id']
    gameweek_str_cols = ['gameweek_id',
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
    logging.info('Completed transform of gameweek data')

    # Data: teams
    logging.info('Beginning transform of teams data')
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
    df_teams[teams_str_cols] = df_teams[teams_str_cols] \
        .applymap(pandas_integerstr_to_int)
    df_teams.sort_values(teams_index, inplace=True)
    logging.info('Completed transform of teams data')

    # Reference data: positions
    logging.info('Beginning transform of positions data')
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
    df_positions[positions_str_cols] = df_positions[positions_str_cols] \
        .applymap(pandas_integerstr_to_int)

    df_positions.sort_values(positions_index, inplace=True)
    logging.info('Completed transform of positions data')

    # Data: players - single row per player with current stats for this point
    # and aggregated up to this point
    logging.info('Beginning transform of player summary data')
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
    df_players_sum[players_sum_str_cols] = df_players_sum[players_sum_str_cols] \
        .applymap(pandas_integerstr_to_int)
    df_players_sum.sort_values(players_sum_index, inplace=True)
    logging.info('Completed transform of player summary data')

    # Data: players - one row per player per fixture with stats for that
    # fixture only
    logging.info('Extracting players from dictionary into dataframes')
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
    logging.info('Beginning transform of player previous seasons data')
    players_prev_seasons_rename = {'element_code': 'player_id_long'}
    players_prev_seasons_drop = []
    players_prev_seasons_index = ['player_id_long', 'season_name']
    players_prev_seasons_str_cols = ['player_id_long']

    df_players_prev_seasons = pd.concat(df_players_prev_seasons)
    df_players_prev_seasons.rename(columns=players_prev_seasons_rename,
                                   inplace=True)
    df_players_prev_seasons.drop(columns=players_prev_seasons_drop,
                                 inplace=True)
    df_players_prev_seasons[players_prev_seasons_str_cols] =\
        df_players_prev_seasons[players_prev_seasons_str_cols]\
            .applymap(pandas_integerstr_to_int)
    df_players_prev_seasons.sort_values(players_prev_seasons_index,
                                        inplace=True)
    logging.info('Completed transform of player previous seasons data')

    # Data: players in previous fixtures this season
    logging.info('Beginning transform of previous player fixtures data')
    players_past_rename = {'element': 'player_id',
                           'fixture': 'fixture_id',
                           'team_h_score': 'home_team_score',
                           'team_a_score': 'away_team_score',
                           'round': 'gameweek_id',
                           'was_home': 'fixture_home'}
    players_past_drop = ['kickoff_time', 'opponent_team']
    players_past_str_cols = ['player_id', 'fixture_id', 'gameweek_id']
    players_past_index = ['player_id', 'fixture_id']
    df_players_past = pd.concat(df_players_past)

    df_players_past.rename(columns=players_past_rename, inplace=True)
    df_players_past['kickoff_datetime'] = pd.to_datetime(
        df_players_past['kickoff_time'],
        errors='coerce')
    df_players_past.drop(columns=players_past_drop, inplace=True)
    df_players_past[players_past_str_cols] = df_players_past[players_past_str_cols] \
            .applymap(pandas_integerstr_to_int)
    df_players_past = pd.merge(df_players_past, df_fixtures[
        ['fixture_id', 'fixture_id_long', 'away_team_id', 'home_team_id']],
                               how='inner',
                               right_on='fixture_id',
                               left_on='fixture_id'
                               )
    df_players_past.sort_values(players_past_index, inplace=True)
    logging.info('Completed transform of previous player fixtures data')

    # Data: players' remaining fixtures
    logging.info('Beginning transform of remaining player fixtures data')
    player_future_rename = {'event': 'gameweek_id',
                            'code': 'fixture_id_long',
                            'team_h': 'home_team_id',
                            'team_a': 'away_team_id',
                            'team_h_score': 'home_team_score',
                            'team_a_score': 'away_team_score',
                            'is_home': 'fixture_home'}
    players_future_drop = ['kickoff_time', 'event_name']
    players_future_str_cols = ['fixture_id_long',
                               'gameweek_id',
                               'home_team_id',
                               'away_team_id']
    players_future_index = ['player_id', 'fixture_id_long']
    df_players_future = pd.concat(df_players_future)
    df_players_future.rename(columns=player_future_rename, inplace=True)
    df_players_future['kickoff_datetime'] =\
        pd.to_datetime(df_players_future['kickoff_time'], errors='coerce')
    df_players_future.drop(columns=players_future_drop, inplace=True)

    # Account for unscheduled games (otherwise there will be primary key issues with the
    # gameweek later)
    if missing_gameweeks:
        missing_gameweek_player_rows = df_players_future['gameweek_id'].isna()
        n_missing_gameweek_player_rows = np.sum(missing_gameweek_player_rows)
        n_missing_gameweek_fixtures =\
            df_players_future.loc[missing_gameweek_player_rows, 'fixture_id_long'].nunique()
        logging.info(f"There are {n_missing_gameweek_player_rows} player rows having been "
                     f"deleted due to {n_missing_gameweek_fixtures} fixtures which have not "
                     f"been (re)scheduled. These will be deleted.")
        df_players_future = df_players_future[~missing_gameweek_player_rows]

    df_players_future[players_future_str_cols] = df_players_future[players_future_str_cols] \
        .applymap(pandas_integerstr_to_int)

    df_players_future = pd.merge(df_players_future,
                                 df_fixtures[['fixture_id',
                                              'fixture_id_long']],
                                 how='inner',
                                 on='fixture_id_long'
                                 )
    df_players_future.sort_values(players_future_index, inplace=True)
    logging.info('Completed transform of remaining player fixtures data')

    # Data: players - one row per fixture for this season's previous and
    # remaining fixtures
    logging.info('Combining previous and remaining player fixture data')
    players_full_index = ['player_id', 'gameweek_id', 'fixture_id']
    df_players_full = pd.concat((df_players_past, df_players_future),
                                sort=False)

    df_players_full.sort_values(['player_id', 'kickoff_datetime'],
                                inplace=True)
    df_players_full['team_id'] = np.where(df_players_full['fixture_home'],
                                          df_players_full['home_team_id'],
                                          df_players_full['away_team_id'])
    df_players_full = pd.merge(df_players_full,
                               df_players_sum[['player_id', 'position_id']],
                               how='left',
                               on='player_id')
    # For current gameweek (depending on when data is taken), both past and
    # future can contain the same row. This needs to be removed.
    duplicate_rows = df_players_full.duplicated(subset=players_full_index,
                                                keep=False)
    drop_rows = pd.isna(df_players_full.total_points) & duplicate_rows
    df_players_full = df_players_full[~drop_rows]
    df_players_full.sort_values(players_full_index, inplace=True)

    # Data: team results
    logging.info('Beginning transform of team results data')
    team_results_cols = ['fixture_id_long',
                         'fixture_id',
                         'gameweek_id',
                         'away_team_id',
                         'home_team_id',
                         'away_team_score',
                         'home_team_score',
                         'fixture_kickoff_datetime',
                         'fixture_finished']
    team_results_index = ['team_id', 'fixture_id']
    home = df_fixtures[team_results_cols].copy()
    home.rename(columns={'fixture_finished': 'played',
                         'home_team_id': 'team_id',
                         'away_team_id': 'opponent_team_id',
                         'home_team_score': 'goals_scored',
                         'away_team_score': 'goals_conceded'}, inplace=True)
    home['fixture_home'] = True
    home['win'] = home['played'] & (home['goals_scored'] >
                                    home['goals_conceded'])
    home['draw'] = home['played'] & (home['goals_scored'] ==
                                     home['goals_conceded'])
    home['loss'] = home['played'] & (home['goals_scored'] <
                                     home['goals_conceded'])
    home['points'] = home['win'] * 3 + home['draw'] * 1
    home.loc[~home['played'], ['win', 'draw', 'loss']] = np.nan
    home['goal_difference'] = home['goals_scored'] - home['goals_conceded']

    away = df_fixtures[team_results_cols].copy()
    away.rename(columns={'fixture_finished': 'played',
                         'away_team_id': 'team_id',
                         'home_team_id': 'opponent_team_id',
                         'away_team_score': 'goals_scored',
                         'home_team_score': 'goals_conceded'}, inplace=True)
    away['fixture_home'] = False
    away['win'] = away['played'] & (away['goals_scored'] >
                                    away['goals_conceded'])
    away['draw'] = away['played'] & (away['goals_scored'] ==
                                     away['goals_conceded'])
    away['loss'] = away['played'] & (away['goals_scored'] <
                                     away['goals_conceded'])
    away['points'] = away['win'] * 3 + away['draw'] * 1
    away.loc[~away['played'], ['win', 'draw', 'loss']] = np.nan
    away['goal_difference'] = away['goals_scored'] - away['goals_conceded']

    df_team_results = pd.concat([home, away], sort=False)
    df_team_results.sort_values(['team_id', 'fixture_kickoff_datetime'],
                                inplace=True)
    logging.info('Completed transform of team results data')

    # Data: Premier League table
    logging.info('Beginning transform of Premier League table data')
    tbl_cols = ['points',
                'goal_difference',
                'played',
                'win',
                'loss',
                'draw',
                'goals_scored',
                'goals_conceded']
    df_table = pd.merge(df_team_results,
                        df_teams[['team_id', 'team_name_long']],
                        how='left',
                        on='team_id')
    df_table =\
        df_table.groupby(['team_id', 'team_name_long'],
                         as_index=False)[tbl_cols].sum()
    df_table.sort_values(['points', 'goal_difference', 'goals_scored'],
                         ascending=False,
                         inplace=True)
    df_table[tbl_cols] = df_table[tbl_cols].applymap(pandas_integerstr_to_int)
    df_table.reset_index(drop=True, inplace=True)
    df_table.index.rename('table_position', inplace=True)
    logging.info('Completed transform of Premier League table data')

    # Set indexes (primary keys) of tables
    logging.info('Setting indexes (to be used as primary keys)')
    df_fixtures.set_index(fixtures_index, inplace=True)
    df_gameweeks.set_index(gameweek_index, inplace=True)
    df_teams.set_index(teams_index, inplace=True)
    df_positions.set_index(positions_index, inplace=True)
    df_players_sum.set_index(players_sum_index, inplace=True)
    df_players_prev_seasons.set_index(players_prev_seasons_index, inplace=True)
    df_players_past.set_index(players_past_index, inplace=True)
    df_players_future.set_index(players_future_index, inplace=True)
    df_players_full.set_index(players_full_index, inplace=True)
    df_team_results.set_index(team_results_index, inplace=True)

    # Verify unique indexes
    logging.info('Verifying unique indexes')
    check_unique_index(df_fixtures, 'fixtures',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_gameweeks, 'gameweeks',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_teams, 'teams',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_positions, 'positions',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_players_sum, 'players_summary',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_players_prev_seasons, 'players_prev_seasons',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_players_past, 'players_past',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_players_future, 'players_future',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_players_full, 'players_full',
                       raise_errors=RAISE_ERRORS)
    check_unique_index(df_team_results, 'team_results',
                       raise_errors=RAISE_ERRORS)

    # Verify not-null indexes
    logging.info('Verifying non-null indexes')
    check_not_null_index(df_fixtures, 'fixtures',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_gameweeks, 'gameweeks',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_teams, 'teams',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_positions, 'positions',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_players_sum, 'players_summary',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_players_prev_seasons, 'players_prev_seasons',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_players_past, 'players_past',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_players_future, 'players_future',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_players_full, 'players_full',
                         raise_errors=RAISE_ERRORS)
    check_not_null_index(df_team_results, 'team_results',
                         raise_errors=RAISE_ERRORS)


    # Pickle dataframes so as to retain column information (types etc.) and
    # indexes
    logging.info('Pickling final dataframes')
    pickle_data(df_fixtures, OUT_FIXTURES, DATA_LOC_OUT)
    pickle_data(df_gameweeks, OUT_GAMEWEEKS, DATA_LOC_OUT)
    pickle_data(df_teams, OUT_TEAMS, DATA_LOC_OUT)
    pickle_data(df_positions, OUT_POSITIONS, DATA_LOC_OUT)
    pickle_data(df_players_sum, OUT_PLAYERS_SUM, DATA_LOC_OUT)
    pickle_data(df_players_prev_seasons, OUT_PLAYERS_PREVIOUS_SEASONS, DATA_LOC_OUT)
    pickle_data(df_players_past, OUT_PLAYERS_PAST, DATA_LOC_OUT)
    pickle_data(df_players_future, OUT_PLAYERS_FUTURE, DATA_LOC_OUT)
    pickle_data(df_players_full, OUT_PLAYERS_FULL, DATA_LOC_OUT)
    pickle_data(df_team_results, OUT_TEAM_RESULTS, DATA_LOC_OUT)
    pickle_data(df_table, OUT_LEAGUE_TABLE, DATA_LOC_OUT)

    if not args.skip_s3_upload:
        dfiles = [f'{DATA_LOC_OUT}/{OUT_FIXTURES}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_GAMEWEEKS}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_TEAMS}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_POSITIONS}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_PLAYERS_SUM}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_PLAYERS_PREVIOUS_SEASONS}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_PLAYERS_PAST}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_PLAYERS_FUTURE}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_PLAYERS_FULL}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_TEAM_RESULTS}.pkl',
                  f'{DATA_LOC_OUT}/{OUT_LEAGUE_TABLE}.pkl'
                  ]

        s3 = AwsS3()
        s3.upload(dfiles, args.s3_bucket, args.s3_folder)

    logging.info('================Transform complete================')

    if not args.skip_s3_upload:
        lfiles = [args.log_file]
        logging.info(f'Uploading {args.log_file} to S3')
        s3_l = AwsS3()
        s3_l.upload(lfiles, args.s3_bucket, args.s3_log_output)

import logging
import argparse
import keyring
from datetime import datetime

import sqlalchemy

from fpltools.load import (load_pickle_data, BatchSQLUpdate, RecordTable)
from fpltools.load import (QUERY_RECORD, QUERY_PLAYERS_FULL,
                           QUERY_PLAYERS_PAST, QUERY_FIXTURES,
                           QUERY_GAMEWEEKS, QUERY_PLAYERS_FUTURE,
                           QUERY_PLAYERS_PREVIOUS_SEASONS,
                           QUERY_PLAYERS_STATUSES, QUERY_PLAYERS_SUMMARY,
                           QUERY_POSITIONS, QUERY_TABLE, QUERY_TEAM_RESULTS,
                           QUERY_TEAMS)
from fpltools.utils import get_datetime


# TODO: perform final transforms (mostly renaming) in transform part of code
# TODO: Explicit begin() and rollback() for query execution (may require large
#  rewriting of SQLLoad class

def _get_latest_gameweek(dbengine, table_name='gameweeks'):
    with dbengine.connect() as con:
        res = con.execute(f"""SELECT
                           MAX(CAST(nullif(gameweek_id, '') AS integer))
                           FROM {table_name} WHERE gameweek_finished""")
    return res.first()[0] + 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load into postresql db')

    parser.add_argument('host',
                        type=str,
                        help='database host')
    parser.add_argument('port',
                        type=str,
                        help='database port')
    parser.add_argument('db_name',
                        type=str,
                        help='name of database')
    parser.add_argument('db_user',
                        type=str,
                        help='username for database')
    parser.add_argument('db_keyring_name',
                        type=str,
                        help='name for service used when setting keyring '
                             'keyring credentials for this database')
    parser.add_argument('--data_input',
                        type=str,
                        default='data/',
                        help='path from which to load data')
    args = parser.parse_args()

    DATA_LOC = args.data_input
    DB_HOST = args.host
    DB_PORT = args.port
    DB_NAME = args.db_name
    DB_USER = args.db_user
    DB_KEYRING_NAME = args.db_keyring_name
    DB_PSWD = keyring.get_password(DB_KEYRING_NAME, DB_USER)

    logging.basicConfig(level=logging.INFO,
                        filename=f'logs/load_{get_datetime()}.log',
                        filemode='w',
                        format='%(levelname)s - %(asctime)s - %(message)s')

    load_date = datetime.utcnow().isoformat().replace('T', ' ')

    # Dataframes to be loaded are originally pickled from run_transform.py
    df_fixtures = load_pickle_data('transformed_fixtures.pkl',
                                   DATA_LOC)
    df_gameweeks = load_pickle_data('transformed_gameweeks.pkl',
                                    DATA_LOC)
    df_league_table = load_pickle_data('transformed_league_table.pkl',
                                       DATA_LOC)
    df_players_future = load_pickle_data('transformed_players_future.pkl',
                                         DATA_LOC)
    df_players_past = load_pickle_data('transformed_players_past.pkl',
                                       DATA_LOC)
    df_players_full = load_pickle_data('transformed_players_full.pkl',
                                       DATA_LOC)
    df_players_previous_seasons =\
        load_pickle_data('transformed_players_previous_seasons.pkl', DATA_LOC)
    df_players_summary = load_pickle_data('transformed_players_summary.pkl',
                                          DATA_LOC)
    df_positions = load_pickle_data('transformed_positions.pkl',
                                    DATA_LOC)
    df_team_results = load_pickle_data('transformed_team_results.pkl',
                                       DATA_LOC)
    df_teams = load_pickle_data('transformed_teams.pkl',
                                DATA_LOC)

    engine = sqlalchemy.create_engine(
        f'postgresql://{DB_USER}:{DB_PSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Players' previous season data
    bu_players_previous_seasons =\
        BatchSQLUpdate(df_players_previous_seasons,
                       engine,
                       QUERY_PLAYERS_PREVIOUS_SEASONS,
                       'players_previous_seasons')
    bu_players_previous_seasons.batch_overwrite()

    # Positional data
    bu_positions = BatchSQLUpdate(df_positions,
                                  engine,
                                  QUERY_POSITIONS,
                                  'positions')
    bu_positions.batch_overwrite()

    # Team data
    bu_teams = BatchSQLUpdate(df_teams,
                              engine,
                              QUERY_TEAMS,
                              'teams')
    bu_teams.batch_overwrite()

    # League table data
    bu_league_table = BatchSQLUpdate(df_league_table,
                                     engine,
                                     QUERY_TABLE,
                                     'league_table')
    bu_league_table.batch_overwrite()

    # Player summary data
    bu_players_summary = BatchSQLUpdate(df_players_summary,
                                        engine,
                                        QUERY_PLAYERS_SUMMARY,
                                        'players_summary')
    bu_players_summary.batch_overwrite()

    # Gameweeks data
    bu_gameweeks = BatchSQLUpdate(df_gameweeks,
                                  engine,
                                  QUERY_GAMEWEEKS,
                                  'gameweeks')
    bu_gameweeks.batch_overwrite()

    # Fixtures data
    bu_fixtures = BatchSQLUpdate(df_fixtures,
                                 engine,
                                 QUERY_FIXTURES,
                                 'fixtures')
    bu_fixtures.batch_overwrite()

    # Upcoming fixtures player data
    bu_players_future = BatchSQLUpdate(df_players_future,
                                       engine,
                                       QUERY_PLAYERS_FUTURE,
                                       'players_future')
    bu_players_future.batch_overwrite()

    # Previous fixtures (current season) player data
    bu_players_past = BatchSQLUpdate(df_players_past,
                                     engine,
                                     QUERY_PLAYERS_PAST,
                                     'players_past')
    bu_players_past.batch_overwrite()

    # Full player data
    bu_players_full = BatchSQLUpdate(df_players_full,
                                     engine,
                                     QUERY_PLAYERS_FULL,
                                     'players_full')
    bu_players_full.batch_overwrite()

    # Team results data
    bu_team_results = BatchSQLUpdate(df_team_results,
                                     engine,
                                     QUERY_TEAM_RESULTS,
                                     'team_results')
    bu_team_results.batch_overwrite()

    # Player statuses (i.e. appending statuses to previous gameweeks)
    df_players_statuses = df_players_summary.copy().reset_index()
    gw_now = _get_latest_gameweek(engine)
    df_players_statuses['gameweek_now'] = str(gw_now)
    df_players_statuses['load_datetime'] = load_date
    bu_players_statuses = BatchSQLUpdate(df_players_statuses,
                                         engine,
                                         QUERY_PLAYERS_STATUSES,
                                         'players_statuses')
    bu_players_statuses.batch_append()

    # Record of batch update
    bu_record = RecordTable(load_date,
                            gw_now,
                            DB_USER,
                            engine,
                            QUERY_RECORD,
                            'record')
    bu_record.batch_append()

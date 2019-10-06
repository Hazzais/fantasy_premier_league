import os
import pickle
import logging
import argparse
import keyring
from datetime import datetime

import sqlalchemy

from fpltools.utils import get_datetime
from fpltools.queries import (QUERY_RECORD, QUERY_PLAYERS_FULL,
                              QUERY_PLAYERS_PAST, QUERY_FIXTURES,
                              QUERY_GAMEWEEKS, QUERY_PLAYERS_FUTURE,
                              QUERY_PLAYERS_PREVIOUS_SEASONS,
                              QUERY_PLAYERS_STATUSES, QUERY_PLAYERS_SUMMARY,
                              QUERY_POSITIONS, QUERY_TABLE, QUERY_TEAM_RESULTS,
                              QUERY_TEAMS)


def load_pickle_data(data_name, data_loc):
    """Load in transformed and pickled dataframes"""
    logging.info(f'Reading in pickled {data_name} from {data_loc}')
    try:
        with open(os.path.join(data_loc, data_name), 'rb') as f:
            loaded = pickle.load(f)
    except FileNotFoundError as e:
        logging.exception('Could not find file')
    else:
        logging.info(f'Successfully loaded {data_name}')
        return loaded


def table_get_columns(table_name, engine):
    with engine.connect() as con:
        return con.execute(f"""SELECT * FROM {table_name} LIMIT 0""").keys()


def _get_latest_gameweek(table_name='gameweeks'):
    with engine.connect() as con:
        res = con.execute(f"""SELECT
                           MAX(CAST(nullif(gameweek_id, '') AS integer))
                           FROM {table_name} WHERE gameweek_finished""")
    return res.first()[0] + 1


class BatchSQLUpdate:

    def __init__(self, data, engine, create_table_query, table_name):
        self._data = data
        self._engine = engine
        self.query = create_table_query.format(table_name)
        self._table_name = table_name
        logging.info(f'Beginning batch update for table {self._table_name}')

    def _table_create(self):
        with self._engine.connect() as con:
            con.execute(self.query)

    def _batch_drop_table(self):
        logging.info(f'Dropping table {self._table_name} if it exists')
        query_drop = f"""DROP TABLE IF EXISTS {self._table_name};"""
        with self._engine.connect() as con:
            con.execute(query_drop)

    def _batch_load(self, columns):
        logging.info(f'Loading data into {self._table_name}')
        df_load = self._data.reset_index()[columns]
        df_load.to_sql(self._table_name, self._engine, if_exists='append',
                       index=False)

    def batch_overwrite(self):
        logging.info(f'Overwriting {self._table_name}')
        self._batch_drop_table()
        self._table_create()
        columns = table_get_columns(self._table_name, self._engine)
        self._batch_load(columns)

    def batch_append(self):
        logging.info(f'Appending to {self._table_name}')
        if self._table_name not in engine.table_names():
            self._table_create()
        columns = table_get_columns(self._table_name, self._engine)
        self._batch_load(columns)


class RecordTable(BatchSQLUpdate):

    def __init__(self, load_datetime, gameweek_now, user, engine,
                 create_table_query, table_name):
        super().__init__(None, engine, create_table_query, table_name)
        self._load_datetime = load_datetime
        self._gameweek_now = gameweek_now
        self._user = user
        self._insert_vals = self._create_update_vals()

    def _create_update_vals(self):
        return {'load_datetime': self._load_datetime,
                'gameweek_now': self._gameweek_now,
                'username': self._user}

    def _batch_load(self, columns):
        logging.info(f'Loading data into {self._table_name}')
        with engine.connect() as con:
            con.execute("INSERT INTO record"
                        "(load_datetime, gameweek_now, username)"
                        "VALUES"
                        "(%(load_datetime)s, %(gameweek_now)s, %(username)s)",
                        self._create_update_vals())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load into postresql db')
    parser.add_argument('data_input',
                        type=str,
                        help='path from which to load data')
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
    args = parser.parse_args()

    # DATA_LOC = args.data_input
    # DB_HOST = args.host
    # DB_PORT = args.port
    # DB_NAME = args.db_name
    # DB_USER = args.db_user
    # DB_KEYRING_NAME = args.db_keyring_name
    DATA_LOC = 'data/'
    DB_HOST = 'localhost'
    DB_PORT = 5432
    DB_NAME = 'fpl_data'
    DB_USER = 'postgres'
    DB_KEYRING_NAME = 'postgres_db_fpl_data'

    DB_PSWD = keyring.get_password(DB_KEYRING_NAME, DB_USER)

    logging.basicConfig(level=logging.INFO,
                        filename=f'logs/load_{get_datetime()}.log',
                        filemode='w',
                        format='%(levelname)s - %(asctime)s - %(message)s')

    load_date = datetime.utcnow().isoformat().replace('T', ' ')

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

    # TODO: perform in transform part of code
    # league_table -> name index
    df_league_table.index.rename('table_position', inplace=True)
    # rename gameweek -> gameweek_id EVERYWHERE
    df_gameweeks.index.rename('gameweek_id', inplace=True)
    df_team_results.rename(columns={'gameweek': 'gameweek_id'}, inplace=True)
    df_players_full.rename(columns={'gameweek': 'gameweek_id'}, inplace=True)
    df_players_full.index.rename(['player_id', 'gameweek_id', 'fixture_id'],
                                 inplace=True)
    df_players_future.rename(columns={'gameweek': 'gameweek_id'}, inplace=True)
    df_players_past.rename(columns={'gameweek': 'gameweek_id'}, inplace=True)
    df_fixtures.rename(columns={'gameweek': 'gameweek_id'}, inplace=True)

    bu_players_previous_seasons =\
        BatchSQLUpdate(df_players_previous_seasons,
                       engine,
                       QUERY_PLAYERS_PREVIOUS_SEASONS,
                       'players_previous_seasons')
    bu_players_previous_seasons.batch_overwrite()

    bu_positions = BatchSQLUpdate(df_positions,
                                  engine,
                                  QUERY_POSITIONS,
                                  'positions')
    bu_positions.batch_overwrite()

    bu_teams = BatchSQLUpdate(df_teams,
                              engine,
                              QUERY_TEAMS,
                              'teams')
    bu_teams.batch_overwrite()

    bu_league_table = BatchSQLUpdate(df_league_table,
                                     engine,
                                     QUERY_TABLE,
                                     'league_table')
    bu_league_table.batch_overwrite()

    bu_players_summary = BatchSQLUpdate(df_players_summary,
                                        engine,
                                        QUERY_PLAYERS_SUMMARY,
                                        'players_summary')
    bu_players_summary.batch_overwrite()

    bu_gameweeks = BatchSQLUpdate(df_gameweeks,
                                  engine,
                                  QUERY_GAMEWEEKS,
                                  'gameweeks')
    bu_gameweeks.batch_overwrite()
    bu_fixtures = BatchSQLUpdate(df_fixtures,
                                 engine,
                                 QUERY_FIXTURES,
                                 'fixtures')
    bu_fixtures.batch_overwrite()

    bu_players_future = BatchSQLUpdate(df_players_future,
                                       engine,
                                       QUERY_PLAYERS_FUTURE,
                                       'players_future')
    bu_players_future.batch_overwrite()

    bu_players_past = BatchSQLUpdate(df_players_past,
                                     engine,
                                     QUERY_PLAYERS_PAST,
                                     'players_past')
    bu_players_past.batch_overwrite()

    bu_players_full = BatchSQLUpdate(df_players_full,
                                     engine,
                                     QUERY_PLAYERS_FULL,
                                     'players_full')
    bu_players_full.batch_overwrite()

    bu_team_results = BatchSQLUpdate(df_team_results,
                                     engine,
                                     QUERY_TEAM_RESULTS,
                                     'team_results')
    bu_team_results.batch_overwrite()

    df_players_statuses = df_players_summary.copy().reset_index()

    gw_now = _get_latest_gameweek()

    df_players_statuses['gameweek_now'] = str(gw_now)
    df_players_statuses['load_datetime'] = load_date

    bu_players_statuses = BatchSQLUpdate(df_players_statuses,
                                         engine,
                                         QUERY_PLAYERS_STATUSES,
                                         'players_statuses')
    bu_players_statuses.batch_append()

    bu_record = RecordTable(load_date,
                            gw_now,
                            DB_USER,
                            engine,
                            QUERY_RECORD,
                            'record')
    bu_record.batch_append()

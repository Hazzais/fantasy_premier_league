import os
import pickle
import logging
import argparse
import keyring

import sqlalchemy
import pandas as pd
import numpy as np

from fpltools.utils import get_datetime


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

    all_tables = engine.table_names()

# 1) If not exists - create table with SQL code NOT to_sql
# 2) If entries already and overwrite - delete current data and insert as new
# rows. Retain structure and table relationships.
# 3) May need to stage above
# 4) Explain why
# 3) If append - append


# fixtures - overwrite - add gameweek FK
create_query = """CREATE TABLE {} (
    fixture_id VARCHAR(3) PRIMARY KEY,
    fixture_id_long VARCHAR(7) UNIQUE NOT NULL,
    gameweek VARCHAR(2) NOT NULL,
    fixture_kickoff_datetime TIMESTAMP NOT NULL,
    fixture_started BOOL NOT NULL,
    fixture_finished BOOL NOT NULL,
    fixture_finished_provisional BOOL NOT NULL,
    fixture_minutes INT CHECK(fixture_minutes<=90),
    home_team_id VARCHAR(2) NOT NULL,
    away_team_id VARCHAR(2) NOT NULL,
    home_team_score INT,
    away_team_score INT,
    home_team_fixture_difficulty INT CHECK(home_team_fixture_difficulty<=4),
    away_team_fixture_difficulty INT CHECK(home_team_fixture_difficulty<=4)
    )
    """
print(create_query.format('fixtures'))

class BatchSQLUpdate:

    def __init__(self, data, engine, create_table_query, table_name):
        self._data = data
        self._engine = engine
        self.query = create_table_query.format(table_name)
        self._table_name = table_name
        logging.info(f'Beginning batch update for table {self._table_name}')

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
        with self._engine.connect() as con:
            con.execute(self.query)
        columns = table_get_columns(self._table_name, self._engine)
        self._batch_load(columns)

    def batch_append(self):
        logging.info(f'Appending to {self._table_name}')
        with self._engine.connect() as con:
            con.execute(self.query)
        columns = table_get_columns(self._table_name, self._engine)
        self._batch_load(columns)

    def add_foreign_key_constraint(self):
        pass


bu_fixtures = BatchSQLUpdate(df_fixtures,
                             engine,
                             create_query,
                             'fixtures')
bu_fixtures.batch_overwrite()





#
# with engine.connect() as con:
#     con.execute(create_query)
#     columns = con.execute("""SELECT * FROM fixtures LIMIT 0""").keys()
#
# df_load = df_fixtures.reset_index()[columns]
# df_load.to_sql('fixtures', engine, if_exists='append', index=False)
#
# results = pd.read_sql("""SELECT * from fixtures""", con=engine)

# gameweeks - overwrite


# league_table - overwrite


# players_future - overwrite


# players_past - overwrite


# players_previous_seasons - overwrite


# players_summary - overwrite


# players_summary_cont - append


# positions - none


# team_results - overwrite


# teams - overwrite
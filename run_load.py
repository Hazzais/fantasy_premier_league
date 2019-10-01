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

    all_tables = engine.table_names()

# 1) If not exists - create table with SQL code NOT to_sql
# 2) If entries already and overwrite - delete current data and insert as new
# rows. Retain structure and table relationships.
# 3) May need to stage above
# 4) Explain why
# 3) If append - append


# fixtures - overwrite - add gameweek FK
QUERY_FIXTURES = """CREATE TABLE {} (
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
    );
    """

QUERY_GAMEWEEKS = """CREATE TABLE {} (
    gameweek_id VARCHAR(2) PRIMARY KEY,
    gameweek_name VARCHAR(11) UNIQUE NOT NULL,
    gameweek_deadline_time TIMESTAMP NOT NULL,
    gameweek_previous BOOL NOT NULL,
    gameweek_current BOOL NOT NULL,
    gameweek_next BOOL NOT NULL,
    gameweek_finished BOOT NOT NULL,
    gameweek_data_checked BOOL NOT NULL,
    average_entry_score INT,
    highest_scoring_entry VARCHAR(8),
    highest_scoring_entry_score INT,
    player_id_most_selected VARCHAR(3),
    player_id_most_transferred_in VARCHAR(3),
    player_id_highest_score VARCHAR(3),
    player_id_most_captained VARCHAR(3),
    player_id_most_vice_captained VARCHAR(3),
    transfers_made INT
    );
"""

# ref: teams(team_id)
QUERY_TABLE = """CREATE TABLE {} (
    table_position INT PRIMARY KEY CHECK(table_position<=20),
    team_id VARCHAR(2) NOT NULL UNIQUE,
    team_name_long VARCHAR(25) NOT NULL UNIQUE,
    points INT NOT NULL,
    goal_difference INT NOT NULL,
    played INT NOT NULL,
    win INT NOT NULL,
    draw INT NOT NULL,
    loss draw INT NOT NULL,
    goals_scored INT NOT NULL,
    goals_conceded INT NOT NULL
    );
"""

QUERY_PLAYERS_FUTURE = """CREATE TABLE {} (
    player_id VARCHAR(3),
    fixture_id VARCHAR(3),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek VARCHAR(2) NOT NULL,
    home_team_id VARCHAR(2) NOT NULL,
    away_team_id VARCHAR(2) NOT NULL,
    home_team_score INT,
    away_team_score INT,
    finished BOOL NOT NULL,
    minutes INT CHECK(minutes<=90),
    provisional_start_time BOOL,
    fixture_home BOOL NOT NULL,
    difficulty INT CHECK(difficulty<=4),
    kickoff_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (player_id, fixture_id)
    );
"""


QUERY_PLAYERS_PAST = """CREATE TABLE {} (
    player_id VARCHAR(2),
    fixture_id VARCHAR(3),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek VARCHAR(2) NOT NULL,
    total_points INT NOT NULL,
    fixture_home BOOL NOT NULL,
    home_team_id VARCHAR(2) NOT NULL,
    away_team_id VARCHAR(2) NOT NULL,
    home_team_score INT,
    away_team_score INT,
    minutes INT NOT NULL,
    goals_scored INT NOT NULL,
    assists INT NOT NULL,
    clean_sheets INT NOT NULL,
    goals_conceded INT NOT NULL,
    own_goals INT NOT NULL,
    penalties_saved INT NOT NULL,
    penalties_missed INT NOT NULL,
    yellow_cards INT NOT NULL,
    red_cards INT NOT NULL,
    saves INT NOT NULL,
    bonus INT NOT NULL,
    bps INT NOT NULL,
    influence FLOAT(8) NOT NULL,
    creativity FLOAT(8) NOT NULL,
    threat FLOAT(8) NOT NULL,
    ict_index FLOAT(8) NOT NULL,
    value INT NOT NULL,
    transfers_balance INT NOT NULL,
    selected INT NOT NULL,
    transfers_in INT NOT NULL,
    transfers_out INT NOT NULL,
    kickoff_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (player_id, fixture_id)
);
"""

QUERY_PLAYERS_FULL = """CREATE TABLE {} (
    player_id VARCHAR(2),
    fixture_id VARCHAR(3),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek VARCHAR(2) NOT NULL,
    team_id VARCHAR(2) NOT NULL,
    position_id VARCHAR(1) NOT NULL,
    total_points INT NOT NULL,
    fixture_home BOOL NOT NULL,
    home_team_id VARCHAR(2) NOT NULL,
    away_team_id VARCHAR(2) NOT NULL,
    home_team_score INT,
    away_team_score INT,
    minutes INT NOT NULL,
    goals_scored INT NOT NULL,
    assists INT NOT NULL,
    clean_sheets INT NOT NULL,
    goals_conceded INT NOT NULL,
    own_goals INT NOT NULL,
    penalties_saved INT NOT NULL,
    penalties_missed INT NOT NULL,
    yellow_cards INT NOT NULL,
    red_cards INT NOT NULL,
    saves INT NOT NULL,
    bonus INT NOT NULL,
    bps INT NOT NULL,
    influence FLOAT(8) NOT NULL,
    creativity FLOAT(8) NOT NULL,
    threat FLOAT(8) NOT NULL,
    ict_index FLOAT(8) NOT NULL,
    value INT NOT NULL,
    transfers_balance INT NOT NULL,
    selected INT NOT NULL,
    transfers_in INT NOT NULL,
    transfers_out INT NOT NULL,
    kickoff_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (player_id, gameweek, fixture_id)
);
"""

QUERY_PLAYERS_PREVIOUS_SEASONS = """CREATE TABLE {} (
    player_id_long VARCHAR(6),
    season_name VARCHAR(7),
    start_cost INT NOT NULL,
    end_cost INT NOT NULL,
    total_points INT NOT NULL,
    minutes INT NOT NULL,
    goals_scored INT NOT NULL,
    assists INT NOT NULL,
    clean_sheets INT NOT NULL,
    goals_conceded INT NOT NULL,
    own_goals INT NOT NULL,
    penalties_saved INT NOT NULL,
    penalties_missed INT NOT NULL,
    yellow_cards INT NOT NULL,
    red_cards INT NOT NULL,
    saves INT NOT NULL,
    bonus INT NOT NULL,
    bps INT NOT NULL,
    influence FLOAT(8) NOT NULL,
    creativity FLOAT(8) NOT NULL,
    threat FLOAT(8) NOT NULL,
    ict_index FLOAT(8) NOT NULL,
    PRIMARY KEY(player_id_long, season_name)
);
"""

QUERY_PLAYERS_SUMMARY = """CREATE TABLE {} (
    player_id VARCHAR(3) PRIMARY KEY,
    player_id_long VARCHAR(6) NOT NULL UNIQUE,
    chance_of_playing_next_round INT,
    chance_of_playing_this_round INT,
    cost_change_event INT NOT NULL,
    cost_change_event_fall INT NOT NULL,
    cost_change_start INT NOT NULL,
    cost_change_start_fall INT NOT NULL,
    dreamteam_count INT NOT NULL,
    position_id VARCHAR(1) NOT NULL,
    ep_next FLOAT(8) NOT NULL,
    ep_this FLOAT(8) NOT NULL,
    gameweek_points INT NOT NULL,
    first_name VARCHAR(25) NOT NULL,
    form FLOAT(8) NOT NULL,
    in_dreamteam BOOL NOT NULL,
    news VARCHAR(150),
    news_added_datetime TIMESTAMP,
    now_cost INT NOT NULL,
    photo VARCHAR(11) NOT NULL,
    points_per_game FLOAT(8),
    second_name VARCHAR(25) NOT NULL,
    selected_by_percent FLOAT(8) NOT NULL,
    special BOOL NOT NULL,
    status VARCHAR(1) NOT NULL,
    team_id VARCHAR(2) NOT NULL,
    team_id_long VARCHAR(3) NOT NULL,
    total_points INT NOT NULL,
    transfers_in INT NOT NULL,
    transfers_out INT NOT NULL,
    transfers_in_event INT NOT NULL,
    transfers_out_event INT NOT NULL,
    value_form FLOAT(8) NOT NULL,
    value_season FLOAT(8) NOT NULL,
    minutes INT NOT NULL,
    goals_scored INT NOT NULL,
    assists INT NOT NULL,
    clean_sheets INT NOT NULL,
    goals_conceded INT NOT NULL,
    own_goals INT NOT NULL,
    penalties_saved INT NOT NULL,
    penalties_missed INT NOT NULL,
    yellow_cards INT NOT NULL,
    red_cards INT NOT NULL,
    saves INT NOT NULL,
    bonus INT NOT NULL,
    bps INT NOT NULL,
    influence FLOAT(8) NOT NULL,
    creativity FLOAT(8) NOT NULL,
    threat FLOAT(8) NOT NULL,
    ict_index FLOAT(8) NOT NULL,
);
"""


QUERY_POSITIONS = """CREATE TABLE {} (
    position_id VARCHAR(1) PRIMARY KEY,
    position_name VARCHAR(3) UNIQUE NOT NULL,
    position_name_long VARCHAR(10) UNIQUE NOT NULL,
    squad_select INT NOT NULL,
    squad_min_play INT NOT NULL,
    squad_max_play INT NOT NULL
    ;
"""

QUERY_TEAM_RESULTS = """CREATE TABLE {}(
    team_id VARCHAR(2) NOT NULL,
    fixture_id VARCHAR(3) NOT NULL,
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek VARCHAR(2) NOT NULL,
    opponent_team_id VARCHAR(2) NOT NULL,
    goals_conceded INT NOT NULL,
    goals_scored INT NOT NULL,
    fixture_kickoff_datetime TIMESTAMP NOT NULL,
    played BOOL NOT NULL,
    fixture_home BOOL NOT NULL,
    win INT NOT NULL,
    draw INT NOT NULL,
    loss INT NOT NULL,
    points INT NOT NULL,
    goal_difference INT NOT NULL,
    PRIMARY KEY(team_id, fixture_id)
);
"""

QUERY_TEAMS = """CREATE TABLE {{ (
    team_id VARCHAR(2) PRIMARY KEY,
    team_id_long VARCHAR(3) NOT NULL UNIQUE,
    team_name_long VARCHAR(25) NOT NULL UNIQUE,
    team_name VARCHAR(3) NOT NULL UNIQUE,
    team_strength INT NOT NULL,
    team_strength_overall_home INT NOT NULL,
    team_strength_overall_away INT NOT NULL,
    team_strength_attack_home INT NOT NULL,
    team_strength_attack_away INT NOT NULL,
    team_strength_defence_home INT NOT NULL,
    team_strength_defence_away INT NOT NULL
);
"""


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
                             QUERY_FIXTURES,
                             'fixtures')
bu_fixtures.batch_overwrite()

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
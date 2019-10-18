import os
import pickle
import abc
import logging

import sqlalchemy.exc as sqlaexc


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


def table_get_columns(table_name, dbengine):
    with dbengine.connect() as con:
        return con.execute(f"""SELECT * FROM {table_name} LIMIT 0""").keys()


class SQLLoad(abc.ABC):
    def __init__(self, data, dbengine, create_table_query, table_name):
        self._data = data
        self._dbengine = dbengine
        self.query = create_table_query.format(table_name)
        self._table_name = table_name

    def _table_create(self):
        try:
            with self._dbengine.connect() as con:
                con.execute(self.query)
            logging.info(f'Successfully created {self._table_name}')
        except sqlaexc.ProgrammingError as e:
            logging.exception(f'Exception in _table_create for '
                              f'{self._table_name}')

    def _batch_drop_table(self):
        logging.info(f'Dropping table {self._table_name} if it exists')
        query_drop = f"""DROP TABLE IF EXISTS {self._table_name} CASCADE;"""
        try:
            with self._dbengine.connect() as con:
                con.execute(query_drop)
            logging.info(f'Successfully dropped {self._table_name}')
        except sqlaexc.ProgrammingError as e:
            logging.exception(f'Exception in _batch_drop_table for '
                              f'{self._table_name}')

    @abc.abstractmethod
    def _batch_load(self, columns):
        pass

    def batch_overwrite(self):
        logging.info(f'Overwriting {self._table_name}')
        self._batch_drop_table()
        self._table_create()
        columns = table_get_columns(self._table_name, self._dbengine)
        self._batch_load(columns)

    def batch_append(self):
        logging.info(f'Appending to {self._table_name}')
        if self._table_name not in self._dbengine.table_names():
            self._table_create()
        columns = table_get_columns(self._table_name, self._dbengine)
        self._batch_load(columns)


class BatchSQLUpdate(SQLLoad):

    def __init__(self, data, dbengine, create_table_query, table_name):
        super().__init__(data, dbengine, create_table_query, table_name)
        logging.info(f'Beginning batch update for table {self._table_name}')

    def _batch_load(self, columns):
        logging.info(f'Loading data into {self._table_name}')
        df_load = self._data.reset_index()[columns]
        df_load.to_sql(self._table_name, self._dbengine, if_exists='append',
                       index=False)


class RecordTable(SQLLoad):

    def __init__(self, load_datetime, gameweek_now, user, dbengine,
                 create_table_query, table_name):
        super().__init__(None, dbengine, create_table_query, table_name)
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
        with self._dbengine.connect() as con:
            con.execute("INSERT INTO record"
                        "(load_datetime, gameweek_now, username)"
                        "VALUES"
                        "(%(load_datetime)s, %(gameweek_now)s, %(username)s)",
                        self._create_update_vals())


# Table create order: 0
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

# Table create order: 0
QUERY_POSITIONS = """CREATE TABLE {} (
    position_id VARCHAR(1) PRIMARY KEY,
    position_name VARCHAR(3) UNIQUE NOT NULL,
    position_name_long VARCHAR(10) UNIQUE NOT NULL,
    squad_select INT NOT NULL,
    squad_min_play INT NOT NULL,
    squad_max_play INT NOT NULL
    );
"""

# Table create order: 0
QUERY_TEAMS = """CREATE TABLE {} (
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

# Table create order: 1
QUERY_TABLE = """CREATE TABLE {} (
    table_position INT PRIMARY KEY CHECK(table_position<=20),
    team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    team_name_long VARCHAR(25) NOT NULL UNIQUE,
    points INT NOT NULL,
    goal_difference INT NOT NULL,
    played INT NOT NULL,
    win INT NOT NULL,
    draw INT NOT NULL,
    loss INT NOT NULL,
    goals_scored INT NOT NULL,
    goals_conceded INT NOT NULL
    );
"""

# Table create order: 1
QUERY_PLAYERS_SUMMARY = """CREATE TABLE {} (
    player_id VARCHAR(3) PRIMARY KEY,
    player_id_long VARCHAR(6) NOT NULL UNIQUE,
    first_name VARCHAR(40) NOT NULL,
    second_name VARCHAR(40) NOT NULL,
    position_id VARCHAR(1) NOT NULL REFERENCES positions(position_id),
    team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    team_id_long VARCHAR(3) NOT NULL,
    now_cost INT NOT NULL,
    selected_by_percent FLOAT(8) NOT NULL,
    form FLOAT(8) NOT NULL,
    chance_of_playing_next_round INT,
    chance_of_playing_this_round INT,
    cost_change_event INT NOT NULL,
    cost_change_event_fall INT NOT NULL,
    cost_change_start INT NOT NULL,
    cost_change_start_fall INT NOT NULL,
    news VARCHAR(150),
    news_added_datetime TIMESTAMP,
    ep_next FLOAT(8),
    ep_this FLOAT(8),
    in_dreamteam BOOL NOT NULL,
    dreamteam_count INT NOT NULL,
    gameweek_points INT NOT NULL,
    photo VARCHAR(11) NOT NULL,
    points_per_game FLOAT(8),
    special BOOL NOT NULL,
    status VARCHAR(1) NOT NULL,
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
    ict_index FLOAT(8) NOT NULL
);
"""

# Table create order: 3
QUERY_GAMEWEEKS = """CREATE TABLE {} (
    gameweek_id VARCHAR(2) PRIMARY KEY,
    gameweek_name VARCHAR(11) UNIQUE NOT NULL,
    gameweek_deadline_time TIMESTAMP NOT NULL,
    gameweek_previous BOOL NOT NULL,
    gameweek_current BOOL NOT NULL,
    gameweek_next BOOL NOT NULL,
    gameweek_finished BOOL NOT NULL,
    gameweek_data_checked BOOL NOT NULL,
    average_entry_score INT,
    highest_scoring_entry VARCHAR(8),
    highest_scoring_entry_score INT,
    player_id_most_selected VARCHAR(3) REFERENCES players_summary(player_id),
    player_id_most_transferred_in VARCHAR(3)
        REFERENCES players_summary(player_id),
    player_id_highest_score VARCHAR(3)
        REFERENCES players_summary(player_id),
    player_id_most_captained VARCHAR(3) REFERENCES players_summary(player_id),
    player_id_most_vice_captained VARCHAR(3)
        REFERENCES players_summary(player_id),
    transfers_made INT
    );
"""

# Table create order: 4
QUERY_FIXTURES = """CREATE TABLE {} (
    fixture_id VARCHAR(3) PRIMARY KEY,
    fixture_id_long VARCHAR(7) UNIQUE NOT NULL,
    gameweek_id VARCHAR(2) REFERENCES gameweeks(gameweek_id),
    fixture_kickoff_datetime TIMESTAMP NOT NULL,
    fixture_started BOOL NOT NULL,
    fixture_finished BOOL NOT NULL,
    fixture_finished_provisional BOOL NOT NULL,
    fixture_minutes INT CHECK(fixture_minutes<=90),
    home_team_id VARCHAR(2) REFERENCES teams(team_id),
    away_team_id VARCHAR(2)REFERENCES teams(team_id),
    home_team_score INT,
    away_team_score INT,
    home_team_fixture_difficulty INT CHECK(home_team_fixture_difficulty<=4),
    away_team_fixture_difficulty INT CHECK(home_team_fixture_difficulty<=4)
    );
    """

# Table create order: 5
QUERY_PLAYERS_FUTURE = """CREATE TABLE {} (
    player_id VARCHAR(3) REFERENCES players_summary(player_id),
    fixture_id VARCHAR(3) REFERENCES fixtures(fixture_id),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek_id VARCHAR(2) REFERENCES gameweeks(gameweek_id),
    home_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    away_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    home_team_score INT,
    away_team_score INT,
    finished BOOL NOT NULL,
    minutes INT CHECK(minutes<=90),
    provisional_start_time BOOL,
    fixture_home BOOL NOT NULL,
    difficulty INT,
    kickoff_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (player_id, fixture_id)
    );
"""

# Table create order: 5
QUERY_PLAYERS_PAST = """CREATE TABLE {} (
    player_id VARCHAR(3) REFERENCES players_summary(player_id),
    fixture_id VARCHAR(3) REFERENCES fixtures(fixture_id),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek_id VARCHAR(2) NOT NULL REFERENCES gameweeks(gameweek_id),
    total_points INT NOT NULL,
    fixture_home BOOL NOT NULL,
    home_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    away_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
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

# Table create order: 5
QUERY_PLAYERS_FULL = """CREATE TABLE {} (
    player_id VARCHAR(3) REFERENCES players_summary(player_id),
    fixture_id VARCHAR(3) REFERENCES fixtures(fixture_id),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek_id VARCHAR(2) NOT NULL REFERENCES gameweeks(gameweek_id),
    team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    position_id VARCHAR(1) NOT NULL REFERENCES positions(position_id),
    total_points INT,
    fixture_home BOOL NOT NULL,
    home_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    away_team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    home_team_score INT,
    away_team_score INT,
    minutes INT,
    goals_scored INT,
    assists INT,
    clean_sheets INT,
    goals_conceded INT,
    own_goals INT,
    penalties_saved INT,
    penalties_missed INT,
    yellow_cards INT,
    red_cards INT,
    saves INT,
    bonus INT,
    bps INT,
    influence FLOAT(8),
    creativity FLOAT(8),
    threat FLOAT(8),
    ict_index FLOAT(8),
    value INT,
    transfers_balance INT,
    selected INT,
    transfers_in INT,
    transfers_out INT,
    kickoff_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (player_id, gameweek_id, fixture_id)
);
"""

# Table create order: 5
QUERY_TEAM_RESULTS = """CREATE TABLE {}(
    team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    fixture_id VARCHAR(3) NOT NULL REFERENCES fixtures(fixture_id),
    fixture_id_long VARCHAR(8) NOT NULL,
    gameweek_id VARCHAR(2) NOT NULL REFERENCES gameweeks(gameweek_id),
    opponent_team_id VARCHAR(2) NOT NULL,
    goals_conceded INT,
    goals_scored INT,
    fixture_kickoff_datetime TIMESTAMP NOT NULL,
    played BOOL NOT NULL,
    fixture_home BOOL NOT NULL,
    win INT,
    draw INT,
    loss INT,
    points INT,
    goal_difference INT,
    PRIMARY KEY(team_id, fixture_id)
);
"""

# Table create order: 1
QUERY_PLAYERS_STATUSES = """CREATE TABLE {} (
    load_datetime TIMESTAMP,
    gameweek_now VARCHAR(2) REFERENCES gameweeks(gameweek_id),
    player_id VARCHAR(3) NOT NULL REFERENCES players_summary(player_id),
    player_id_long VARCHAR(6) NOT NULL,
    first_name VARCHAR(40) NOT NULL,
    second_name VARCHAR(40) NOT NULL,
    position_id VARCHAR(1) NOT NULL REFERENCES positions(position_id),
    team_id VARCHAR(2) NOT NULL REFERENCES teams(team_id),
    team_id_long VARCHAR(3) NOT NULL,
    now_cost INT NOT NULL,
    selected_by_percent FLOAT(8) NOT NULL,
    form FLOAT(8) NOT NULL,
    chance_of_playing_next_round INT,
    chance_of_playing_this_round INT,
    cost_change_event INT NOT NULL,
    cost_change_event_fall INT NOT NULL,
    cost_change_start INT NOT NULL,
    cost_change_start_fall INT NOT NULL,
    news VARCHAR(150),
    news_added_datetime TIMESTAMP,
    ep_next FLOAT(8) NOT NULL,
    ep_this FLOAT(8) NOT NULL,
    in_dreamteam BOOL NOT NULL,
    dreamteam_count INT NOT NULL,
    gameweek_points INT NOT NULL,
    photo VARCHAR(11) NOT NULL,
    points_per_game FLOAT(8),
    special BOOL NOT NULL,
    status VARCHAR(1) NOT NULL,
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
    PRIMARY KEY(load_datetime, gameweek_now, player_id)
);
"""

# Table create order: 0 (though it requires max gameweek to be calculated
# before for insert value)
# gameweek need not reference gameweek primary key as at the end of the season
# this table may still be added to (e.g. gameweek above 'maximum')
QUERY_RECORD = """CREATE TABLE {} (
    load_datetime TIMESTAMP,
    gameweek_now VARCHAR(2),
    username VARCHAR(25),
    PRIMARY KEY (load_datetime)
);
"""

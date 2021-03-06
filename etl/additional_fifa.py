import sys
from argparse import ArgumentParser
from configparser import ConfigParser, NoSectionError

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import keyring
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

from etl.load import BatchSQLUpdate


MATCH_LOOKUP_DEF = """CREATE TABLE {} (
    player_id VARCHAR(3) NOT NULL UNIQUE REFERENCES players_summary(player_id),
    sofifa_id VARCHAR(20),
    match_best INT,
    fpl_player_name VARCHAR(550),
    fifa_name_short VARCHAR(550),
    fifa_name_long VARCHAR(550)
    )
    """


def get_config_section(section, path='config.ini'):
    """"Retrieve general config data from config file"""
    parser = ConfigParser()
    parser.read(path)

    try:
        config_section = dict(parser.items(section))
    except NoSectionError:
        raise NoSectionError(f'Check ini file for [{section}]')
    else:
        return config_section


class BuildFifaData:
    team_mapping = {
        'Arsenal': 'Arsenal',
        'Liverpool': 'Liverpool',
        'Manchester City ': 'Man City',
        'Manchester United': 'Man Utd',
        'Newcastle United': 'Newcastle',
        'Norwich City': 'Norwich',
        'Sheffield United': 'Sheffield Utd',
        'Southampton': 'Southampton',
        'Tottenham Hotspur': 'Spurs',
        'Watford': 'Watford',
        'West Ham United': 'West Ham',
        'Aston Villa': 'Aston Villa',
        'Wolverhampton Wanderers': 'Wolves',
        'Bournemouth': 'Bournemouth',
        'Brighton & Hove Albion': 'Brighton',
        'Burnley': 'Burnley',
        'Chelsea': 'Chelsea',
        'Crystal Palace': 'Crystal Palace',
        'Everton': 'Everton',
        'Leicester City': 'Leicester'
    }

    @staticmethod
    def _check_position(vals, positions: list):
        if any(p in vals for p in positions):
            return True
        else:
            return False

    def _position_columns(self,
                          data,
                          gkp_possibles={'GK'},
                          def_possibles={'LCB', 'RCB', 'LB', 'RB', 'CB', 'RWB',
                                         'LWB'},
                          mid_possibles={'RW', 'LW', 'CAM', 'RCM', 'CDM',
                                         'LDM', 'RM', 'LCM', 'LM', 'RDM',
                                         'RAM', 'CM', 'LAM'},
                          fwd_possibles={'ST', 'CF', 'LS', 'RS', 'RF', 'LF'}
                          ):
        mid_possibles = ['RW', 'LW', 'CAM', 'RCM', 'CDM', 'LDM',
                         'RM', 'LCM', 'LM', 'RDM', 'RAM', 'CM', 'LAM']
        data['is_GKP'] =\
            data['player_positions'].apply(self._check_position,
                                           args=(gkp_possibles))
        data['is_DEF'] =\
            data['player_positions'].apply(lambda x:
                                           self._check_position(x,
                                                                def_possibles))
        data['is_MID'] =\
            data['player_positions'].apply(lambda x:
                                           self._check_position(x,
                                                                mid_possibles))
        data['is_FWD'] =\
            data['player_positions'].apply(lambda x:
                                           self._check_position(x,
                                                                fwd_possibles))
        return data

    def load_fifa_data(self, filepath):
        fifa_data = pd.read_csv(filepath)
        fifa_data = fifa_data.loc[:, ['sofifa_id', 'short_name', 'long_name',
                                      'club', 'player_positions']]
        fifa_data['team_name_long_fifa'] = \
            fifa_data['club'].map(self.team_mapping)
        fifa_data.rename(columns={'short_name': 'fifa_name_short',
                                  'long_name': 'fifa_name_long'}, inplace=True)
        fifa_data = self._position_columns(fifa_data)
        return fifa_data.drop(columns=['club', 'player_positions'])


class MatchData:

    def __init__(self, match_threshold=90, match_func=fuzz.token_set_ratio):
        self._match_threshold = match_threshold
        self._match_func = match_func

    @staticmethod
    def wrap_func(x, y, exact=False, skip=False, func=fuzz.token_set_ratio):
        # If it's an exact match by name, no need to do fuzzy matching!
        if exact:
            return 100
        elif skip:
            return np.nan
        else:
            return func(x, y)

    def match(self, data):
        """Match players in data together based on fuzzy matching"""
        # Match fpl player name to long FIFA name first. This is useful as if
        # there is an immediate close match for a player, we don't have to try
        # matching on short name (long name tends to be better for matching).
        data['match_long'] = \
            data.apply(lambda x: self.wrap_func(x['fpl_player_name'],
                                                x['fifa_name_long'],
                                                exact=x['exact_match_long'],
                                                func=self._match_func),
                       axis=1)

        # Flag if player has exact match based on long name - skip these
        # players in next match phase
        data['complete'] = \
            data.groupby('player_id')['match_long'].transform(max) == 100

        # Match on short name, skipping successful long name matched players
        data['match_short'] = data\
            .apply(lambda x: self.wrap_func(x['fpl_player_name'],
                                            x['fifa_name_short'],
                                            exact=x['exact_match_short'],
                                            skip=x['complete'],
                                            func=self._match_func), axis=1)

        # Want to select match based upon best score from short and long name
        # matching
        data['match_best'] = data[['match_short', 'match_long']].max(axis=1)

        # For those matches lower than our threshold, replace matches with NaN
        set_nan_cols = ['fifa_name_short', 'fifa_name_long',
                        'team_name_long_fifa', 'exact_match_short',
                        'exact_match_long', 'match_short', 'match_long',
                        'match_best', 'sofifa_id']

        data.loc[data.match_best <= self._match_threshold,
                 set_nan_cols] = np.nan

        # Return best match per player
        best_matches = data.sort_values(['player_id', 'match_best'],
                                        ascending=[True, False])
        best_matches.drop_duplicates(subset=['player_id'], inplace=True)

        return best_matches[['sofifa_id', 'player_id', 'match_best',
                             'fpl_player_name', 'fifa_name_short',
                             'fifa_name_long']]

    @staticmethod
    def _subset_data(data):
        # Position info is in both FPL and FIFA tables, albeit multiple, more
        # granular positions are available for the latter. Here, check
        # positions for each overlap e.g. if a player is a DEF in FPL and a
        # CB (defender) in FIFA they can be kept as a possible match.
        drop_gkp = (data.position_name == 'GKP') & ~(data.is_GKP)
        drop_def = (data.position_name == 'DEF') & ~(data.is_DEF)
        drop_mid = (data.position_name == 'MID') & ~(data.is_MID)
        drop_fwd = (data.position_name == 'FWD') & ~(data.is_FWD)
        data = data.loc[~drop_gkp & ~drop_def & ~drop_mid & ~drop_fwd]

        del drop_def, drop_gkp, drop_mid, drop_fwd
        return data.drop(
            columns=['join_key', 'position_name', 'is_GKP',
                     'is_DEF', 'is_MID', 'is_FWD']
            )

    @staticmethod
    def _preprocess_names(data, replace_regex=r'[^a-z\s]'):
        # Ensure names are consistent (e.g. same case and character-set)
        data['fpl_player_name'] = \
            data['fpl_player_name'].str.lower().str.replace(replace_regex, '')
        data['fifa_name_short'] = \
            data['fifa_name_short'].str.lower().str.replace(replace_regex, '')
        data['fifa_name_long'] = \
            data['fifa_name_long'].str.lower().str.replace(replace_regex, '')
        return data

    @staticmethod
    def _find_exact_matches(data):
        # If player names match exactly (without fuzzy matching), flag as True
        data['exact_match_short'] = \
            data['fpl_player_name'] == data['fifa_name_short']
        data['exact_match_long'] = \
            data['fpl_player_name'] == data['fifa_name_long']
        return data

    def execute(self, data):
        """Find best matches between players in FPL and FIFA datasets. Offers
        additional preprocessing steps compared to match method.

        Parameters:
            data: pd.DataFrame
                data set containing names for FPL (fpl_player_name) and FIFA
                (fifa_short_name and fifa_long_name)
        """
        # Subset data so that only matches between players of same position are
        # considered
        data = self._subset_data(data)
        # Make names consistent
        data = self._preprocess_names(data)
        # For speed, find names which match exactly
        data = self._find_exact_matches(data)
        # Use fuzzy matching
        return self.match(data)


def get_existing_player_ids(engine, table_other, table_fpl='players_summary'):
    """Get list of new players which have not yet been added to the lookup
    table"""
    with engine.connect() as conn:
        results = conn.execute(f"""SELECT DISTINCT player_id
                          FROM {table_fpl}
                          INTERSECT
                          SELECT DISTINCT player_id
                          FROM {table_other}""").fetchall()
    return [r for r, in results]


def clear_table(engine, table):
    """Drop lookup table"""
    with engine.connect() as conn:
        conn.execute(f"""DROP TABLE {table}""")


def get_config(path='config.ini', section='db'):
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


def build_fpl_frame(engine,
                    data_players='players_summary',
                    data_teams='teams',
                    data_positions='positions'):
    """From existing tables, build base for FPL to FIFA lookup table"""

    player_names = pd.read_sql(
        f"""SELECT player_id, position_id, team_id,
                CONCAT_WS(' ', first_name, second_name) AS fpl_player_name
            FROM {data_players}""",
        engine
            )
    postions = pd.read_sql(
        f"""SELECT position_id, position_name
            FROM {data_positions}""",
        engine
            )
    teams = pd.read_sql(
        f"""SELECT team_id, team_name_long
            FROM {data_teams}""",
        engine
            )
    fpl_data = player_names.merge(postions,
                                  how='left',
                                  validate='many_to_one',
                                  on='position_id')
    fpl_data = fpl_data.merge(teams,
                              how='left',
                              validate='many_to_one',
                              on='team_id')
    fpl_data.drop(columns=['position_id', 'team_id'], inplace=True)
    return fpl_data.drop_duplicates()


def get_new_player_frame(data, engine, lookup_table):
    """Subset players to those which have not yet been matched"""
    try:
        previous_match = get_existing_player_ids(engine, lookup_table)
    except ProgrammingError:
        # TODO: make more specific - other, unrelated errors could raise this
        # exception
        previous_match = []
    return data.loc[~data['player_id'].isin(previous_match)].copy()


def get_batches_needed(data, fifa_data, config):
    """Determine number of batches needed based upon config"""
    n_left = data['player_id'].nunique()
    n_right = fifa_data['sofifa_id'].nunique()

    max_batch_size = int(config.get('batch_max', 10000))
    return int(np.ceil(n_right * n_left/max_batch_size))


def batch_preparation(data, batches_needed):
    """Split data into batches (list)"""
    return np.array_split(data, batches_needed)


def batch_matching(data, fifa_data):
    """Match data based upon batches"""
    all_batch_matches = []
    match_class = MatchData()
    fifa_data['join_key'] = 1
    for batch_id, batch_data in enumerate(data):
        print(f'Batch: {batch_id + 1}')
        batch_data['join_key'] = 1
        use_data = batch_data.merge(fifa_data,
                                    how='outer',
                                    on='join_key')
        all_batch_matches.append(match_class.execute(use_data))
    return all_batch_matches


def combine_batch_matches(all_batch_matches):
    """Return batch data combined into one"""
    return pd.concat(all_batch_matches)


def final_preparation(data):
    """Final preparation for output matched data"""
    data['sofifa_id'] = \
        data['sofifa_id'].astype(str).replace(r'\.0', '', regex=True)
    return data


def parse_arguments(args):
    parser = ArgumentParser()
    parser.add_argument('-r', '--rebuild',
                        help='Delete and rebuild lookup',
                        action='store_true')
    args = parser.parse_args()
    return vars(args)


def main(args):
    pargs = parse_arguments(args)

    db_config = get_config()
    task_config = get_config_section('fpl_fifa_mapping')
    table_name = task_config['table_name']
    source_data = task_config['source_csv']

    engine = set_db(db_config)

    if pargs['rebuild']:
        # If requested, delete table to rebuild
        clear_table(engine, table_name)

    # Tables to be merged and matched
    fpl_data = build_fpl_frame(engine)
    fifa_data = BuildFifaData().load_fifa_data(filepath=source_data)

    # Only need to, by default, match new players (as it can take long time for
    # all players)
    subset_data = get_new_player_frame(fpl_data, engine,
                                       lookup_table=table_name)
    if len(subset_data):
        # Players split into batches depending on config value
        n_batches = get_batches_needed(subset_data, fifa_data, task_config)
        batched_data = batch_preparation(subset_data, n_batches)

        # Perform matching, can take a while for lots of players
        matched_data = batch_matching(batched_data, fifa_data)

        # Recombine individual batches
        matched_data_full = combine_batch_matches(matched_data)

        # Any final cleaning of matched data
        matched_data_full = final_preparation(matched_data_full)

        # Create table if it does not exists, else append to it
        bsu = BatchSQLUpdate(matched_data_full, engine, MATCH_LOOKUP_DEF,
                             table_name)
        bsu.batch_append()
    else:
        print('No new players - no matching to be done')


if __name__ == "__main__":
    main(sys.argv[1:])

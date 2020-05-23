from configparser import ConfigParser, NoSectionError

from sqlalchemy import create_engine
import keyring
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

# from etl.load import SQLLoad

data_fifa = 'data/fifa-20-complete-player-dataset/players_20.csv'
data_players = 'players_summary'
data_teams = 'teams'
data_positions = 'positions'


def get_config_section(section, path='config.ini'):
    parser = ConfigParser()
    parser.read(path)

    try:
        config_section = dict(parser.items(section))
    except NoSectionError:
        raise NoSectionError(f'Check ini file for [{section}]')
    else:
        return config_section


db_config = get_config_section('db')

db_config.update(
    {'password': keyring.get_password(db_config['keyring_key'],
                                      db_config['user'])}
    )

db_url = f"postgresql://"\
    f"{db_config['user']}:{db_config['password']}@"\
    f"{db_config['host']}:{db_config['port']}/"\
    f"{db_config['db']}"
engine = create_engine(db_url)

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

fpl_data.drop_duplicates(inplace=True)

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


fifa_data = pd.read_csv(data_fifa)
small_fifa = fifa_data.loc[:, ['sofifa_id', 'short_name', 'long_name',
                               'club', 'player_positions']].copy()
small_fifa['team_name_long_fifa'] = small_fifa['club'].map(team_mapping)
small_fifa.rename(columns={'short_name': 'fifa_name_short',
                           'long_name': 'fifa_name_long'},
                  inplace=True)


def check_position(vals, positions: list):
    if any(p in vals for p in positions):
        return True
    else:
        return False


mid_possibles = ['RW', 'LW', 'CAM', 'RCM', 'CDM', 'LDM',
                 'RM', 'LCM', 'LM', 'RDM', 'RAM', 'CM', 'LAM']
small_fifa['is_GKP'] =\
    small_fifa['player_positions'].apply(check_position, args=(['GK']))
small_fifa['is_DEF'] =\
    small_fifa['player_positions'].apply(lambda x: check_position(x, ['LCB', 'RCB', 'LB', 'RB', 'CB', 'RWB', 'LWB']))
small_fifa['is_MID'] =\
    small_fifa['player_positions'].apply(lambda x: check_position(x, mid_possibles))
small_fifa['is_FWD'] =\
    small_fifa['player_positions'].apply(lambda x: check_position(x, ['ST', 'CF', 'LS', 'RS', 'RF', 'LF']))

subset_data = fpl_data.loc[fpl_data.team_name_long == 'Man Utd'].copy()
subset_data = fpl_data.copy()

subset_data['join_key'] = 1
small_fifa_merge = small_fifa.copy()
small_fifa_merge['join_key'] = 1
subset_data2 = subset_data.merge(small_fifa_merge,
                                how='outer',
                                on='join_key')
drop_gkp = (subset_data2.position_name == 'GKP') & ~(subset_data2.is_GKP)
drop_def = (subset_data2.position_name == 'DEF') & ~(subset_data2.is_DEF)
drop_mid = (subset_data2.position_name == 'MID') & ~(subset_data2.is_MID)
drop_fwd = (subset_data2.position_name == 'FWD') & ~(subset_data2.is_FWD)
subset_data2 = subset_data2.loc[~drop_gkp & ~drop_def & ~drop_mid & ~drop_fwd]
subset_data2['fpl_player_name'] = subset_data2['fpl_player_name'].str.lower().str.replace(r'[^a-z\s]', '')
subset_data2['fifa_name_short'] = subset_data2['fifa_name_short'].str.lower().str.replace(r'[^a-z\s]', '')
subset_data2['fifa_name_long'] = subset_data2['fifa_name_long'].str.lower().str.replace(r'[^a-z\s]', '')
subset_data2.drop(columns=['join_key', 'position_name', 'is_GKP', 'is_DEF', 'is_MID', 'is_FWD'], inplace=True)

subset_data2['same_club'] = subset_data2['team_name_long'] == subset_data2['team_name_long_fifa']
subset_data2['exact_match_short'] = subset_data2['fpl_player_name'] == subset_data2['fifa_name_short']
subset_data2['exact_match_long'] = subset_data2['fpl_player_name'] == subset_data2['fifa_name_long']

del drop_def, drop_gkp, drop_mid, drop_fwd


def wrap_func(x, y, exact=False, skip=False, func=fuzz.token_set_ratio):
    # If it's an exact match by name, no need to do fuzzy matching!
    if exact:
        return 100
    elif skip:
        return np.nan
    else:
        return func(x, y)



subset_data2['match_long'] = subset_data2\
                .apply(lambda x: wrap_func(x['fpl_player_name'], x['fifa_name_long'], exact=x['exact_match_long']), axis=1)

subset_data2['complete'] = subset_data2.groupby('player_id')['match_long'].transform(max) == 100

subset_data2['match_short'] = subset_data2\
                .apply(lambda x: wrap_func(x['fpl_player_name'], x['fifa_name_short'], exact=x['exact_match_short'], skip=x['complete']), axis=1)

subset_data2['match_best'] = subset_data2[['match_short', 'match_long']].max(axis=1)
subset_data2.loc[subset_data2.match_best <= 90,
                 ['fifa_name_short', 'fifa_name_long', 'club', 'player_positions', 'team_name_long_fifa',
                 'same_club', 'exact_match_short', 'exact_match_long', 'match_short', 'match_long', 'match_best']] = np.nan

best_matches = subset_data2.sort_values(['player_id', 'match_best'], ascending=[True, False]) 
best_matches.drop_duplicates(subset=['player_id'], inplace=True)

final = best_matches[['sofifa_id', 'player_id', 'match_best', 'fpl_player_name', 'fifa_name_short', 'fifa_name_long']]

def main():
    pass

if __name__ == "__main__":
    pass


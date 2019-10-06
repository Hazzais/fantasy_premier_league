import os
import json
import re
import logging
import pickle

import pandas as pd
import numpy as np


def dval_unique_index(df):
    return len(df.index.unique()) == len(df.index)


def dval_notnull_index(df):
    df_index = df.index
    if isinstance(df_index, pd.MultiIndex):
        # REFACTOR: improve this process (unimportant)
        nulls = 0
        lvls = df_index.levels
        for lvls_f in lvls:
            for lvls_f2 in lvls_f:
                if pd.isnull(lvls_f2):
                    nulls + 1  # noqa
        return nulls == 0
    else:
        return sum(df_index.isnull()) == 0


def check_unique_index(df, df_name, dval_func=dval_unique_index,
                       raise_errors=True):
    """Check indexes are valid (i.e. unique)"""
    try:
        assert dval_func(df)
    except AssertionError as e:
        logging.exception(f'Non-unique index for {df_name}')
        if raise_errors:
            raise AssertionError(e)
    else:
        logging.info(f'Unique index for {df_name}')


def check_not_null_index(df, df_name, dval_func=dval_notnull_index,
                         raise_errors=True):
    """Check no null index values"""
    try:
        assert dval_func(df)
    except AssertionError as e:
        logging.exception(f'Null index value(s) for {df_name}')
        if raise_errors:
            raise AssertionError(e)
    else:
        logging.info(f'No null index entries for {df_name}')


def pandas_integerstr_to_int(x):
    """Pandas is not able to use int() on columns with NaNs in. This function
    does this by stripping out characters including and after a decimal place,
    returning a string."""
    if np.isnan(x):
        return np.nan
    else:
        return re.sub(r'(\.\d+)', '', str(x))


def load_json(data_name, data_loc):
    """Load data from JSON file in data_loc with name data_name"""
    logging.info(f'Loading {data_name} from {data_loc}')
    try:
        with open(os.path.join(data_loc, data_name), 'r') as f:
            loaded = json.load(f)
    except FileNotFoundError as e:
        logging.exception('Unable to find load location')
        raise FileNotFoundError(e)
    else:
        logging.info(f'Successfully loaded {data_name}')
        return loaded


def pickle_data(data, data_name, data_loc):
    """Save unedited data as JSON files"""
    logging.info(f'Saving {data_name} as pickle in {data_loc}')
    try:
        with open(os.path.join(data_loc, f'{data_name}.pkl'), 'wb') as f:
            pickle.dump(data, f)
    except FileNotFoundError as e:
        logging.exception('Unable to find save location')
    else:
        logging.info(f'Successfully saved {data_name}')

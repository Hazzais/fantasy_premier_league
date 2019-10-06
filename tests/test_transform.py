import os
import json
import pickle

import pytest
import pandas as pd
import pandas.testing as pdt
import numpy as np

from fpltools.transform import (dval_unique_index, dval_notnull_index,
                                check_unique_index, check_not_null_index,
                                pandas_integerstr_to_int, load_json,
                                pickle_data)


def test_dval_unique_correct_index():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person2', 'person3'])
    expected = True
    assert dval_unique_index(df) == expected


def test_dval_unique_incorrect_index():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person1', 'person3'])
    expected = False
    assert dval_unique_index(df) == expected


def test_dval_unique_multiindex_correct():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', 'b'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = True
    assert dval_unique_index(df) == expected


def test_dval_unique_multiindex_incorrect():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person1', 'a'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = False
    assert dval_unique_index(df) == expected


def test_dval_unique_empty():
    df = pd.DataFrame()
    expected = True
    assert dval_unique_index(df) == expected


def test_dval_notnull_index_correct():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person2', 'person3'])
    expected = True
    assert dval_notnull_index(df) == expected


def test_dval_notnull_index_incorrect():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=[np.nan, 'person2', 'person3'])
    expected = False
    assert dval_notnull_index(df) == expected


def test_dval_notnull_multiindex_correct():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', 'b'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = True
    assert dval_notnull_index(df) == expected


def test_dval_notnull_multiindex_incorrect():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', np.nan),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = False
    assert dval_notnull_index(df) == expected


def test_dval_notnull_multiindex_incorrect_switch():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  (np.nan, 'b'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = False
    assert dval_notnull_index(df) == expected


def test_dval_notnull_multiindex_incorrect_both():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  (np.nan, np.nan),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    expected = False
    assert dval_notnull_index(df) == expected


def test_dval_notnull_empty():
    df = pd.DataFrame()
    expected = True
    assert dval_notnull_index(df) == expected


def test_check_unique_index_correct_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person2', 'person3'])
    check_unique_index(df, 'test', raise_errors=True)


def test_check_unique_index_incorrect_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person1', 'person3'])
    with pytest.raises(AssertionError):
        check_unique_index(df, 'test', raise_errors=True)


def test_check_unique_multiindex_correct_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', 'b'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    check_unique_index(df, 'test', raise_errors=True)


def test_check_unique_multiindex_incorrect_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person1', 'a'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    with pytest.raises(AssertionError):
        check_unique_index(df, 'test', raise_errors=True)


def test_check_unique_index_incorrect_noraise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person1', 'person3'])
    check_unique_index(df, 'test', raise_errors=False)


def test_check_unique_multiindex_incorrect_noraise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person1', 'a'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    check_unique_index(df, 'test', raise_errors=False)


def test_check_unique_empty():
    df = pd.DataFrame()
    check_unique_index(df, 'test', raise_errors=False)


def test_check_not_null_index_correct_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', 'person2', 'person3'])
    check_not_null_index(df, 'test', raise_errors=True)


def test_check_not_null_index_incorrect_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', np.nan, 'person3'])
    with pytest.raises(AssertionError):
        check_not_null_index(df, 'test', raise_errors=True)


def test_check_not_null_multiindex_correct_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', 'b'),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    check_not_null_index(df, 'test', raise_errors=True)


def test_check_not_null_multiindex_incorrect_raise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', np.nan),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    with pytest.raises(AssertionError):
        check_not_null_index(df, 'test', raise_errors=True)


def test_check_not_null_index_incorrect_noraise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']},
                      index=['person1', np.nan, 'person3'])
    check_not_null_index(df, 'test', raise_errors=False)


def test_check_not_null_multiindex_incorrect_noraise():
    df = pd.DataFrame({'Col1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
    index_vals = [('person1', 'a'),
                  ('person2', np.nan),
                  ('person3', 'c')]
    df.index = pd.MultiIndex.from_tuples(index_vals)
    check_not_null_index(df, 'test', raise_errors=False)


def test_check_not_null_empty():
    df = pd.DataFrame()
    check_not_null_index(df, 'test', raise_errors=False)


def test_intstr_correct_no_nan_series_ints():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.000, 15.000, -1.000, 43.000]})
    expected = pd.Series(['1', '15', '-1', '43'])
    found = df['B'].apply(pandas_integerstr_to_int)
    assert found.equals(expected)


def test_intstr_correct_nan_series_ints():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.000, np.nan, -1.000, 43.000]})
    expected = pd.Series(['1', np.nan, '-1', '43'])
    found = df['B'].apply(pandas_integerstr_to_int)
    assert found.equals(expected)


def test_intstr_correct_no_nan_series_floats():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.234, 15.000, -1.001, 43.023]})
    expected = pd.Series(['1', '15', '-1', '43'])
    found = df['B'].apply(pandas_integerstr_to_int)
    assert found.equals(expected)


def test_intstr_correct_nan_series_floats():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.234, np.nan, -1.001, 43.023]})
    expected = pd.Series(['1', np.nan, '-1', '43'])
    found = df['B'].apply(pandas_integerstr_to_int)
    assert found.equals(expected)


def test_intstr_correct_no_nan_df_ints():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.000, 15.000, -1.000, 43.000],
                       'C': [0.000, 12.000, -63.000, 3.000]})
    expected = pd.DataFrame({'B': ['1', '15', '-1', '43'],
                             'C': ['0', '12', '-63', '3']})
    found = df[['B', 'C']].applymap(pandas_integerstr_to_int)
    pdt.assert_frame_equal(found, expected, check_like=True)


def test_intstr_correct_nan_df_ints():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.000, np.nan, -1.000, 43.000],
                       'C': [0.000, 12.000, np.nan, np.nan]})
    expected = pd.DataFrame({'B': ['1', np.nan, '-1', '43'],
                             'C': ['0', '12', np.nan, np.nan]})
    found = df[['B', 'C']].applymap(pandas_integerstr_to_int)
    pdt.assert_frame_equal(found, expected, check_like=True)


def test_intstr_correct_no_nan_df_floats():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.234, 15.000, -1.001, 43.023],
                       'C': [0.993, 12.001, -63.777, 3.142]})
    expected = pd.DataFrame({'B': ['1', '15', '-1', '43'],
                             'C': ['0', '12', '-63', '3']})
    found = df[['B', 'C']].applymap(pandas_integerstr_to_int)
    pdt.assert_frame_equal(found, expected, check_like=True)


def test_intstr_correct_nan_df_floats():
    df = pd.DataFrame({'A': ['a', 'b', 'c', 'd'],
                       'B': [1.234, np.nan, -1.001, 43.023],
                       'C': [0.993, 12.001, np.nan, np.nan]})
    expected = pd.DataFrame({'B': ['1', np.nan, '-1', '43'],
                             'C': ['0', '12', np.nan, np.nan]})
    found = df[['B', 'C']].applymap(pandas_integerstr_to_int)
    pdt.assert_frame_equal(found, expected, check_like=True)


# TODO: pull out file creation/deletion into a fixture
def test_json_load_correct():
    fname = 'test_json.json'
    try:
        os.remove(fname)
    except OSError:
        pass

    to_json_data = {'A': 1,
                    'B': 2,
                    'C': 3}
    with open(fname, 'w') as f:
        json.dump(to_json_data, f)

    found_json = load_json('.', fname)
    assert found_json == to_json_data
    os.remove(fname)


# TODO: pull out file creation/deletion into a fixture
def test_json_load_incorrect_path():
    non_existant_file = 'non_existant_test_json'
    try:
        os.remove(non_existant_file)
    except OSError:
        pass

    with pytest.raises(FileNotFoundError):
        load_json('.', non_existant_file)


def test_pickle_correct():
    test_data = {'A': 1,
                 'B': 2,
                 'C': 3}
    pickle_data(test_data, 'test_pickle', '.')
    with open('test_pickle.pkl', 'rb') as f:
        reloaded = pickle.load(f)

    assert reloaded == test_data
    os.remove('test_pickle.pkl')


def test_pickle_incorrect():
    test_data = {'A': 1,
                 'B': 2,
                 'C': 3}
    with pytest.raises(FileNotFoundError):
        pickle_data(test_data, 'test_pickle', 'non_existing_folder')

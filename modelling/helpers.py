import re

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class PercentageCalc(BaseEstimator, TransformerMixin):
    """For one or more variables, perform a sort of normalisation by dividing by the total
    for that column (across a particular by group), multipled by a constant (to reflect player base)"""

    def __init__(self,
                 by_group='gameweek_id',
                 variables='temp',
                 final_suffix='_perc',
                 constant=15,
                 drop_by=True):
        self.by_group = by_group
        self.variables = variables
        self.constant = constant
        self.final_suffix = final_suffix
        self.temp_columns = [f'{k}_tmp' for k in variables]
        self.drop_by = drop_by
        self.totals = None

    def _needed_new_bygroup(self, X):
        """Check to see whether any new by-groups not in training data - return them if so as set"""
        all_groups = X[self.by_group]
        return set(all_groups) - set(self.totals.index)

    def _calc_totals(self, X):
        """Estimate the total of each variable (i.e. total number of users variable corresponds to)"""
        percs = X.groupby(self.by_group)[self.variables].sum()/self.constant
        if isinstance(percs, pd.DataFrame):
            percs.columns = self.temp_columns
            return percs
        else:
            return percs.to_frame(name=f'{self.variables[0]}')

    def _apply_totals(self, X, totals):
        """Add on totals to data"""
        return X.merge(totals, how='left', left_on=self.by_group, right_index=True)

    def _calc_final(self, X):
        """Calculate proportion"""
        for c in self.variables:
            X[f'{c}{self.final_suffix}'] = X[c] / X[f'{c}{"_tmp"}']
            X[f'{c}{self.final_suffix}'].fillna(0, inplace=True)
        return X

    def _drop_original_cols(self, X):
        return X.drop(columns=self.variables + self.temp_columns)

    def fit(self, X, y=None):
        self.totals = self._calc_totals(X)
        return self

    def transform(self, X, y=None):
        new_calcs = self._needed_new_bygroup(X)
        if new_calcs is None:
            # All by groups in transform data in train data so can just apply existing numbers
            X = self._apply_totals(X, self.totals)
        else:
            new_totals = self._calc_totals(X.loc[X[self.by_group].isin(new_calcs)])
            final_totals = pd.concat((self.totals, new_totals))
            X = self._apply_totals(X, final_totals)
        X = self._calc_final(X)
        if self.drop_by:
            X.drop(columns=[self.by_group], inplace=True)
        return self._drop_original_cols(X)


def player_attribute_columns(data, column, tags, prefix):
    data_cp = data.copy()
    for t in tags:
        data_cp[f'{prefix}{t}'] = data_cp[column].str.contains(t)
    return data_cp.drop(columns=[column])


def get_tags(data, column, regex=r'\\xa0|#|\s'):
    tags = data[column].dropna().to_list()
    tags = [re.sub(regex, '', x) for x in tags]
    tags = [x.split(',') for x in tags]
    tags = {x for y in tags for x in y}


def bin_player_values(x):
    if x < 50:
        return 'small'
    elif x < 70:
        return 'moderate'
    elif x < 100:
        return 'big'
    else:
        return 'bigger'


def update_nans(data, group: list, columns: list, transform_stat='mean'):
    data_cp = data.copy()
    # Get average for columns (implictly excludes NaNs) across groups
    imputed = data_cp.groupby(group).transform(transform_stat)
    # Not all columns may be present, select those which are
    insert_cols = [f for f in imputed.columns if f in columns]
    imputed = imputed.loc[:, insert_cols]
    # Do the imputing
    data_cp[insert_cols] = data_cp[insert_cols].fillna(imputed)
    return data_cp


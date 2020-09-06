import sys

# TODO: should model be an all-encompassing class? Currently just a sklearn
# pipeline but should feature transforms be added somehow?

from sklearn.base import BaseEstimator, TransformerMixin
class TemporaryModel(BaseEstimator, TransformerMixin):
    def __init__(self, column, gte=0):
        self.column = column
        self.gte = gte

    def fit(self, X, y=None):
        pass

    def predict(self, X):
        return (X[self.column] >= self.gte).values


# TODO: replace parts when ready - will likely need additional dataframes for
# other features.
# TODO: could/should bring out play/points and call function twice
def apply_models(players_next, model_bucket=None, model_play=None,
                 model_points=None):
    model_set = players_next.copy()

    # TODO: replace temporary bits with model loading from params
    # below
    # model_play = utils.load_s3_pickle('fpl-alldata', model_play)
    # model_points = utils.load_s3_pickle('fpl-alldata', model_points)
    model_set['chance_of_playing_next_round'].fillna(100, inplace=True)
    model_set['points_per_game'] = model_set['points_per_game'].astype(float)
    model_play = TemporaryModel('chance_of_playing_next_round', 75)
    model_points = TemporaryModel('points_per_game', 4)

    # TODO: prep data (merging etc.)

    # Call models
    model_set['predict_plays'] = model_play.predict(model_set)
    model_set['predict_points'] = model_points.predict(model_set)
    return model_set

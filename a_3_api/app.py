import keyring
from datetime import datetime

from flask import Flask, jsonify
from sqlalchemy import create_engine, MetaData, Table, desc, cast, Integer, and_, func
from sqlalchemy.orm import Session, sessionmaker, mapper, session
from sqlalchemy.ext.automap import automap_base
import pandas as pd

app = Flask(__name__)

DB_USER = 'harry'
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'fpl'
DB_KEYRING_NAME = 'db_fpl'
DB_PSWD = keyring.get_password(DB_KEYRING_NAME, DB_USER)
Base = automap_base()

db_url = f'postgresql://{DB_USER}:{DB_PSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(db_url)
Base.prepare(engine, reflect=True)

sess = Session(engine)


def get_gameweeks(covid_date):
    if covid_date:
        # Current covid19 thing means API data is showing games which haven't gone ahead are shown as completed.
        # This looks at the most recent played gameweek from players as an alternative.
        # Previous (29)
        gw_prev = sess.query(func.max(cast(Base.classes.players_past.gameweek_id, Integer)))\
            .filter(Base.classes.gameweeks.gameweek_deadline_time < covid_date).first()[0]
        # Current (30)
        gw_curr = str(gw_prev + 1)
        # Next (31)
        gw_next = str(gw_prev + 2)
        gw_prev = str(gw_prev)
    else:
        gw_prev = sess.query(Base.classes.gameweeks.gameweek_id).filter(
            Base.classes.gameweeks.gameweek_previous.is_(True)).first()[0]
        gw_curr = sess.query(Base.classes.gameweeks.gameweek_id).filter(
            Base.classes.gameweeks.gameweek_current.is_(True)).first()[0]
        gw_next = sess.query(Base.classes.gameweeks.gameweek_id).filter(
            Base.classes.gameweeks.gameweek_next.is_(True)).first()[0]

    return gw_prev, gw_curr, gw_next


@app.route('/gameweek/')
def gameweeks():
    prev_curr_next_gws = get_gameweeks(datetime(2020, 3, 11, 0, 0, 0, 0))
    return jsonify(prev_curr_next_gws)


@app.route('/fixtures/')
def fixtures():
    fixtures = sess.query(Base.classes.fixtures).all()
    out_fixtures = [f.__dict__ for f in fixtures]
    out_fixtures_final = [{k: v for k, v in list_el.items() if k != '_sa_instance_state'}
                          for list_el in out_fixtures]
    return jsonify(out_fixtures_final)


@app.route('/players/<int:id>')
def players(id):
    # id = 104
    _, gw_curr, _ = get_gameweeks(datetime(2020, 3, 11, 0, 0, 0, 0))
    # Get next fixture for double gameweek cases and add as filter
    player_rows = sess.query(Base.classes.players_full) \
        .filter(Base.classes.players_full.player_id == str(id)) \
        .filter(Base.classes.players_full.gameweek_id == gw_curr).all()
    out_players = [f.__dict__ for f in player_rows]
    out_players_final = [{k: v for k, v in list_el.items() if k != '_sa_instance_state'}
                          for list_el in out_players]
    return jsonify(out_players_final)


# Endpoints:
# predict_play
# predict_high_scorer
# train_play ?
# train_high_scorer ?
#
# current gameweek
#

if __name__ == '__main__':
    app.run(debug=True)

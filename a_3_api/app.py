import keyring
from flask import Flask, jsonify
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import Session, sessionmaker, mapper
from sqlalchemy.ext.automap import automap_base
from copy import deepcopy

app = Flask(__name__)

DB_USER = 'harry'
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'fpl'
DB_KEYRING_NAME = 'db_fpl'
DB_PSWD = keyring.get_password(DB_KEYRING_NAME, DB_USER)
Base = automap_base()

# db_url = f'postgresql+psycopg2://{DB_USER}:{DB_PSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
db_url = f'postgresql://{DB_USER}:{DB_PSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# app.config['SQLALCHEMY_DATABASE_URI'] = db_url
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # silence the deprecation warning
# db = SQLAlchemy(app)

engine = create_engine(db_url)
Base.prepare(engine, reflect=True)

sess = Session(engine)
# res_old = sess.query(Base.classes.fixtures).all()

@app.route('/fixtures/')
def fixtures():
    fixtures = sess.query(Base.classes.fixtures).all()

    new = deepcopy(fixtures)

    out_fixtures = [f.__dict__ for f in new]    #
    # def rm_el(x, key='_sa_instance_state'):
    #     del x[key]

    out_fixtures2 = []
    for x in out_fixtures:
        out_fixtures2.append([{k: v} for k, v in x.items() if not k.startswith('_sa')])
    #
    # out_fixtures_final = list(map(rm_el, out_fixtures))
    # #
    # # out_fixtures_final = [{k: v} for k, v in list_el.items() if k != '_sa_instance_state'
    # #                       for list_el in fixtures]
    return jsonify(out_fixtures2)

# p = fixtures[0].__dict__.pop('_sa_instance_state').__dict__
#
# eng2 = create_engine(db_url)
# metadata = MetaData(eng2)
# table_fixtures = Table('fixtures', metadata, autoload=True)
#
# class Fixtures:
#     pass
# mapper(Fixtures, table_fixtures)
# Session = sessionmaker(bind=engine)
# session = Session()
# # res = jsonify(session.query(Fixtures).all())
# @app.route('/fixtures/')
# def fixtures():
#     fixtures = session.query(Fixtures).first()
#
#     out_fixtures = [f.__dict__.pop('_sa_instance_state') for f in fixtures]
#     print(f'############### {type(fixtures)}')
#     return jsonify(fixtures)

@app.route('/')
def index():
    return "Hello World!"


# Endpoints:
# predict_play
# predict_high_scorer
# train_play ?
# train_high_scorer ?
#
#
#

if __name__ == '__main__':
    app.run(debug=True)

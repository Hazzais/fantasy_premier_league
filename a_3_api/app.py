from flask import Flask

app = Flask(__name__)

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

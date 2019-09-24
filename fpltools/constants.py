LOGIN_URL = 'https://users.premierleague.com/accounts/login/'

API_URL_BASE = 'https://fantasy.premierleague.com/api/'

API_URLS = {'event': '{}fixtures/?event={{}}/'.format(API_URL_BASE),
            'me': '{}me/'.format(API_URL_BASE),
            'entry': '{}entry/{{}}/'.format(API_URL_BASE),  # playerid? (e.g. 956841)
            'X_history': '{}entry/{{}}/history/'.format(API_URL_BASE),
            'gameweeks': '{}events/'.format(API_URL_BASE),
            'gameweek_fixtures': '{}fixtures/?event={{}}/'.format(API_URL_BASE),
            'gameweek_current': '{}event/{{}}/live/'.format(API_URL_BASE),
            'dynamic': '{}bootstrap-dynamic/'.format(API_URL_BASE),
            'live': '{}live/'.format(API_URL_BASE),  # event/{{gw}}/live
            'history': '{}history/'.format(API_URL_BASE),  # event/{{gw}}/history
            'fixtures': '{}fixtures/'.format(API_URL_BASE),
            'player': '{}element-summary/{{}}/'.format(API_URL_BASE),
            'static': '{}bootstrap-static/'.format(API_URL_BASE),
            'user_history': '{}entry/{{}}/history/'.format(API_URL_BASE),
            'user_picks': '{}entry/{{}}/event/{{}}/picks/'.format(API_URL_BASE),
            'user_team': '{}my-team/{{}}/'.format(API_URL_BASE),
            'user_transfers': '{}entry/{{}}/transfers/'.format(API_URL_BASE),
            'transfers': '{}transfers/'.format(API_URL_BASE),
            'teams': '{}teams/'.format(API_URL_BASE)
            }

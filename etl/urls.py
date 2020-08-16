url_root = 'https://fantasy.premierleague.com/api'

fixtures = f'{url_root}/fixtures'
main = f'{url_root}/bootstrap-static/'


def player_url(player_id: int):
    """Get URL for specific player

    player_id: id FPL uses for the player
    """
    return f'{url_root}element-summary/{player_id}/'

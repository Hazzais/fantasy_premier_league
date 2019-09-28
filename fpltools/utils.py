from datetime import datetime

def get_datetime():
    return round(datetime.now().timestamp())

def get_datetime_string():
    return str(get_datetime())


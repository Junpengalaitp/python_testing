from datetime import datetime

import pytz


def current_time(timezone):
    """
    Returns the current time using the timezone specified in the input message.
    We will use this to calculate the time we send the requested information to the database.
    """
    now = datetime.now(pytz.timezone(timezone))
    return datetime.strftime(now, '%Y-%m-%d %H:%M:%S')

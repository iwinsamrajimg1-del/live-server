from datetime import datetime


def current_date():
    return datetime.now().strftime(
        "%Y-%m-%d"
    )


def current_time():
    return datetime.now().strftime(
        "%I:%M %p"
    )
from datetime import datetime, timedelta


def get_tracking_state(
    departure_time,
    arrival_time
):
    now = datetime.now()

    dep = datetime.strptime(
        departure_time,
        "%I:%M %p"
    )

    arr = datetime.strptime(
        arrival_time,
        "%I:%M %p"
    )

    dep = dep.replace(
        year=now.year,
        month=now.month,
        day=now.day
    )

    arr = arr.replace(
        year=now.year,
        month=now.month,
        day=now.day
    )

    live_start = dep - timedelta(
        hours=1
    )

    completed = arr + timedelta(
        hours=3
    )

    if now < live_start:
        return "waiting"

    if live_start <= now < dep:
        return "pre-live"

    if dep <= now < completed:
        return "active"

    return "completed"
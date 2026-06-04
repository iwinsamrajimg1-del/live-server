import math


def haversine_distance(
    lat1,
    lon1,
    lat2,
    lon2
):
    R = 6371000

    p1 = math.radians(lat1)
    p2 = math.radians(lat2)

    dlat = math.radians(
        lat2 - lat1
    )

    dlon = math.radians(
        lon2 - lon1
    )

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(p1)
        * math.cos(p2)
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a)
    )

    return R * c


def is_stop_reached(
    bus_lat,
    bus_lng,
    stop_lat,
    stop_lng,
    radius=100
):
    distance = haversine_distance(
        bus_lat,
        bus_lng,
        stop_lat,
        stop_lng
    )

    return distance <= radius, distance
import time
import json
import urllib.request

API_URL = (
    "http://localhost:8000/api/gps"
)

BUS_ID = "SBM_001"

ROUTE = [

    (13.0685, 80.2037),

    (13.0000, 80.1700),

    (12.874686, 80.07851),

    (12.7500, 80.0200),

    (12.6841, 79.9836),

    (11.5, 79.0),

    (10.5, 78.6),

    (9.9252, 78.1198),

    (9.0, 77.9),

    (8.7304, 77.7422),

    (8.7139, 77.7567)
]


def send_gps(lat, lng):

    payload = json.dumps({
        "busId":
        BUS_ID,

        "lat":
        lat,

        "lng":
        lng
    }).encode()

    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={
            "Content-Type":
            "application/json"
        },
        method="POST"
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=10
        ) as res:

            print(
                f"[GPS SENT] "
                f"{lat}, {lng}"
            )

    except Exception as e:

        print(
            "[ERROR]",
            e
        )


print(
    "Starting simulator..."
)

while True:

    for lat, lng in ROUTE:

        send_gps(
            lat,
            lng
        )

        time.sleep(2)
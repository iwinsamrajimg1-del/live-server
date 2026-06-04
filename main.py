from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime
import asyncio
import websockets
import json
import re
import urllib.request
import threading
import time
from stop_detector import is_stop_reached

# ======================================================
# APP
# ======================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ======================================================
# FIREBASE
# ======================================================

cred = credentials.Certificate(
    "serviceAccountKey.json"
)

firebase_admin.initialize_app(
    cred,
    {
        "databaseURL":
        "https://realtime-tracking-3809e-default-rtdb.firebaseio.com"
    }
)

print("[OK] Firebase Connected")

# ======================================================
# AUTO FILL PREVIOUS STOPS
# ======================================================

def auto_fill_previous_stops(
    bus_id,
    route_points,
    current_index
):
    """
    If backend started late and bus
    already crossed stops,
    auto-fill previous pending stops.
    """

    today = datetime.now().strftime(
        "%Y-%m-%d"
    )

    actual_ref = db.reference(
        f"actualTimes/{bus_id}/{today}"
    )

    existing = actual_ref.get() or {}

    for i in range(current_index):

        stop = route_points[i]

        stop_name = stop.get(
            "name"
        ) or stop.get(
            "stopName"
        ) or f"stop_{i}"

        stop_key = stop_name.replace(" ", "_")

        if stop_key in existing:
            continue

        scheduled = stop.get(
            "scheduledTime"
        ) or stop.get(
            "time",
            ""
        )

        actual_ref.child(
            stop_key
        ).set({
            "actual": scheduled,
            "scheduled": scheduled,
            "delayMinutes": 0,
            "status": "Passed"
        })

        print(
            f"[AUTO FILL] "
            f"{stop_key} "
            f"-> {scheduled}"
        )

# ======================================================
# MEMORY CACHE & REGISTRY
# ======================================================

# Store in-memory live coordinates and statuses
# Structure: { service_no: { lat, lng, serviceNo, vehicleNo, operator, lastSeenTime, status } }
live_buses = {}

# Active tracking WebSocket task registry
# Structure: { service_no: asyncio.Task }
main_loop = None
tracking_tasks = {}
firebase_listener_started = False

# ======================================================
# HTML PARSER & SCRAPER
# ======================================================

def fetch_and_parse_tracking_page(url):
    print(f"[LIVE API] Scraping tracking page: {url}")
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode('utf-8', errors='ignore')
            
        data_json = {}
        # Parse embedded data JSON (supports both const, var, and let)
        data_match = re.search(r'(?:const|var|let)\s+data\s*=\s*JSON\.parse\(\'(.*?)\'\);', html)
        if data_match:
            try:
                raw_json = data_match.group(1).replace("\\'", "'")
                data_json = json.loads(raw_json)
            except Exception as e:
                print(f"[EXTRACT ERROR] Failed to parse data JSON: {e}")
                
        ws_json = {}
        # Parse embedded websocket JSON (supports both const, var, and let)
        ws_match = re.search(r'(?:const|var|let)\s+websocket\s*=\s*JSON\.parse\(\'(.*?)\'\);', html)
        if ws_match:
            try:
                raw_json = ws_match.group(1).replace("\\'", "'")
                ws_json = json.loads(raw_json)
            except Exception as e:
                print(f"[EXTRACT ERROR] Failed to parse websocket JSON: {e}")
                
        # Fields resolution
        service_no = data_json.get("service", {}).get("number")
        if not service_no:
            parts = url.rstrip('/').split('/')
            service_no = parts[-1] if parts else None
            
        doj = data_json.get("doj")
        if not doj:
            doj = datetime.now().strftime("%Y-%m-%d")
            
        ws_url = ws_json.get("tracking", {}).get("secure", {}).get("url", "reports.yourbus.in")
        ws_port = ws_json.get("tracking", {}).get("secure", {}).get("port", 1029)
        
        vehicle_no = data_json.get("vehicle", {}).get("number") or data_json.get("vehicle", {}).get("vehicleNumber", "")
        op_name = data_json.get("service", {}).get("operatorId", "")
        
        location = data_json.get("vehicle", {}).get("location", {})
        lat = float(location.get("lat")) if location.get("lat") else None
        lng = float(location.get("lng")) if location.get("lng") else None
        
        return {
            "serviceNo": service_no,
            "doj": doj,
            "ws_url": ws_url,
            "ws_port": ws_port,
            "vehicleNo": vehicle_no,
            "operator": op_name,
            "lat": lat,
            "lng": lng
        }
    except Exception as e:
        print(f"[EXTRACT ERROR] Failed to fetch/parse tracking link {url}: {e}")
        try:
            parts = url.rstrip('/').split('/')
            service_no = parts[-1]
            return {
                "serviceNo": service_no,
                "ws_url": "reports.yourbus.in",
                "ws_port": 1029,
                "doj": datetime.now().strftime("%Y-%m-%d"),
                "vehicleNo": "",
                "operator": "",
                "lat": None,
                "lng": None
            }
        except:
            return None

# ======================================================
# HELPERS
# ======================================================

def is_tracking_enabled(bus):
    if not bus:
        return True
    value = bus.get("trackingEnabled")
    return (
        value is None or
        value is True or
        value == "true" or
        value == 1 or
        value == "1"
    )

def extract_service_no_from_url(url):
    if not url:
        return None
    try:
        parts = url.rstrip('/').split('/')
        return parts[-1]
    except:
        return None

def now_time():
    return datetime.now().strftime(
        "%I:%M %p"
    )

def current_date():
    return datetime.now().strftime(
        "%Y-%m-%d"
    )

def calculate_delay(
    scheduled_time,
    actual_time
):
    delay_minutes = 0
    status = "On Time"

    try:
        # Standardize space before AM/PM and convert to uppercase
        scheduled_time = re.sub(r'\s*([AP]M)', r' \1', str(scheduled_time).strip().upper())
        actual_time = re.sub(r'\s*([AP]M)', r' \1', str(actual_time).strip().upper())

        # Ensure single digit hours are zero-padded for strptime (e.g., 5:55 PM -> 05:55 PM)
        if re.match(r'^\d:\d\d', scheduled_time):
            scheduled_time = '0' + scheduled_time
        if re.match(r'^\d:\d\d', actual_time):
            actual_time = '0' + actual_time

        scheduled_dt = (
            datetime.strptime(
                scheduled_time,
                "%I:%M %p"
            )
        )

        actual_dt = (
            datetime.strptime(
                actual_time,
                "%I:%M %p"
            )
        )

        if actual_dt < scheduled_dt:
            actual_dt = (
                actual_dt.replace(
                    day=2
                )
            )
            scheduled_dt = (
                scheduled_dt.replace(
                    day=1
                )
            )

        delay_minutes = int(
            (
                actual_dt
                - scheduled_dt
            ).total_seconds()
            / 60
        )

        if delay_minutes > 0:
            status = "Delayed"
        elif delay_minutes < 0:
            status = "Early"
        else:
            status = "On Time"

    except Exception as e:
        print(
            "[DELAY ERROR]",
            e
        )

    return (
        delay_minutes,
        status
    )

# ======================================================
# STOP DETECTION
# ======================================================

async def process_stop_detection(
    bus_id,
    lat,
    lng
):
    buses_ref = db.reference(
        "buses"
    )

    buses = (
        buses_ref.get()
        or {}
    )

    matched_bus = None

    # Robust matching: direct link match, fallback to operator prefix check
    for _, bus in buses.items():
        link = bus.get("link")
        if link:
            extracted = extract_service_no_from_url(link)
            if extracted and extracted.upper() == bus_id.upper():
                matched_bus = bus
                break

        operator = (
            bus.get("op", "")
            .split()[0]
            .upper()
        )

        if operator and operator in bus_id.upper():
            matched_bus = bus
            break

    if not matched_bus:
        print(
            "[STOP DETECTION] "
            f"No matching bus for {bus_id}"
        )
        return

    route = matched_bus.get(
        "route",
        ""
    )

    route_key = (
        route.lower()
        .replace(" - ", "___")
        .replace(" ", "_")
    )

    route_points_ref = db.reference(
        f"routePoints/{route_key}"
    )

    route_points = (
        route_points_ref.get()
        or []
    )

    print(
        f"[ROUTE POINTS] "
        f"{len(route_points)} loaded"
    )

    for index, stop in enumerate(route_points):
        stop_name = stop.get(
            "name",
            "UNKNOWN"
        )

        stop_lat = stop.get("lat")
        stop_lng = stop.get("lng")

        if (
            stop_lat is None
            or stop_lng is None
        ):
            continue

        within, distance = (
            is_stop_reached(
                lat,
                lng,
                stop_lat,
                stop_lng,
                100
            )
        )

        print(
            f"[STOP CHECK] "
            f"{stop_name} "
            f"{distance:.2f}m "
            f"within={within}"
        )

        if not within:
            continue

        auto_fill_previous_stops(
            bus_id,
            route_points,
            index
        )

        date = current_date()
        stop_key = (
            stop_name
            .replace(" ", "_")
        )

        actual_ref = db.reference(
            f"actualTimes/"
            f"{bus_id}/"
            f"{date}/"
            f"{stop_key}"
        )

        exists = (
            actual_ref.get()
        )

        if exists:
            print(
                "[SKIP] already recorded"
            )
            continue

        scheduled_time = next(
            (
                s.get("time")
                for s in matched_bus.get(
                    "stopTimes",
                    []
                )
                if (
                    s.get(
                        "name",
                        ""
                    ).upper()
                    ==
                    stop_name.upper()
                )
            ),
            ""
        )

        actual_time = now_time()

        delay_minutes, status = (
            calculate_delay(
                scheduled_time,
                actual_time
            )
        )

        actual_ref.set({
            "scheduled":
            scheduled_time,

            "actual":
            actual_time,

            "delayMinutes":
            delay_minutes,

            "status":
            status,

            "recordedAt":
            int(
                datetime.now()
                .timestamp()
            )
        })

        print(
            f"[STOP ARRIVED] "
            f"{stop_name}"
        )

        print(
            f"[DELAY] "
            f"{delay_minutes} mins "
            f"{status}"
        )

# ======================================================
# WEBSOCKET LISTENER & STALE CHECKER
# ======================================================

async def websocket_listener(service_no, ws_url, ws_port, doj, default_vehicle_no, op_name):
    # Construct complete ws endpoint URL
    endpoint = ws_url
    if not endpoint.startswith("ws://") and not endpoint.startswith("wss://"):
        endpoint = f"wss://{endpoint}"
    if ":" not in endpoint[8:]:
        endpoint = f"{endpoint}:{ws_port}"

    print(f"[WEBSOCKET] Starting connection loop for {service_no} => {endpoint}")
    
    while True:
        try:
            async with websockets.connect(endpoint, ping_interval=30, ping_timeout=10) as websocket:
                # Subscription Payload
                sub_msg = {
                    "serviceNo": service_no,
                    "doj": doj,
                    "trackingType": "full-tracking"
                }
                await websocket.send(json.dumps(sub_msg))
                print(f"[WEBSOCKET] Subscribed successfully to {service_no}")

                # WebSocket receive loop
                async for message in websocket:
                    try:
                        msg_data = json.loads(message)
                        vehicle_info = msg_data.get("vehicleInfo", {})
                        position = vehicle_info.get("position", {})
                        lat = position.get("latitude")
                        lng = position.get("longitude")
                        
                        if lat is not None and lng is not None:
                            veh_no = vehicle_info.get("vehicleNumber") or vehicle_info.get("vehicleNum") or default_vehicle_no or ""
                            
                            # Update in-memory coordinates
                            live_buses[service_no] = {
                                "lat": float(lat),
                                "lng": float(lng),
                                "serviceNo": service_no,
                                "vehicleNo": veh_no,
                                "operator": op_name,
                                "lastSeenTime": datetime.now(),
                                "status": "live"
                            }
                            
                            print(f"[MARKER UPDATE] GPS update: {service_no} => {lat}, {lng}")
                            await process_stop_detection(service_no, float(lat), float(lng))
                            
                    except Exception as e:
                        print(f"[WEBSOCKET ERROR] Message parsing failed: {e}")
                        
        except asyncio.CancelledError:
            print(f"[WEBSOCKET] Task cancelled for {service_no}")
            break
        except Exception as e:
            print(f"[WEBSOCKET ERROR] Disconnected from {service_no}: {e}. Reconnecting in 5 seconds...")
            if service_no in live_buses:
                live_buses[service_no]["status"] = "offline"
            await asyncio.sleep(5)

async def stale_bus_checker():
    while True:
        try:
            now = datetime.now()
            remove_buses = []

            for bus_id, bus_data in list(live_buses.items()):
                last_seen = bus_data.get("lastSeenTime")
                if not last_seen:
                    bus_data["lastSeenTime"] = now
                    continue

                inactive_time = (now - last_seen).total_seconds()

                if inactive_time > 300:
                    print(
                        f"[SCHEDULER] "
                        f"{bus_id} inactive "
                        f"for "
                        f"{inactive_time:.1f}s "
                        f"(>300s)"
                    )
                    remove_buses.append(
                        bus_id
                    )

            for bus_id in remove_buses:
                # cancel websocket task
                if bus_id in tracking_tasks:
                    try:
                        # If future is from asyncio.run_coroutine_threadsafe, it's a concurrent.futures.Future
                        # We should call cancel() on it.
                        tracking_tasks[bus_id].cancel()
                    except Exception:
                        pass
                    tracking_tasks.pop(
                        bus_id,
                        None
                    )

                # remove RAM data
                live_buses.pop(
                    bus_id,
                    None
                )

                print(
                    f"[SCHEDULER] "
                    f"Removed stale bus "
                    f"{bus_id}"
                )

        except Exception as e:
            print(
                "[SCHEDULER ERROR]",
                str(e)
            )

        await asyncio.sleep(10)

# ======================================================
# BACKGROUND SCHEDULER
# ======================================================

def start_firebase_bus_listener():
    """
    Listen for Firebase bus changes.
    Automatically starts tracking when
    new buses are added.
    """
    global firebase_listener_started

    if firebase_listener_started:
        return

    firebase_listener_started = True

    ref = db.reference("buses")

    def on_change(event):
        try:
            print("[FIREBASE] Bus update detected")
            buses = ref.get() or {}

            active_configured_services = set()
            for bus_key, bus in buses.items():
                tracking_enabled = is_tracking_enabled(bus)
                tracking_link = bus.get("link", "")

                if not tracking_enabled or not tracking_link:
                    continue

                service_no = extract_service_no_from_url(tracking_link)
                if not service_no:
                    continue

                active_configured_services.add(service_no)

                # already running
                if service_no in tracking_tasks:
                    continue

                print(f"[TRACKING] Starting {service_no}")

                # Using thread-safe approach to spawn task on main event loop
                page_data = fetch_and_parse_tracking_page(tracking_link)
                
                if page_data:
                    future = asyncio.run_coroutine_threadsafe(
                        websocket_listener(
                            service_no=page_data["serviceNo"] or service_no,
                            ws_url=page_data["ws_url"],
                            ws_port=page_data["ws_port"],
                            doj=page_data["doj"],
                            default_vehicle_no=page_data["vehicleNo"],
                            op_name=page_data["operator"] or bus.get("op", "")
                        ),
                        main_loop
                    )
                    tracking_tasks[service_no] = future

                    # placeholder entry
                    if service_no not in live_buses:
                        live_buses[service_no] = {
                            "lat": page_data["lat"],
                            "lng": page_data["lng"],
                            "serviceNo": service_no,
                            "vehicleNo": page_data["vehicleNo"],
                            "operator": page_data["operator"] or bus.get("op", ""),
                            "lastSeenTime": datetime.now(),
                            "status": "live" if (page_data["lat"] is not None and page_data["lng"] is not None) else "waiting"
                        }

            # Stop tracking and remove memory logs for buses that are no longer configured/active in Firebase
            for service_no in list(tracking_tasks.keys()):
                if service_no not in active_configured_services:
                    print(f"[TRACKING] Stopping tracking for {service_no} (removed/disabled in database)")
                    try:
                        tracking_tasks[service_no].cancel()
                    except Exception:
                        pass
                    tracking_tasks.pop(service_no, None)
                    live_buses.pop(service_no, None)
        except Exception as e:
            print("[FIREBASE LISTENER ERROR]", str(e))

    print("[FIREBASE] Live listener started")
    ref.listen(on_change)

@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()

    print(
        "[STARTUP] Starting "
        "Firebase listener..."
    )

    threading.Thread(
        target=start_firebase_bus_listener,
        daemon=True
    ).start()

    asyncio.create_task(
        stale_bus_checker()
    )

# ======================================================
# API ENDPOINTS
# ======================================================

@app.get("/api/buses/live")
async def get_live():
    print("[LIVE API]")
    formatted = {}
    now = datetime.now()
    
    buses_ref = db.reference("buses")
    configured_buses = buses_ref.get() or {}

    for bus_id, data in live_buses.items():
        last_seen = data.get("lastSeenTime")
        if last_seen:
            diff = int((now - last_seen).total_seconds())
            last_updated_str = f"{diff} sec ago" if diff < 60 else f"{diff // 60} min ago"
        else:
            last_updated_str = "Waiting for GPS"
            
        matched_bus = {}
        for k, b in configured_buses.items():
            if b.get("link", "").find(bus_id) != -1:
                matched_bus = b
                break

        formatted[bus_id] = {
            "lat": data.get("lat"),
            "lng": data.get("lng"),
            "status": data.get("status", "waiting"),
            "lastUpdated": data.get("lastUpdated") or last_updated_str,
            "operator": data.get("operator"),
            "op": matched_bus.get("op", matched_bus.get("operator", data.get("operator"))),
            "route": matched_bus.get("route", ""),
            "time": matched_bus.get("time", matched_bus.get("departureTime", "")),
            "arrivalTime": matched_bus.get("arrivalTime", "")
        }

    return formatted

@app.get("/api/actual-times")
async def actual_times():
    ref = db.reference("actualTimes")
    return ref.get() or {}

@app.get("/api/history")
async def history():
    ref = db.reference("busHistory")
    return ref.get() or {}
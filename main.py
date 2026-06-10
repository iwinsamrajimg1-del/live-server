from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import asyncio
import os
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

firebase_creds = json.loads(
    os.environ["FIREBASE_CREDENTIALS"]
)

cred = credentials.Certificate(firebase_creds)

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
    current_index,
    journey_key
):
    """
    If backend started late and bus
    already crossed stops,
    auto-fill previous pending stops.
    """

    existing = recorded_stops.get(bus_id, set())

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

        db.reference(
            f"journeys/{bus_id}/{journey_key}/actualTimes/{stop_key}"
        ).set({
            "actual": scheduled,
            "scheduled": scheduled,
            "delayMinutes": 0,
            "status": "Passed"
        })
        
        if bus_id in recorded_stops:
            recorded_stops[bus_id].add(stop_key)

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
recorded_stops = {}
provider_stop_statuses = {}
live_bus_locations = {}
buses_cache = {}
active_journeys_cache = {}
route_points_cache = {}
initialized_journeys = set()


# Active tracking WebSocket task registry
# Structure: { service_no: asyncio.Task }
main_loop = None
tracking_tasks = {}
firebase_listener_started = False
STALE_TIMEOUT = 1800

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

IST = ZoneInfo("Asia/Kolkata")

def now_time():
    t = datetime.now(
        IST
    ).strftime("%I:%M %p")
    print(
        f"[TIME DEBUG] "
        f"IST={datetime.now(IST)} "
        f"Actual={t}"
    )
    return t

def current_date():
    return datetime.now(
        IST
    ).strftime("%Y-%m-%d")

STOP_RADIUS_METERS = 1000

def format_time_24hr(time_str):
    if not time_str:
        return "0000"
    time_str = re.sub(r'\s*([AP]M)', r' \1', str(time_str).strip().upper())
    if re.match(r'^\d:\d\d', time_str):
        time_str = '0' + time_str
    try:
        dt = datetime.strptime(time_str, "%I:%M %p")
        return dt.strftime("%H%M")
    except Exception as e:
        print(f"[FORMAT TIME 24HR ERROR] {e} for {time_str}")
        return "0000"

def parse_datetime(date_str, time_str):
    time_str = re.sub(r'\s*([AP]M)', r' \1', str(time_str).strip().upper())
    if re.match(r'^\d:\d\d', time_str):
        time_str = '0' + time_str
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %I:%M %p")
    return dt.replace(tzinfo=IST)

def get_journey_ref(bus_id, journey_key):
    return db.reference(f"journeys/{bus_id}/{journey_key}")

def update_journey_status():
    """
    Updates status of active journeys and handles 24h cleanup.
    """
    now = datetime.now(IST)
    buses = buses_cache or {}
    
    active_ref = db.reference("activeJourneys")
    active_journeys = active_journeys_cache or {}
    
    for bus_id, active_info in active_journeys.items():
        if not isinstance(active_info, dict):
            continue
        journey_date = active_info.get("journeyDate")
        journey_key = active_info.get("journeyKey")
        status = active_info.get("status")
        completed_at = active_info.get("completedAt")
        
        if not journey_date or not journey_key:
            continue
            
        # 24 hours cleanup for COMPLETED journeys
        if status == "COMPLETED" and completed_at:
            if (time.time() - completed_at) > 86400:
                print(f"[STATUS CLEANUP] Removing completed journey {bus_id} ({journey_key})")
                db.reference(f"activeJourneys/{bus_id}").delete()
                continue
                
        # Find matching configured bus to calculate status
        matched_bus = None
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
            continue
            
        departure_time_str = matched_bus.get("time") or matched_bus.get("departureTime")
        arrival_time_str = matched_bus.get("arrivalTime")
        
        if not departure_time_str or not arrival_time_str:
            continue
            
        try:
            departure_dt = parse_datetime(journey_date, departure_time_str)
            arrival_dt = parse_datetime(journey_date, arrival_time_str)
            
            if arrival_dt < departure_dt:
                arrival_dt += timedelta(days=1)
                
            # If status is already marked COMPLETED (e.g. by final stop arrival), we keep it COMPLETED
            if status == "COMPLETED":
                new_status = "COMPLETED"
            elif now < departure_dt:
                new_status = "WAITING"
            elif departure_dt <= now <= (arrival_dt + timedelta(hours=3)):
                new_status = "LIVE"
            else:
                new_status = "COMPLETED"
                
            if new_status != status:
                # Update in activeJourneys ONLY if it changed
                payload = {"status": new_status}
                if new_status == "COMPLETED":
                    payload["completedAt"] = int(time.time())
                active_ref.child(bus_id).update(payload)
                    
                # Update in journeys metadata
                db.reference(f"journeys/{bus_id}/{journey_key}/metadata").update({"status": new_status})
                print(f"[STATUS UPDATE] {bus_id} ({journey_key}) -> {new_status}")
        except Exception as e:
            print(f"[STATUS UPDATE ERROR] For {bus_id} on {journey_date}: {e}")

def init_journey_metadata(bus_id, journey_date, departure_time_str, matched_bus=None):
    journey_key = f"{journey_date}_{format_time_24hr(departure_time_str)}"
    
    # Calculate status
    status = "LIVE"
    if matched_bus:
        departure_time_str = matched_bus.get("time") or matched_bus.get("departureTime") or departure_time_str
        arrival_time_str = matched_bus.get("arrivalTime")
        if departure_time_str and arrival_time_str:
            try:
                now = datetime.now(IST)
                departure_dt = parse_datetime(journey_date, departure_time_str)
                arrival_dt = parse_datetime(journey_date, arrival_time_str)
                if arrival_dt < departure_dt:
                    arrival_dt += timedelta(days=1)
                if now < departure_dt:
                    status = "WAITING"
                elif departure_dt <= now <= (arrival_dt + timedelta(hours=3)):
                    status = "LIVE"
                else:
                    status = "COMPLETED"
            except Exception as e:
                print(f"[INIT STATUS ERROR] {e}")
                
    # Update activeJourneys ONLY if key or status changed
    active_ref = db.reference(f"activeJourneys/{bus_id}")
    active_existing = active_journeys_cache.get(bus_id)
    write_active = True
    if isinstance(active_existing, dict):
        if active_existing.get("journeyKey") == journey_key and active_existing.get("status") == status:
            write_active = False
            
    if write_active:
        active_payload = {
            "journeyDate": journey_date,
            "journeyKey": journey_key,
            "status": status
        }
        if status == "COMPLETED":
            active_payload["completedAt"] = int(time.time())
        active_ref.update(active_payload)
    
    cache_key = f"{bus_id}_{journey_key}"
    if cache_key not in initialized_journeys:
        # Check if metadata exists under journeys/{bus_id}/{journey_key}/metadata
        meta_ref = db.reference(f"journeys/{bus_id}/{journey_key}/metadata")
        meta = meta_ref.get()
        if not meta:
            meta_ref.set({
                "departureDate": journey_date,
                "departureTime": departure_time_str,
                "serviceNo": bus_id,
                "status": status,
                "schemaVersion": 2
            })
        initialized_journeys.add(cache_key)
    return journey_key

def calculate_delay_with_journey_date(
    journey_date,
    departure_time,
    scheduled_time,
    actual_timestamp
):
    """
    Returns:
        (delay_minutes, status)

    status:
        Early
        On Time
        Delayed
    """
    try:
        # Standardize strings
        departure_time = re.sub(r'\s*([AP]M)', r' \1', str(departure_time).strip().upper())
        scheduled_time = re.sub(r'\s*([AP]M)', r' \1', str(scheduled_time).strip().upper())
        
        if re.match(r'^\d:\d\d', departure_time):
            departure_time = '0' + departure_time
        if re.match(r'^\d:\d\d', scheduled_time):
            scheduled_time = '0' + scheduled_time

        # Parse departure time on journey_date
        departure_dt = parse_datetime(journey_date, departure_time)
        
        # Parse scheduled time on journey_date
        scheduled_dt = parse_datetime(journey_date, scheduled_time)
        
        # If scheduled stop time is earlier in the day than departure time, it has crossed midnight
        # E.g. scheduled = 01:30 AM, departure = 09:30 PM
        if scheduled_dt < departure_dt:
            scheduled_dt += timedelta(days=1)
            
        actual_dt = datetime.fromtimestamp(actual_timestamp, IST)
        
        delay_minutes = int(
            (actual_dt - scheduled_dt).total_seconds() / 60
        )
        
        if delay_minutes > 0:
            status = "Delayed"
        elif delay_minutes < 0:
            status = "Early"
        else:
            status = "On Time"
            
        print(
            f"[DELAY DEBUG] "
            f"JourneyDate={journey_date} "
            f"Departure={departure_time} "
            f"Scheduled={scheduled_time} "
            f"Actual={actual_dt.strftime('%Y-%m-%d %I:%M %p')} "
            f"Delay={delay_minutes} "
            f"Status={status}"
        )
        return delay_minutes, status
        
    except Exception as e:
        print(
            f"[DELAY ERROR] "
            f"JourneyDate={journey_date} "
            f"Departure={departure_time} "
            f"Scheduled={scheduled_time} "
            f"Error={e}"
        )
        return 0, "On Time"

# ======================================================
# PROVIDER STOP DETECTION (V3)
# ======================================================

def auto_build_route_from_provider(bus_id, positions, journey_key):
    """
    Auto-build intermediateStops from provider positions data.
    Writes to journeys/{bus_id}/{journey_key}/providerStops
    """
    stops = []
    for pos in positions:
        stops.append({
            "name": pos.get("name", ""),
            "scheduled": pos.get("scheduleTime", ""),
            "providerStatus": pos.get("status", "")
        })
    try:
        db.reference(f"journeys/{bus_id}/{journey_key}/providerStops").set(stops)
        print(f"[AUTO BUILD ROUTE] Wrote {len(stops)} stops to providerStops for {bus_id}")
    except Exception as e:
        print(f"[AUTO BUILD ROUTE ERROR] {e}")

def record_provider_stop(bus_id, journey_key, stop_name, scheduled_time, 
                          journey_date, departure_time_str, is_first_message_missed=False, source="provider"):
    stop_key = stop_name.replace(" ", "_")
    
    if bus_id not in recorded_stops:
        recorded_stops[bus_id] = set()
    
    if stop_key in recorded_stops[bus_id]:
        return  # Already recorded
    
    if is_first_message_missed:
        # Auto-fill past stops with scheduled time and 0 delay
        try:
            db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes/{stop_key}").set({
                "scheduled": scheduled_time,
                "actual": scheduled_time,
                "actualTimestamp": int(time.time()),
                "delayMinutes": 0,
                "status": "Passed",
                "recordedAt": int(time.time()),
                "source": "provider_autofill"
            })
            print(f"[STOP AUTO-FILL RECORDED] {bus_id}: {stop_name} set to scheduled={scheduled_time}")
        except Exception as e:
            print(f"[STOP AUTO-FILL RECORDED ERROR] {e}")
    else:
        actual_dt = datetime.now(IST)
        actual_time = actual_dt.strftime("%I:%M %p")
        actual_timestamp = int(actual_dt.timestamp())
        
        delay_minutes, status = calculate_delay_with_journey_date(
            journey_date, departure_time_str, scheduled_time, actual_timestamp
        )
        
        try:
            db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes/{stop_key}").set({
                "scheduled": scheduled_time,
                "actual": actual_time,
                "actualTimestamp": actual_timestamp,
                "delayMinutes": delay_minutes,
                "status": status,
                "recordedAt": actual_timestamp,
                "source": source
            })
            print(f"[STOP ARRIVED PROVIDER ({source})] {bus_id}: {stop_name} | delay={delay_minutes} status={status}")
        except Exception as e:
            print(f"[STOP ARRIVE RECORDED ERROR] {e}")
    
    recorded_stops[bus_id].add(stop_key)

async def process_provider_stops(bus_id, positions, journey_date, journey_key, departure_time_str, lat=None, lng=None):
    if not positions:
        return

    # 1. Initialize caches for this bus
    is_first_msg = False
    if bus_id not in provider_stop_statuses:
        provider_stop_statuses[bus_id] = {}
        is_first_msg = True
        
    if bus_id not in recorded_stops:
        try:
            actual_times_existing = db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes").get() or {}
            recorded_stops[bus_id] = set(actual_times_existing.keys())
        except Exception as e:
            print(f"[CACHE STOP ERROR] {e}")
            recorded_stops[bus_id] = set()

    # 2. Check if providerStops exists in Firebase, if not, auto build
    journey_init_key = f"{bus_id}_{journey_key}_providerStops"
    if journey_init_key not in initialized_journeys:
        auto_build_route_from_provider(bus_id, positions, journey_key)
        initialized_journeys.add(journey_init_key)

    # 3. Analyze status transitions and current statuses
    prev_statuses = provider_stop_statuses[bus_id]
    
    last_passed = None
    next_stop = None
    
    for index, pos in enumerate(positions):
        stop_name = pos.get("name")
        status = pos.get("status")
        scheduled_time = pos.get("scheduleTime")
        
        if not stop_name:
            continue
            
        stop_key = stop_name.replace(" ", "_")
        prev_status = prev_statuses.get(stop_name)
        
        # Check radius-based detection if GPS coordinates are available
        stop_lat = pos.get("latitude") or pos.get("lat")
        stop_lng = pos.get("longitude") or pos.get("lng")
        
        within_radius = False
        if lat is not None and lng is not None and stop_lat is not None and stop_lng is not None:
            try:
                within, distance = is_stop_reached(
                    float(lat),
                    float(lng),
                    float(stop_lat),
                    float(stop_lng),
                    STOP_RADIUS_METERS
                )
                if within:
                    within_radius = True
                    print(f"[PROVIDER STOP RADIUS DETECTED] {bus_id}: {stop_name} is within radius ({distance:.1f}m)")
            except Exception as e:
                print(f"[PROVIDER STOP RADIUS ERROR] {e}")
                
        # Determine if stop is passed/crossed
        is_passed = (status in ["MISSED", "PASSED"]) or (stop_key in recorded_stops.get(bus_id, set())) or within_radius
        
        if is_passed:
            last_passed = stop_name
        elif next_stop is None:
            # First stop that is not passed is the next stop
            next_stop = stop_name
            
        should_record = False
        is_first_message_missed = False
        source_val = "provider"
        
        if prev_status == "NEXT" and status in ["MISSED", "PASSED"]:
            should_record = True
            source_val = "provider"
            print(f"[STOP TRANSITION] {bus_id}: {stop_name} transitioned from NEXT to {status}")
        elif status in ["MISSED", "PASSED"] and stop_key not in recorded_stops.get(bus_id, set()):
            should_record = True
            if is_first_msg or prev_status is None:
                is_first_message_missed = True
                source_val = "provider_autofill"
                print(f"[STOP AUTO-FILL] {bus_id}: {stop_name} was already {status} when tracking started")
            else:
                source_val = "provider"
                print(f"[STOP INITIAL DETECTION] {bus_id}: {stop_name} is {status} (not previously recorded)")
        elif within_radius and stop_key not in recorded_stops.get(bus_id, set()):
            should_record = True
            is_first_message_missed = False
            source_val = "provider_gps"
            print(f"[PROVIDER STOP GPS DETECTION] {bus_id}: {stop_name} reached via radius check")
            
        if should_record:
            record_provider_stop(
                bus_id=bus_id,
                journey_key=journey_key,
                stop_name=stop_name,
                scheduled_time=scheduled_time,
                journey_date=journey_date,
                departure_time_str=departure_time_str,
                is_first_message_missed=is_first_message_missed,
                source=source_val
            )
            
        prev_statuses[stop_name] = status

    # Update live_buses in-memory details
    if bus_id in live_buses:
        if last_passed:
            live_buses[bus_id]["lastPassedStop"] = last_passed
        if next_stop:
            live_buses[bus_id]["nextStop"] = next_stop
        live_buses[bus_id]["providerStopCount"] = len(positions)
        
        # Update activeJourneys in Firebase
        try:
            update_data = {}
            if last_passed:
                update_data["lastPassedStop"] = last_passed
            if next_stop:
                update_data["nextStop"] = next_stop
            if update_data:
                db.reference(f"activeJourneys/{bus_id}").update(update_data)
        except Exception as e:
            print(f"[FIREBASE UPDATE ERROR] activeJourneys update failed: {e}")

    # Complete journey if last stop in provider positions is MISSED or PASSED or recorded
    is_last_passed = False
    if positions:
        last_stop_pos = positions[-1]
        last_stop_name = last_stop_pos.get("name", "")
        last_stop_key = last_stop_name.replace(" ", "_")
        if last_stop_pos.get("status") in ["MISSED", "PASSED"] or last_stop_key in recorded_stops.get(bus_id, set()):
            is_last_passed = True
            
    if is_last_passed:
        is_completed_in_mem = live_buses.get(bus_id, {}).get("trip_completed", False)
        if not is_completed_in_mem:
            actual_arrival_dt = datetime.now(IST).strftime("%Y-%m-%d %I:%M %p")
            actual_arrival_ts = int(time.time())
            print(f"[FINAL STOP REACHED (PROVIDER)] Recording actual arrival for {bus_id}: {actual_arrival_dt}")
            
            try:
                db.reference(f"journeys/{bus_id}/{journey_key}/metadata").update({
                    "actualArrival": actual_arrival_dt,
                    "actualArrivalTimestamp": actual_arrival_ts,
                    "status": "COMPLETED"
                })
                
                db.reference(f"activeJourneys/{bus_id}").update({
                    "status": "COMPLETED",
                    "completedAt": actual_arrival_ts
                })
                
                if bus_id in live_buses:
                    live_buses[bus_id]["trip_completed"] = True
            except Exception as e:
                print(f"[FINAL STOP PROVIDER ERROR] {e}")

# ======================================================
# STOP DETECTION
# ======================================================

async def process_stop_detection(
    bus_id,
    lat,
    lng,
    journey_date
):
    buses = buses_cache or db.reference("buses").get() or {}

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

    departure_time_str = (
        matched_bus.get("time")
        or matched_bus.get("departureTime")
        or ""
    )

    # Try retrieving journeyKey from in-memory cache first
    journey_key = None
    if bus_id in live_buses:
        journey_key = live_buses[bus_id].get("journeyKey")
        
    if not journey_key:
        journey_key = init_journey_metadata(bus_id, journey_date, departure_time_str, matched_bus)

    # Initialize recorded stops cache for this bus from Firebase if not done yet
    if bus_id not in recorded_stops:
        try:
            actual_times_existing = db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes").get() or {}
            recorded_stops[bus_id] = set(actual_times_existing.keys())
        except Exception as e:
            print(f"[CACHE STOP ERROR] {e}")
            recorded_stops[bus_id] = set()

    route = matched_bus.get(
        "route",
        ""
    )

    route_key = (
        route.lower()
        .replace(" - ", "___")
        .replace(" ", "_")
    )

    if route_key not in route_points_cache:
        route_points_ref = db.reference(
            f"routePoints/{route_key}"
        )
        route_points_cache[route_key] = (
            route_points_ref.get()
            or []
        )
    route_points = route_points_cache[route_key]

    print(
        f"[ROUTE DEBUG] "
        f"route={route}"
    )

    print(
        f"[ROUTE KEY] "
        f"{route_key}"
    )

    print(
        f"[ROUTE POINT COUNT] "
        f"{len(route_points)}"
    )

    print(f"[AVAILABLE ROUTES] {list(route_points_cache.keys())}")

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
                STOP_RADIUS_METERS
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
            index,
            journey_key
        )

        stop_key = (
            stop_name
            .replace(" ", "_")
        )

        if stop_key in recorded_stops[bus_id]:
            print(
                "[SKIP] already recorded (RAM cache)"
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

        actual_dt = datetime.now(IST)
        actual_time = actual_dt.strftime("%I:%M %p")
        actual_timestamp = int(actual_dt.timestamp())

        delay_minutes, status = (
            calculate_delay_with_journey_date(
                journey_date,
                departure_time_str,
                scheduled_time,
                actual_timestamp
            )
        )

        print(
            f"[TIME DEBUG] "
            f"Stop={stop_name} "
            f"Scheduled={scheduled_time} "
            f"Actual={actual_time}"
        )

        actual_ref = db.reference(
            f"journeys/{bus_id}/{journey_key}/actualTimes/{stop_key}"
        )
        actual_ref.set({
            "scheduled": scheduled_time,
            "actual": actual_time,
            "actualTimestamp": actual_timestamp,
            "delayMinutes": delay_minutes,
            "status": status,
            "recordedAt": actual_timestamp,
            "source": "gps_fallback"
        })
        recorded_stops[bus_id].add(stop_key)

        print(
            f"[STOP ARRIVED] "
            f"{stop_name}"
        )

        print(
            f"[DELAY] "
            f"{delay_minutes} mins "
            f"{status}"
        )

        # Actual Arrival Auto-Fill if final stop reached
        if index == len(route_points) - 1:
            is_completed_in_mem = live_buses.get(bus_id, {}).get("trip_completed", False)
            if not is_completed_in_mem:
                actual_arrival_dt = datetime.now(IST).strftime("%Y-%m-%d %I:%M %p")
                actual_arrival_ts = int(time.time())
                print(f"[FINAL STOP REACHED] Recording actual arrival: {actual_arrival_dt}")
                
                db.reference(f"journeys/{bus_id}/{journey_key}/metadata").update({
                    "actualArrival": actual_arrival_dt,
                    "actualArrivalTimestamp": actual_arrival_ts,
                    "status": "COMPLETED"
                })
                
                db.reference(f"activeJourneys/{bus_id}").update({
                    "status": "COMPLETED",
                    "completedAt": actual_arrival_ts
                })
                
                if bus_id in live_buses:
                    live_buses[bus_id]["trip_completed"] = True

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
    
    # Initialize active journey metadata on connect
    journey_key = None
    try:
        buses = buses_cache or db.reference("buses").get() or {}
        matched_bus = None
        for _, bus in buses.items():
            link = bus.get("link")
            if link:
                extracted = extract_service_no_from_url(link)
                if extracted and extracted.upper() == service_no.upper():
                    matched_bus = bus
                    break
            operator = (
                bus.get("op", "")
                .split()[0]
                .upper()
            )
            if operator and operator in service_no.upper():
                matched_bus = bus
                break
        departure_time_str = ""
        if matched_bus:
            departure_time_str = matched_bus.get("time") or matched_bus.get("departureTime", "")
        journey_key = init_journey_metadata(service_no, doj, departure_time_str, matched_bus)
        
        if service_no not in live_buses:
            live_buses[service_no] = {}
        live_buses[service_no]["journeyKey"] = journey_key
    except Exception as e:
        print(f"[WEBSOCKET INIT ERROR] {e}")

    while True:
        try:
            async with websockets.connect(
                endpoint,
                ping_interval=30,
                ping_timeout=10
            ) as websocket:
                print(f"[CONNECTED] {service_no}")
                print(
                    f"[SUBSCRIBE] "
                    f"service={service_no} "
                    f"doj={doj}"
                )
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
                        print(f"[RAW WS] {message[:1000]}")

                        msg_data = json.loads(message)

                        print(f"[WS KEYS] {list(msg_data.keys())}")

                        # ========== PROVIDER POSITIONS LOGGING ==========
                        positions = msg_data.get("positions", [])
                        if not positions:
                            positions = msg_data.get("vehicleInfo", {}).get("positions", [])

                        if positions:
                            print("\n========== PROVIDER POSITIONS ==========")
                            for pos in positions:
                                print(
                                    f"STOP={pos.get('name')} | "
                                    f"STATUS={pos.get('status')} | "
                                    f"SCHEDULE={pos.get('scheduleTime')} | "
                                    f"ETA={pos.get('estimatedTime')}"
                                )
                            print("========================================\n")
                        else:
                            print(f"[PROVIDER] No positions array in message")
                        # ================================================

                        # Robust GPS extraction from various possible structures
                        vehicle_info = msg_data.get("vehicleInfo") or {}
                        if not isinstance(vehicle_info, dict):
                            vehicle_info = {}
                        position = vehicle_info.get("position") or msg_data.get("position") or {}
                        if not isinstance(position, dict):
                            position = {}
                            
                        lat = position.get("latitude") or position.get("lat")
                        lng = position.get("longitude") or position.get("lng")
                        
                        if lat is None or lng is None:
                            lat = vehicle_info.get("latitude") or vehicle_info.get("lat")
                            lng = vehicle_info.get("longitude") or vehicle_info.get("lng")
                            
                        if lat is None or lng is None:
                            lat = msg_data.get("latitude") or msg_data.get("lat")
                            lng = msg_data.get("longitude") or msg_data.get("lng")
                            
                        if lat is None or lng is None:
                            # Try nested location dicts
                            for loc_key in ["location", "vehicleLocation", "lastLocation", "liveTracking"]:
                                loc = msg_data.get(loc_key) or vehicle_info.get(loc_key) or position.get(loc_key)
                                if isinstance(loc, dict):
                                    lat = loc.get("lat") or loc.get("latitude")
                                    lng = loc.get("lng") or loc.get("longitude")
                                    if lat is not None and lng is not None:
                                        break

                        print(f"[GPS EXTRACTED] lat={lat} lng={lng}")
                        
                        veh_no = vehicle_info.get("vehicleNumber") or vehicle_info.get("vehicleNum") or default_vehicle_no or ""
                        existing = live_buses.get(service_no, {})
                        
                        lat_val = float(lat) if lat is not None else existing.get("lat")
                        lng_val = float(lng) if lng is not None else existing.get("lng")
                        status_val = "live" if lat_val is not None else "waiting"
                        
                        live_buses[service_no] = {
                            "lat": lat_val,
                            "lng": lng_val,
                            "serviceNo": service_no,
                            "vehicleNo": veh_no,
                            "operator": op_name,
                            "lastSeenTime": datetime.now(),
                            "last_seen": time.time(),
                            "status": status_val,
                            "journeyKey": journey_key,
                            "trip_completed": existing.get("trip_completed", False),
                            "nextStop": existing.get("nextStop"),
                            "lastPassedStop": existing.get("lastPassedStop"),
                            "providerStopCount": existing.get("providerStopCount", 0)
                        }
                        
                        if lat is not None and lng is not None:
                            print(f"[MARKER UPDATE] GPS update: {service_no} => {lat}, {lng}")
                            
                            # Run GPS-based route stop detection if we have coordinates
                            await process_stop_detection(
                                service_no,
                                float(lat),
                                float(lng),
                                doj
                            )
                            
                            # Update RAM cache for live-tracking endpoint from GPS messages
                            updated_existing = live_buses.get(service_no, {})
                            live_bus_locations[service_no] = {
                                "lat": float(lat),
                                "lng": float(lng),
                                "currentStop": updated_existing.get("lastPassedStop") or "",
                                "nextStop": updated_existing.get("nextStop") or "",
                                "lastSeen": int(time.time())
                            }
                        
                        # === PROVIDER STOP DETECTION (PRIMARY) ===
                        positions = msg_data.get("positions", [])
                        if not positions:
                            positions = vehicle_info.get("positions", [])
                            
                        if positions and len(positions) > 0:
                            lat_val = float(lat) if lat is not None else None
                            lng_val = float(lng) if lng is not None else None
                            await process_provider_stops(
                                bus_id=service_no,
                                positions=positions,
                                journey_date=doj,
                                journey_key=journey_key,
                                departure_time_str=departure_time_str,
                                lat=lat_val,
                                lng=lng_val
                            )
                            
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
    last_status_check = 0
    while True:
        try:
            now = time.time()
            if now - last_status_check >= 900:
                update_journey_status()
                last_status_check = now
            remove_buses = []

            for bus_id, bus_data in list(live_buses.items()):
                last_seen = bus_data.get("last_seen")
                if not last_seen:
                    bus_data["last_seen"] = now
                    continue

                inactive_time = now - last_seen

                if inactive_time > STALE_TIMEOUT:
                    print(
                        f"[SCHEDULER] "
                        f"{bus_id} inactive "
                        f"for "
                        f"{inactive_time:.1f}s "
                        f"(>{STALE_TIMEOUT}s)"
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

                # prevent memory leak in recorded_stops
                recorded_stops.pop(
                    bus_id,
                    None
                )

                # remove from initialized_journeys cache
                to_remove = [k for k in initialized_journeys if k.startswith(f"{bus_id}_")]
                for k in to_remove:
                    initialized_journeys.discard(k)

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
            global buses_cache
            print("[FIREBASE] Bus update detected")
            buses = ref.get() or {}
            buses_cache = buses

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
                    page_doj = page_data.get("doj")
                    active_ref = db.reference("activeJourneys")
                    active_journey = active_ref.child(service_no).get()

                    if active_journey:
                        recovered_doj = active_journey.get("journeyDate")

                        if recovered_doj == page_doj:
                            doj = recovered_doj
                        else:
                            print(
                                f"[DOJ MISMATCH] {service_no} "
                                f"Firebase={recovered_doj} "
                                f"Page={page_doj}"
                            )
                            doj = page_doj
                    else:
                        doj = page_doj

                    future = asyncio.run_coroutine_threadsafe(
                        websocket_listener(
                            service_no=page_data["serviceNo"] or service_no,
                            ws_url=page_data["ws_url"],
                            ws_port=page_data["ws_port"],
                            doj=doj,
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
                    if page_data["lat"] is not None and page_data["lng"] is not None:
                        live_bus_locations[service_no] = {
                            "lat": float(page_data["lat"]),
                            "lng": float(page_data["lng"]),
                            "currentStop": "",
                            "nextStop": "",
                            "lastSeen": int(time.time())
                        }

            # Stop tracking and remove memory logs for buses that are no longer configured/active in Firebase
            all_current_tracked = set(tracking_tasks.keys()) | set(live_buses.keys())
            for service_no in list(all_current_tracked):
                if service_no not in active_configured_services:
                    print(f"[TRACKING] Stopping tracking for {service_no} (removed/disabled in database)")
                    if service_no in tracking_tasks:
                        try:
                            tracking_tasks[service_no].cancel()
                        except Exception:
                            pass
                        tracking_tasks.pop(service_no, None)
                    live_buses.pop(service_no, None)
                    recorded_stops.pop(service_no, None)
                    try:
                        db.reference(f"activeJourneys/{service_no}").delete()
                        print(f"[TRACKING] Deleted activeJourneys/{service_no} from Firebase")
                    except Exception as e:
                        print(f"[FIREBASE DELETE ERROR] Failed to delete activeJourneys/{service_no}: {e}")
                    to_remove = [k for k in initialized_journeys if k.startswith(f"{service_no}_")]
                    for k in to_remove:
                        initialized_journeys.discard(k)

            # Clean up activeJourneys database path for any service that is no longer configured
            try:
                active_journeys = db.reference("activeJourneys").get() or {}
                for service_no in list(active_journeys.keys()):
                    if service_no.upper() not in {s.upper() for s in active_configured_services}:
                        print(f"[TRACKING CLEANUP] Removing orphaned active journey {service_no} from Firebase")
                        db.reference(f"activeJourneys/{service_no}").delete()
            except Exception as e:
                print(f"[TRACKING CLEANUP ERROR] Failed to clean up activeJourneys: {e}")
        except Exception as e:
            print("[FIREBASE LISTENER ERROR]", str(e))

    print("[FIREBASE] Live listener started")
    ref.listen(on_change)

def start_firebase_active_journeys_listener():
    """
    Listen for activeJourneys updates and update active_journeys_cache in-memory.
    """
    ref = db.reference("activeJourneys")
    def on_change(event):
        global active_journeys_cache
        try:
            active_journeys_cache = ref.get() or {}
            print("[FIREBASE] Active journeys cache updated")
        except Exception as e:
            print("[FIREBASE LISTENER ERROR] Active journeys listener failed:", e)
    ref.listen(on_change)

print("[FIREBASE] Active journeys listener defined")

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

    threading.Thread(
        target=start_firebase_active_journeys_listener,
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
    
    configured_buses = buses_cache or db.reference("buses").get() or {}
    active_journeys = db.reference("activeJourneys").get() or {}
    
    # Determine active configured service numbers
    active_configured_services = set()
    for bus_key, bus in configured_buses.items():
        if is_tracking_enabled(bus):
            link = bus.get("link", "")
            if link:
                service_no = extract_service_no_from_url(link)
                if service_no:
                    active_configured_services.add(service_no.upper())
    
    all_bus_ids = set(live_buses.keys()) | set(active_journeys.keys())
    # Filter to only keep active configured services
    all_bus_ids = {bid for bid in all_bus_ids if bid.upper() in active_configured_services}

    for bus_id in all_bus_ids:
        data = live_buses.get(bus_id)
        active_info = active_journeys.get(bus_id) or {}
        
        # Determine status, coordinates, and last updated time
        status = "waiting"
        lat = None
        lng = None
        last_updated_str = "Waiting for GPS"
        
        if data:
            lat = data.get("lat")
            lng = data.get("lng")
            status = data.get("status", "waiting")
            last_seen = data.get("lastSeenTime")
            if last_seen:
                diff = int((now - last_seen).total_seconds())
                last_updated_str = f"{diff} sec ago" if diff < 60 else f"{diff // 60} min ago"
        elif active_info:
            status_map = {
                "WAITING": "waiting",
                "LIVE": "offline",
                "COMPLETED": "offline"
            }
            status = status_map.get(active_info.get("status"), "waiting")
            
        matched_bus = {}
        for k, b in configured_buses.items():
            link = b.get("link", "")
            if link:
                extracted = extract_service_no_from_url(link)
                if extracted and extracted.upper() == bus_id.upper():
                    matched_bus = b
                    break

        formatted[bus_id] = {
            "lat": lat,
            "lng": lng,
            "status": status,
            "lastUpdated": last_updated_str,
            "operator": data.get("operator") if data else matched_bus.get("op", ""),
            "op": matched_bus.get("op", matched_bus.get("operator", data.get("operator") if data else "")),
            "route": matched_bus.get("route", ""),
            "time": matched_bus.get("time", matched_bus.get("departureTime", "")),
            "arrivalTime": matched_bus.get("arrivalTime", ""),
            "intermediateStops": matched_bus.get("intermediateStops", []),
            "stopTimes": matched_bus.get("stopTimes", []),
            "type": matched_bus.get("type", ""),
            "nextStop": data.get("nextStop") if data else active_info.get("nextStop"),
            "lastPassedStop": data.get("lastPassedStop") if data else active_info.get("lastPassedStop"),
            "providerStopCount": data.get("providerStopCount") if data else 0,
        }

    return formatted

@app.get("/api/live-track/{bus_id}")
async def live_track(bus_id: str):
    import urllib.parse
    bus_id_clean = urllib.parse.unquote(bus_id).replace("%20", " ").strip().upper()
    
    # Case-insensitive RAM lookups with space & encoding normalization
    data = None
    for k, v in live_bus_locations.items():
        k_clean = urllib.parse.unquote(k).replace("%20", " ").strip().upper()
        if k_clean == bus_id_clean:
            data = v
            break

    active_info = None
    for k, v in active_journeys_cache.items():
        k_clean = urllib.parse.unquote(k).replace("%20", " ").strip().upper()
        if k_clean == bus_id_clean:
            active_info = v
            break
            
    if not active_info:
        active_info = db.reference(f"activeJourneys/{bus_id_clean}").get() or {}
        if not active_info and "%" in bus_id:
            unquoted_id = urllib.parse.unquote(bus_id).strip()
            active_info = db.reference(f"activeJourneys/{unquoted_id}").get() or {}
        
    provider_stops = []
    if active_info:
        journey_key = active_info.get("journeyKey")
        if journey_key:
            provider_stops = db.reference(f"journeys/{active_info.get('serviceNo') or bus_id_clean}/{journey_key}/providerStops").get() or []
            
    print("LIVE DATA:", data)
    if not data:
        return {
            "lat": None,
            "lng": None,
            "currentStop": active_info.get("lastPassedStop"),
            "nextStop": active_info.get("nextStop"),
            "lastSeen": None,
            "providerStops": provider_stops
        }
    return {
        "lat": data.get("lat"),
        "lng": data.get("lng"),
        "currentStop": data.get("currentStop") or active_info.get("lastPassedStop"),
        "nextStop": data.get("nextStop") or active_info.get("nextStop"),
        "lastSeen": data.get("lastSeen"),
        "providerStops": provider_stops
    }

@app.get("/api/route-points/{bus_id}")
async def route_points_endpoint(bus_id: str):
    import urllib.parse
    bus_id_clean = urllib.parse.unquote(bus_id).replace("%20", " ").strip().upper()
    
    buses = buses_cache or db.reference("buses").get() or {}
    matched_bus = None
    for _, bus in buses.items():
        link = bus.get("link")
        if link:
            extracted = extract_service_no_from_url(link)
            if extracted:
                extracted_clean = urllib.parse.unquote(extracted).replace("%20", " ").strip().upper()
                if extracted_clean == bus_id_clean:
                    matched_bus = bus
                    break
        operator = bus.get("op", "").split()[0].upper()
        if operator and operator in bus_id_clean:
            matched_bus = bus
            break
            
    if not matched_bus:
        return []
        
    route = matched_bus.get("route", "")
    if not route:
        return []
        
    route_key = route.lower().replace(" - ", "___").replace(" ", "_")
    points = db.reference(f"routePoints/{route_key}").get() or []
    
    formatted_points = []
    for pt in points:
        formatted_points.append({
            "name": pt.get("name", ""),
            "lat": pt.get("lat"),
            "lng": pt.get("lng")
        })
    return formatted_points

@app.get("/api/actual-times")
async def actual_times(bus_id: str = None):
    if bus_id:
        new_data = db.reference(f"journeys/{bus_id}").get()
        if new_data and isinstance(new_data, dict):
            # Format the output to look like { journey_key: actualTimes }
            formatted = {}
            for j_key, j_val in new_data.items():
                if isinstance(j_val, dict) and "actualTimes" in j_val:
                    formatted[j_key] = j_val["actualTimes"]
            if formatted:
                return formatted
                
        old_data = db.reference(f"actualTimes/{bus_id}").get()
        return old_data or {}

    # Unified fallback if no bus_id: merge journeys and old actualTimes
    old_actual = db.reference("actualTimes").get() or {}
    journeys = db.reference("journeys").get() or {}
    
    merged = {}
    for b_id, dates in old_actual.items():
        if isinstance(dates, dict):
            merged[b_id] = {}
            for d, stops in dates.items():
                if isinstance(stops, dict):
                    merged[b_id][d] = dict(stops)
                
    for b_id, journey_keys in journeys.items():
        if not isinstance(journey_keys, dict):
            continue
        if b_id not in merged:
            merged[b_id] = {}
        for j_key, j_val in journey_keys.items():
            if isinstance(j_val, dict) and "actualTimes" in j_val:
                merged[b_id][j_key] = j_val["actualTimes"]
                
    return merged

@app.get("/api/journey-status")
async def journey_status(bus_id: str):
    # check activeJourneys first
    active = active_journeys_cache.get(bus_id)
    if active and isinstance(active, dict) and active.get("journeyKey"):
        journey_key = active["journeyKey"]
        status = active.get("status", "LIVE")
        journey_date = active.get("journeyDate")
        
        meta_ref = db.reference(f"journeys/{bus_id}/{journey_key}/metadata")
        meta = meta_ref.get() or {}
        return {
            "busId": bus_id,
            "journeyKey": journey_key,
            "departureDate": journey_date,
            "status": status,
            "actualArrival": meta.get("actualArrival")
        }
        
    # Fallback: if not in activeJourneys, look at the latest journey in journeys/{bus_id}
    journeys_ref = db.reference(f"journeys/{bus_id}")
    journeys = journeys_ref.get()
    if journeys and isinstance(journeys, dict):
        latest_key = sorted(journeys.keys())[-1]
        latest_meta = journeys[latest_key].get("metadata", {})
        return {
            "busId": bus_id,
            "journeyKey": latest_key,
            "departureDate": latest_meta.get("departureDate"),
            "status": latest_meta.get("status", "COMPLETED"),
            "actualArrival": latest_meta.get("actualArrival")
        }
        
    return {
        "busId": bus_id,
        "journeyKey": None,
        "departureDate": None,
        "status": "UNKNOWN",
        "actualArrival": None
    }

@app.get("/api/history")
async def history():
    ref = db.reference("busHistory")
    return ref.get() or {}
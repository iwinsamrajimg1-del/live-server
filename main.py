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
import logging
from stop_detector import is_stop_reached

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("winbus-tracker")


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

    cache_key = get_journey_cache_key(bus_id, journey_key)
    existing = recorded_stops.setdefault(cache_key, set())

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
        
        existing.add(stop_key)

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

def is_stop_name_match(name1, name2):
    if not name1 or not name2:
        return False
    def normalize(name):
        n = name.lower()
        n = re.sub(r'[\s_\-\(\)]+', '', n)
        n = re.sub(r'busstand|metro|omni|bypass|tollgate|junction|toll|village|city|terminal|terminus', '', n)
        n = n.replace('palli', 'palli').replace('pali', 'palli')
        n = n.replace('poram', 'puram').replace('puram', 'puram')
        return n
    n1 = normalize(name1)
    n2 = normalize(name2)
    return n1 == n2 or n1 in n2 or n2 in n1

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

def find_configured_bus(bus_id, buses):
    target = str(bus_id or "").strip().upper()
    for bus in (buses or {}).values():
        if not isinstance(bus, dict):
            continue
        extracted = extract_service_no_from_url(bus.get("link"))
        if extracted and extracted.strip().upper() == target:
            return bus
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

def get_route_key(route):
    if not route:
        return ""
    clean_route = re.sub(r'[^a-z0-9]', '', route.lower())
    try:
        route_keys = db.reference("routePoints").get(shallow=True) or {}
        for r_key in route_keys:
            clean_key = re.sub(r'[^a-z0-9]', '', r_key.lower())
            if clean_key == clean_route:
                return r_key
    except Exception as e:
        print(f"[ROUTE KEY MATCH ERROR] {e}")
    return route.lower().replace(" - ", "___").replace(" ", "_")

def get_journey_ref(bus_id, journey_key):
    return db.reference(f"journeys/{bus_id}/{journey_key}")

def get_journey_cache_key(bus_id, journey_key):
    return f"{bus_id}::{journey_key}"

def finalize_journey(bus_id, journey_key, journey_date, completed_at=None, final_stop_reached=False, last_passed=None, final_stop_name=None, status_override=None):
    """Archive and reset a completed journey with one Firebase update.

    status_override: if provided, written as the final metadata/archive status.
        Pass "STALE" when the tracking window expired before the final stop was
        confirmed (so the frontend can distinguish lost-tracking from a genuine
        trip completion).  When None the status is derived from final_stop_reached:
        True -> "COMPLETED", False -> "STALE".
    """
    completed_at = int(completed_at or time.time())
    actual_arrival_dt = datetime.fromtimestamp(completed_at, IST)
    actual_arrival = actual_arrival_dt.strftime("%Y-%m-%d %I:%M %p")

    # Resolve names if not passed
    if last_passed is None or final_stop_name is None:
        buses = buses_cache or db.reference("buses").get() or {}
        matched_bus = find_configured_bus(bus_id, buses)
        if matched_bus:
            stops = matched_bus.get("intermediateStops") or matched_bus.get("stopTimes") or []
            if stops:
                if final_stop_name is None:
                    final_stop = stops[-1]
                    final_stop_name = final_stop.get("name") or final_stop.get("stopName") or ""
        if last_passed is None:
            if bus_id in live_buses:
                last_passed = live_buses[bus_id].get("lastPassedStop")
            if not last_passed:
                active_info = db.reference(f"activeJourneys/{bus_id}").get() or {}
                last_passed = active_info.get("lastPassedStop") or ""

    if last_passed and final_stop_name and is_stop_name_match(last_passed, final_stop_name):
        final_stop_reached = True

    # Determine the status to persist.  An explicit override takes precedence;
    # otherwise use COMPLETED only when the final stop was actually reached, and
    # STALE when tracking ended before the destination was confirmed.
    if status_override:
        final_status = status_override
    else:
        final_status = "COMPLETED" if final_stop_reached else "STALE"

    journey_ref = get_journey_ref(bus_id, journey_key)
    journey_data = journey_ref.get() or {}
    metadata = dict(journey_data.get("metadata") or {})
    metadata.update({
        "departureDate": journey_date,
        "status": final_status,
        "actualArrival": actual_arrival,
        "actualArrivalTimestamp": completed_at,
        "completedAt": completed_at,
        "finalStopReached": final_stop_reached,
        "lastPassedStop": last_passed or "",
        "finalStopName": final_stop_name or ""
    })

    archive = dict(journey_data)
    archive["metadata"] = metadata
    archive["journeyKey"] = journey_key
    archive["journeyDate"] = journey_date
    archive["status"] = final_status
    archive["completedAt"] = completed_at

    db.reference().update({
        f"journeys/{bus_id}/{journey_key}/metadata": metadata,
        f"busHistory/{bus_id}/{journey_key}": archive,
        f"activeJourneys/{bus_id}": None,
    })

    active_journeys_cache.pop(bus_id, None)
    if bus_id in live_buses:
        live_buses[bus_id]["trip_completed"] = True
        live_buses[bus_id]["status"] = final_status.lower()
        live_buses[bus_id]["nextStop"] = None
    live_bus_locations.pop(bus_id, None)

    # Standard logger statement
    logger.info(
        f"Journey finalized: {bus_id} status={final_status} "
        f"lastPassed={last_passed} "
        f"finalStop={final_stop_name} "
        f"finalStopReached={final_stop_reached}"
    )
    print(f"[JOURNEY FINALIZED] {bus_id} ({journey_key}) status={final_status} archived and reset")

def update_journey_status():
    """
    Updates status of active journeys and handles 24h cleanup.
    """
    now = datetime.now(IST)
    buses = buses_cache or {}
    
    active_ref = db.reference("activeJourneys")
    active_journeys = active_journeys_cache or {}
    
    for bus_id, active_info in list(active_journeys.items()):
        if not isinstance(active_info, dict):
            continue
        journey_date = active_info.get("journeyDate")
        journey_key = active_info.get("journeyKey")
        status = active_info.get("status")
        completed_at = active_info.get("completedAt")
        
        if not journey_date or not journey_key:
            continue
            
        # Old deployments left completed records under activeJourneys.
        if status == "COMPLETED":
            finalize_journey(
                bus_id,
                journey_key,
                journey_date,
                completed_at=completed_at,
            )
            continue
                
        # Find matching configured bus to calculate status
        matched_bus = find_configured_bus(bus_id, buses)
                
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
                
            # Resolve final stop name and check if it is reached
            final_stop_reached = False
            final_stop_name = ""
            stops = matched_bus.get("intermediateStops") or matched_bus.get("stopTimes") or []
            last_passed = active_info.get("lastPassedStop") or ""
            if stops:
                final_stop = stops[-1]
                final_stop_name = final_stop.get("name") or final_stop.get("stopName") or ""
                if last_passed and final_stop_name and is_stop_name_match(last_passed, final_stop_name):
                    final_stop_reached = True

            if now < (departure_dt - timedelta(hours=1)):
                new_status = "NOT_STARTED"
            elif (departure_dt - timedelta(hours=1)) <= now <= (arrival_dt + timedelta(hours=12)):
                if final_stop_reached:
                    new_status = "COMPLETED"
                else:
                    new_status = "LIVE"
            else:
                # Arrival window exceeded.  Only mark COMPLETED when the final
                # stop was actually reached; otherwise use STALE so the frontend
                # can distinguish "tracking lost" from a genuine trip completion.
                if final_stop_reached:
                    new_status = "COMPLETED"
                else:
                    new_status = "STALE"

            if new_status in ("COMPLETED", "STALE"):
                finalize_journey(
                    bus_id,
                    journey_key,
                    journey_date,
                    final_stop_reached=final_stop_reached,
                    last_passed=last_passed,
                    final_stop_name=final_stop_name,
                    status_override=new_status,
                )
            elif new_status != status:
                active_ref.child(bus_id).update({"status": new_status})
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

                # Resolve final stop name and check if it is reached
                final_stop_reached = False
                final_stop_name = ""
                stops = matched_bus.get("intermediateStops") or matched_bus.get("stopTimes") or []
                active_info = active_journeys_cache.get(bus_id)
                if not active_info:
                    active_info = db.reference(f"activeJourneys/{bus_id}").get() or {}
                last_passed = active_info.get("lastPassedStop") or ""
                if stops:
                    final_stop = stops[-1]
                    final_stop_name = final_stop.get("name") or final_stop.get("stopName") or ""
                    if last_passed and final_stop_name and is_stop_name_match(last_passed, final_stop_name):
                        final_stop_reached = True

                if now < (departure_dt - timedelta(hours=1)):
                    status = "NOT_STARTED"
                elif (departure_dt - timedelta(hours=1)) <= now <= (arrival_dt + timedelta(hours=12)):
                    if final_stop_reached:
                        status = "COMPLETED"
                    else:
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
        if (
            active_existing.get("journeyKey") == journey_key
            and active_existing.get("journeyDate") == journey_date
            and active_existing.get("status") == status
            and not active_existing.get("completedAt")
        ):
            write_active = False
            
    if write_active:
        active_payload = {
            "journeyDate": journey_date,
            "journeyKey": journey_key,
            "status": status
        }
        if status == "COMPLETED":
            finalize_journey(bus_id, journey_key, journey_date)
        else:
            # set() clears stale completedAt/stop fields from the previous journey.
            active_ref.set(active_payload)
    
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

        if not departure_time or not scheduled_time:
            return None, "Unknown"
        
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

        # Pick the nearest daily occurrence. This prevents a stop just before its
        # scheduled minute from becoming a 1439-minute delay around midnight.
        candidates = [
            scheduled_dt - timedelta(days=1),
            scheduled_dt,
            scheduled_dt + timedelta(days=1),
        ]
        scheduled_dt = min(
            candidates,
            key=lambda candidate: abs((actual_dt - candidate).total_seconds()),
        )

        delay_minutes = round((actual_dt - scheduled_dt).total_seconds() / 60)
        if abs(delay_minutes) > 720:
            return None, "Unknown"
        
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

async def process_provider_stops(bus_id, positions, journey_date, journey_key, departure_time_str, lat=None, lng=None):
    if not positions:
        return

    if live_buses.get(bus_id, {}).get("trip_completed"):
        return

    # 1. Initialize caches for this bus
    cache_key = get_journey_cache_key(bus_id, journey_key)
    if cache_key not in provider_stop_statuses:
        provider_stop_statuses[cache_key] = {}
        
    if cache_key not in recorded_stops:
        try:
            actual_times_existing = db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes").get() or {}
            recorded_stops[cache_key] = set(actual_times_existing.keys())
        except Exception as e:
            print(f"[CACHE STOP ERROR] {e}")
            recorded_stops[cache_key] = set()

    # 2. Check if providerStops exists in Firebase, if not, auto build
    journey_init_key = f"{bus_id}_{journey_key}_providerStops"
    if journey_init_key not in initialized_journeys:
        auto_build_route_from_provider(bus_id, positions, journey_key)
        initialized_journeys.add(journey_init_key)

    # 3. Analyze provider positions to extract current stop, next stop, and upcoming stops
    last_passed = None
    next_stop = None
    upcoming_stops = []
    
    # Find next_stop (either status == "NEXT" or first stop not passed/crossed)
    next_index = -1
    for i, pos in enumerate(positions):
        status = pos.get("status", "").upper()
        if status == "NEXT":
            next_stop = pos.get("name")
            next_index = i
            break
            
    if next_stop is None:
        for i, pos in enumerate(positions):
            status = pos.get("status", "").upper()
            if status not in ["PASSED", "CROSSED", "MISSED"]:
                next_stop = pos.get("name")
                next_index = i
                break

    # Find last_passed (the latest stop with passed/crossed/missed status)
    for pos in positions:
        status = pos.get("status", "").upper()
        if status in ["PASSED", "CROSSED", "MISSED"]:
            last_passed = pos.get("name")

    if last_passed is None and next_index > 0:
        last_passed = positions[next_index - 1].get("name")

    # Collect upcoming stops (all stops after next_index)
    if next_index != -1:
        for i in range(next_index + 1, len(positions)):
            name = positions[i].get("name")
            if name:
                upcoming_stops.append(name)

    # Update in-memory live_buses details
    if bus_id in live_buses:
        if last_passed:
            live_buses[bus_id]["lastPassedStop"] = last_passed
        if next_stop:
            live_buses[bus_id]["nextStop"] = next_stop
        live_buses[bus_id]["upcomingStops"] = upcoming_stops
        live_buses[bus_id]["providerStopCount"] = len(positions)
        
        # Update activeJourneys in Firebase
        try:
            update_data = {}
            if last_passed:
                update_data["lastPassedStop"] = last_passed
            if next_stop:
                update_data["nextStop"] = next_stop
            # Always update upcomingStops
            update_data["upcomingStops"] = upcoming_stops
            db.reference(f"activeJourneys/{bus_id}").update(update_data)
            print(f"[PROVIDER MAP UPDATE] {bus_id} | Current={last_passed} Next={next_stop} Upcoming={upcoming_stops}")
        except Exception as e:
            print(f"[FIREBASE UPDATE ERROR] activeJourneys update failed: {e}")

# ======================================================
# STOP DETECTION
# ======================================================

async def process_stop_detection(
    bus_id,
    lat,
    lng,
    journey_date
):
    if live_buses.get(bus_id, {}).get("trip_completed"):
        return

    buses = buses_cache or db.reference("buses").get() or {}

    matched_bus = find_configured_bus(bus_id, buses)

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
    cache_key = get_journey_cache_key(bus_id, journey_key)
    if cache_key not in recorded_stops:
        try:
            actual_times_existing = db.reference(f"journeys/{bus_id}/{journey_key}/actualTimes").get() or {}
            recorded_stops[cache_key] = set(actual_times_existing.keys())
        except Exception as e:
            print(f"[CACHE STOP ERROR] {e}")
            recorded_stops[cache_key] = set()

    route = matched_bus.get(
        "route",
        ""
    )

    route_key = get_route_key(route)

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

        # We no longer update lastPassedStop, nextStop, or activeJourneys progress indicators
        # here to keep live map tracking completely isolated from actual timing detection.

        if stop_key in recorded_stops[cache_key]:
            print(
                "[SKIP] already recorded (RAM cache)"
            )
            if index == len(route_points) - 1:
                finalize_journey(
                    bus_id,
                    journey_key,
                    journey_date,
                    final_stop_reached=True,
                    status_override="COMPLETED",
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
        recorded_stops[cache_key].add(stop_key)

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
                actual_arrival_ts = int(time.time())
                print(f"[FINAL STOP REACHED] Completing {bus_id}")
                finalize_journey(
                    bus_id,
                    journey_key,
                    journey_date,
                    completed_at=actual_arrival_ts,
                    final_stop_reached=True,
                    status_override="COMPLETED",
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
    
    # Initialize active journey metadata on connect
    journey_key = None
    try:
        buses = buses_cache or db.reference("buses").get() or {}
        matched_bus = find_configured_bus(service_no, buses)
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
                                "upcomingStops": updated_existing.get("upcomingStops") or [],
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
                for key in [k for k in recorded_stops if k.startswith(f"{bus_id}::")]:
                    recorded_stops.pop(key, None)
                for key in [k for k in provider_stop_statuses if k.startswith(f"{bus_id}::")]:
                    provider_stop_statuses.pop(key, None)

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
                            "upcomingStops": [],
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
                    for key in [k for k in recorded_stops if k.startswith(f"{service_no}::")]:
                        recorded_stops.pop(key, None)
                    for key in [k for k in provider_stop_statuses if k.startswith(f"{service_no}::")]:
                        provider_stop_statuses.pop(key, None)
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

        if data and data.get("trip_completed"):
            continue
        if active_info.get("status") == "COMPLETED":
            continue
        
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
                "NOT_STARTED": "waiting",
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
            "upcomingStops": active_info.get("upcomingStops", []),
            "lastSeen": None,
            "providerStops": provider_stops
        }
    return {
        "lat": data.get("lat"),
        "lng": data.get("lng"),
        "currentStop": data.get("currentStop") or active_info.get("lastPassedStop"),
        "nextStop": data.get("nextStop") or active_info.get("nextStop"),
        "upcomingStops": data.get("upcomingStops") or active_info.get("upcomingStops", []),
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
    if not matched_bus:
        return []
        
    route = matched_bus.get("route", "")
    if not route:
        return []
        
    route_key = get_route_key(route)
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
        return {}

    # Schema v2 source of truth. Legacy /actualTimes is intentionally excluded
    # because it is date-only and can overwrite the current journey in clients.
    journeys = db.reference("journeys").get() or {}
    merged = {}
    for b_id, journey_keys in journeys.items():
        if not isinstance(journey_keys, dict):
            continue
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

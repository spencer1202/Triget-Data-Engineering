import queue
import pandas as pd
import time
import datetime as dt
import numpy as np
import json
import psycopg2
import dotenv
import os
from pyproj import Transformer
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs

dotenv.load_dotenv()

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = "admin"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TABLE_NAME = "stop_event"

# -- Configuration ---------------------------------------------
PROJECT_ID      = 'triget-data-engineering'
SUBSCRIPTION_ID = 'se_analysis_sub'

# Debug settings
DEBUG_PRINT = False

# Number of worker threads processing message queue
NUM_THREADS = 4

# Maximum messages in internal message queue
MESSAGE_QUEUE_MAX = 0

# Flow control settings
flow_control = FlowControl(
        max_messages=1000,
        max_bytes=1000 * 1024 * 1024
)

# Coordinate transformer: EPSG:2913 (Oregon State Plane North, feet) -> WGS 84
_transformer = Transformer.from_crs("EPSG:2913", "EPSG:4326", always_xy=True)

# Tell psycopg2 how to handle numpy integers
def add_numpy_support():
    register_adapter(np.int64, AsIs)
    register_adapter(np.float64, AsIs)


# -- Statistics ------------------------------------------------
class Statistics:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first_record_wall_time       = None  # Wall-clock time when first stop event of the day is received
        self.first_record_timestamp       = None  # Human readable timestamp
        self.vehicle_ids                  = set() # Unique vehicle_number values
        self.trip_ids                     = set() # Unique trips (unique trip_number values)
        self.earliest_time                = None  # Earliest arrive_time across all records
        self.latest_time                  = None  # Latest arrive_time across all records
        self.total_stop_events            = 0     # Total number of stop events received
        self.invalid_stop_events          = 0     # Total number of invalid stop events dropped
        self.sentinel_received_timestamp  = None  # Timestamp of moment when sentinel is received
        self.total_time                   = None  # Elapsed wall-clock time from first record to sentinel
        self.throughput                   = None  # Analysis throughput (records per second)
        self.invalid_records              = 0     # Number of invalid records received
        self.stop_events_stored           = 0     # Number of stop events stored


    # Update running stats with one stop event
    def record_stop_event(self, payload):
        if self.first_record_wall_time is None:
            debug_print("Received first stop event.")
            self.first_record_wall_time = time.time()
            self.first_record_timestamp = time.ctime()

        self.total_stop_events += 1
        self.vehicle_ids.add(payload.get('vehicle_number'))
        self.trip_ids.add(payload.get('trip_number'))

        # Get stop event timestamp from service_date + arrive_time (seconds after midnight)
        try:
            service_date = payload.get('service_date', '')
            arrive_time = int(payload.get('arrive_time', 0))
            base = dt.datetime.strptime(service_date, '%Y-%m-%d')
            se_time = base + dt.timedelta(seconds=arrive_time)

            # Check if it's the earliest or latest time
            if self.earliest_time is None or se_time < self.earliest_time:
                self.earliest_time = se_time
            if self.latest_time is None or se_time > self.latest_time:
                self.latest_time = se_time

        except:
            debug_print("Malformed timestamp")


    # Calculate end-of-run stats
    def end_stats(self, invalid_record_count):
        self.sentinel_received_timestamp = time.ctime()
        if self.first_record_wall_time:
            self.total_time = time.time() - self.first_record_wall_time
            self.throughput = (self.total_stop_events / self.total_time) if self.total_time > 0 else 0
        self.invalid_records = invalid_record_count


    def report(self):
        print(f"--- SE Analysis Summary ---")
        print(f"First stop event timestamp:     {self.first_record_timestamp}")
        print(f"Earliest stop event timestamp:  {self.earliest_time}")
        print(f"Latest stop event timestamp:    {self.latest_time}")
        print(f"Sentinel message received at:   {self.sentinel_received_timestamp}")
        print(f"Unique vehicle IDs:             {len(self.vehicle_ids)}")
        print(f"Unique trips:                   {len(self.trip_ids)}")
        print(f"Stop events received:           {self.total_stop_events}")
        print(f"Invalid stop events dropped:    {self.invalid_stop_events}")
        print(f"Total elapsed time:             {self.total_time:.3f}")
        print(f"Throughput:                     {self.throughput:.3f} msg/s")
        print(f"Invalid record count:           {self.invalid_records}")
        print(f"Inserted into database:         {self.stop_events_stored}")
        print(f"---------------------------\n")


# -- Globals ---------------------------------------------------
stats           = Statistics()
stats_lock      = Lock()
sentinel_queue  = queue.Queue(maxsize=1)

# Output lists
valid_records           = []
invalid_records         = []

# Internal work queue: callback enqueues raw payloads; workers dequeue and process
message_queue = queue.Queue(maxsize=MESSAGE_QUEUE_MAX)

# -- Helper Functions ------------------------------------------
# Print only if DEBUG_PRINT option is true
def debug_print(val):
    if DEBUG_PRINT:
        print(val)


# Handle sentinel message
def handle_sentinel(payload):
    debug_print("Sentinel received!")

    # Wait until remaining messages are processed
    expected_count = payload.get('count')
    timeout = 30

    while timeout > 0:
        # Queue has been fully processed
        with stats_lock:
            current = stats.total_stop_events
        # Expected number of messages have been processed, proceed
        if current >= expected_count and message_queue.empty():
            break
        timeout -= 1
        time.sleep(1)

    # Enqueue poison pills to shut down worker threads
    for _ in range(NUM_THREADS):
        message_queue.put(None)

    message_queue.join()


# -- Validation ------------------------------------------------
# Validate stop events
def validate_stop_event(payload):
    errors = []

    required_fields = [
        'vehicle_number',
        'trip_number',
        'service_date',
        'arrive_time',
        'route_number',
        'x_coordinate',
        'y_coordinate'
    ]

    for field in required_fields:
        if field not in payload or payload[field] in [None, '']:
            errors.append(f"Missing required field: {field}")

    # vehicle_number must be a positive integer
    try:
        vehicle_num = int(payload.get('vehicle_number'))
        if vehicle_num <= 0:
            errors.append("vehicle_number must be positive")
    except:
        errors.append("vehicle_number is not numeric")

    # arrive_time must be in valid range (seconds after midnight)
    try:
        arrive_time = int(payload.get('arrive_time'))
        if arrive_time < 0 or arrive_time > 172800:    # allow up to 48 hours past midnight
            errors.append("arrive_time out of range")
    except:
        errors.append("arrive_time is not numeric")

    # leave_time must come at or after arrive_time (intra-record consistency)
    try:
        arrive_time = int(payload.get('arrive_time'))
        leave_time = int(payload.get('leave_time'))
        if leave_time < arrive_time:
            errors.append("leave_time is before arrive_time")
    except:
        pass

    # ons, offs, estimated_load must all be non-negative
    for field in ['ons', 'offs', 'estimated_load']:
        try:
            value = int(payload.get(field, 0))
            if value < 0:
                errors.append(f"{field} cannot be negative")
        except:
            errors.append(f"{field} is not numeric")

    # service_date must be a valid YYYY-MM-DD date
    try:
        dt.datetime.strptime(payload.get('service_date'), '%Y-%m-%d')
    except:
        errors.append("service_date is not a valid YYYY-MM-DD date")

    # x_coordinate and y_coordinate must be numeric and non-zero
    try:
        x = float(payload.get('x_coordinate'))
        y = float(payload.get('y_coordinate'))
        if x == 0.0 or y == 0.0:
            errors.append("x_coordinate or y_coordinate is zero")
    except:
        errors.append("x_coordinate or y_coordinate is not numeric")

    return len(errors) == 0, errors


# Write invalid records to json file
def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid stop event records to a dated JSON file.

    Parameters
    ----------
    invalid_records : list of dict
        Each dict should have a 'record' key (the original data)
        and a 'violations' key (list of assertion violation messages).
    run_date : str, optional
        Date string in YYYY-MM-DD format. Defaults to today.
    """
    if run_date is None:
        run_date = dt.date.today().isoformat()

    filename = f"invalid_se_data_{run_date}.json"

    with open(filename, "w") as f:
        json.dump(invalid_records, f, indent=2, default=str)

    debug_print(f"Wrote {len(invalid_records)} invalid records to {filename}")


# -- Transformation --------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Build full timestamps from service_date + seconds-after-midnight fields
    service_date = pd.to_datetime(df["service_date"], format="%Y-%m-%d")

    df["arrive_time"] = service_date + pd.to_timedelta(df["arrive_time"].astype(int), unit="s")
    df["leave_time"]  = service_date + pd.to_timedelta(df["leave_time"].astype(int), unit="s")
    df["stop_time"]   = service_date + pd.to_timedelta(df["stop_time"].astype(int), unit="s")

    # Convert Oregon State Plane coordinates (feet) to GPS lat/lon (WGS 84)
    x = df["x_coordinate"].astype(float).values
    y = df["y_coordinate"].astype(float).values
    lon, lat = _transformer.transform(x, y)
    df["GPS_latitude"]  = lat
    df["GPS_longitude"] = lon

    # Convert numeric string columns to proper types
    int_columns = ['vehicle_number', 'train', 'route_number', 'direction', 'trip_number',
                   'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
                   'maximum_speed', 'data_source', 'schedule_status']
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    float_columns = ['train_mileage', 'pattern_distance', 'location_distance']
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop the raw coordinate fields and service_date now that we've used them
    df = df.drop(columns=['x_coordinate', 'y_coordinate', 'service_date'])

    return df


# -- Process Worker --------------------------------------------
# Pulls payloads off shared queue and processes them
def process_worker():
    while True:
        payload = message_queue.get()

        if payload is None:
            message_queue.task_done()
            break

        with stats_lock:
            stats.record_stop_event(payload)

        # Validate stop event
        valid, errors = validate_stop_event(payload)

        if not valid:
            record = {
                "record": payload,
                "violations": errors
            }
            invalid_records.append(record)

        else:
            valid_records.append(payload)

        message_queue.task_done()


# -- Message Callback ------------------------------------------
def callback(message):
    try:
        payload = json.loads(message.data.decode('utf-8'))

    except Exception as ex:
        debug_print(f"Failed to decode message: {ex}")
        message.ack()
        return

    # Sentinel message received
    if payload.get('sentinel'):
        sentinel_queue.put(payload)
        message.ack()
        return

    message_queue.put(payload)
    message.ack()


# -- Database helper functions -----------------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def store_stop_events(df):
    if df.empty:
        return 0

    records = list(df[[
        "vehicle_number",
        "leave_time",
        "train",
        "route_number",
        "direction",
        "service_key",
        "trip_number",
        "stop_time",
        "arrive_time",
        "dwell",
        "location_id",
        "door",
        "lift",
        "ons",
        "offs",
        "estimated_load",
        "maximum_speed",
        "train_mileage",
        "pattern_distance",
        "location_distance",
        "GPS_latitude",
        "GPS_longitude",
        "data_source",
        "schedule_status"
    ]].itertuples(index=False, name=None))

    sql = """
        INSERT INTO stop_event
        (vehicle_number, leave_time, train, route_number, direction,
         service_key, trip_number, stop_time, arrive_time, dwell,
         location_id, door, lift, ons, offs, estimated_load,
         maximum_speed, train_mileage, pattern_distance, location_distance,
         gps_latitude, gps_longitude, data_source, schedule_status)
        VALUES %s
    """

    conn = get_connection()
    add_numpy_support()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, records)
    finally:
        conn.close()

    return len(records)


# -- Main ------------------------------------------------------
def main():
    global stats

    sub_path = SubscriberClient().subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    debug_print(f"Listening on: {SUBSCRIPTION_ID}")

    while True:
        valid_records.clear()
        invalid_records.clear()

        executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
        for _ in range(NUM_THREADS):
            executor.submit(process_worker)

        subscriber = SubscriberClient()
        with subscriber:
            streaming_pull = subscriber.subscribe(
                    sub_path,
                    callback=callback,
                    flow_control=flow_control,
            )
            debug_print("Waiting for stop events...")

            # Block to wait for sentinel event
            sentinel_payload = sentinel_queue.get()
            handle_sentinel(sentinel_payload)

        try:
            streaming_pull.result()
            streaming_pull.cancel()

        except Exception:
            pass

        # Wait for all workers to finish processing
        executor.shutdown(wait=False)
        debug_print("Finished receiving stop events. Validating data...\n")

        # Validation
        write_invalid_records(invalid_records)

        # Transformation
        df = pd.DataFrame(valid_records)
        df = transform(df)

        debug_print(df)

        # stop_event table should already exist
        inserted_count = store_stop_events(df)
        debug_print(f"Inserted {inserted_count} valid stop event records into PostgreSQL.")

        # Report stats
        with stats_lock:
            stats.stop_events_stored = inserted_count
            stats.end_stats(len(invalid_records))
            stats.report()
            stats.reset()

        debug_print("Finished processing. Stats reset.\n")


if __name__ == '__main__':
    main()
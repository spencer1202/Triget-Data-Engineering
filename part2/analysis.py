import queue
import pandas as pd
import time
import datetime as dt
import json
import psycopg2
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_values

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "trimetdb"
DB_USER = "trimetuser"
DB_PASSWORD = "trimetpassword"
TABLE_NAME = "breadcrumb"

# -- Configuration ---------------------------------------------
PROJECT_ID      = 'triget-data-engineering' 
SUBSCRIPTION_ID = 'analysis_sub'

# Debug settings
DEBUG_PRINT = False

# Number of worker threads processing message queue
NUM_THREADS = 4

# Maximum messages in internal message queue
MESSAGE_QUEUE_MAX = 0

# FLow control settings
flow_control = FlowControl(
        max_messages=1000,
        max_bytes=1000 * 1024 * 1024
)

# -- Statistics ------------------------------------------------
class Statistics:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first_breadcrumb_wall_time   = None  # Wall-clock time when first breadcrumb of the day is received
        self.first_breadcrumb_timestamp   = None  # Human readable timestamp
        self.vehicle_ids                  = set() # Unique vehicle_id values
        self.trip_ids                     = set() # Unique trips (unique EVENT_NO_TRIP values)
        self.earliest_time                = None  # Earliest breadcrumb timestamp (derived from OPD_DATE and ACT_TIME)
        self.latest_time                  = None  # Latest breadcrumb timestamp (derived from OPD_DATE and ACT_TIME)
        self.total_breadcrumbs            = 0     # Total number of breadcrumbs received
        self.sentinel_received_timestamp  = None  # Timestamp of moment when sentinel is received
        self.total_time                   = None  # Elapsed wall-clock time from moment when first breadcrumb of the 
                                                  # day is received until moment when the sentinel message is received.
        self.throughput                   = None  # Analysis Throughput (breadcrumbs per second)
        self.invalid_records              = 0     # Number of invalid records received


    # Update running stats with one breadcrumb
    def record_breadcrumb(self, payload):
        if self.first_breadcrumb_wall_time is None:
            debug_print("Received first breadcrumb.")
            self.first_breadcrumb_wall_time = time.time()
            self.first_breadcrumb_timestamp = time.ctime()
        
        self.total_breadcrumbs += 1
        self.vehicle_ids.add(payload.get('VEHICLE_ID'))
        self.trip_ids.add(payload.get('EVENT_NO_TRIP'))

        # Get breadcrumb timestamp
        try:
            opd_date = payload.get('OPD_DATE', '')
            act_time = int(payload.get('ACT_TIME', 0))
            base = dt.datetime.strptime(opd_date[:9], '%d%b%Y')
            bc_time = base + dt.timedelta(seconds=act_time)

          # Check if it's the earliest or latest time
            if self.earliest_time is None or bc_time < self.earliest_time:
                self.earliest_time = bc_time
            if self.latest_time is None or bc_time > self.latest_time:
                self.latest_time = bc_time
          
        except:
            debug_print("Malformed timestamp")

      
    # Calculate end-of-run stats
    def end_stats(self, invalid_record_count):
        self.sentinel_received_timestamp = time.ctime()
        if self.first_breadcrumb_wall_time:
            self.total_time = time.time() - self.first_breadcrumb_wall_time
            self.throughput = (self.total_breadcrumbs / self.total_time) if self.total_time > 0 else 0
        self.invalid_records = invalid_record_count


    def report(self):
        print(f"--- Analysis Summary ---")
        print(f"First breadcrumb timestamp:     {self.first_breadcrumb_timestamp}")
        print(f"Earliest breadcrumb timestamp:  {self.earliest_time}")
        print(f"Latest breadcrumb timestamp:    {self.latest_time}")
        print(f"Sentinel messages received at:  {self.sentinel_received_timestamp}")
        print(f"Unique vehicle IDs:             {len(self.vehicle_ids)}")
        print(f"Unique trips:                   {len(self.trip_ids)}")
        print(f"Breadcrumbs received:           {self.total_breadcrumbs}")
        print(f"Total elapsed time:             {self.total_time:.3f}")
        print(f"Throughput:                     {self.throughput:.3f} msg/s")
        print(f"Invalid record count:           {self.invalid_records}")
        print(f"------------------------\n")




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
            current = stats.total_breadcrumbs
        # Expected number of messages have been processed, proceed
        if current >= expected_count and message_queue.empty():
            break
        timeout -= 1
        time.sleep(1)

    # Enqueue poison pills to shut down worker threads
    for _ in range(NUM_THREADS):
        message_queue.put(None)
    
    message_queue.join()

    # Report stats
    with stats_lock:
        stats.end_stats(len(invalid_records))
        stats.report()


# -- Validation ------------------------------------------------
# Validate breadcrumbs
def validate_breadcrumb(payload):
    errors = []

    required_fields = [
        'EVENT_NO_TRIP',
        'VEHICLE_ID',
        'OPD_DATE',
        'ACT_TIME',
        'METERS',
        'GPS_LATITUDE',
        'GPS_LONGITUDE'
    ]

    for field in required_fields:
        if field not in payload or payload[field] in [None, '']:
            errors.append(f"Missing required field: {field}")

    try:
        lat = float(payload.get('GPS_LATITUDE'))
        if lat < -90 or lat > 90:
            errors.append("GPS_LATITUDE out of range")
    except:
        errors.append("GPS_LATITUDE is not numeric")

    try:
        lon = float(payload.get('GPS_LONGITUDE'))
        if lon < -180 or lon > 180:
            errors.append("GPS_LONGITUDE out of range")
    except:
        errors.append("GPS_LONGITUDE is not numeric")

    try:
        act_time = int(payload.get('ACT_TIME'))
        if act_time < 0 or act_time > 90000:
            errors.append("ACT_TIME out of range")
    except:
        errors.append("ACT_TIME is not numeric")

    try:
        meters = float(payload.get('METERS'))
        if meters < 0:
            errors.append("METERS cannot be negative")
    except:
        errors.append("METERS is not numeric")

    try:
        lat = float(payload.get('GPS_LATITUDE'))
        lon = float(payload.get('GPS_LONGITUDE'))

        if not (45.0 <= lat <= 46.0 and -123.5 <= lon <= -122.0):
            errors.append("GPS coordinates outside Portland metro area")
    except:
        pass

    return len(errors) == 0, errors


# Write invalid records to json file
def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid breadcrumb records to a dated JSON file.

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

    filename = f"invalid_data_{run_date}.json"

    with open(filename, "w") as f:
        json.dump(invalid_records, f, indent=2, default=str)

    debug_print(f"Wrote {len(invalid_records)} invalid records to {filename}")


# -- Transformation --------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    opd_date = pd.to_datetime(df["OPD_DATE"], format="%d%b%Y:%H:%M:%S")
    act_time = pd.to_timedelta(df["ACT_TIME"], unit="s")
    df["TIMESTAMP"] = opd_date + act_time

    df = df.sort_values(by=["EVENT_NO_TRIP", "TIMESTAMP"])

    delta_meters = df.groupby("EVENT_NO_TRIP")["METERS"].diff()
    delta_time = df.groupby("EVENT_NO_TRIP")["TIMESTAMP"].diff().dt.total_seconds()

    df["SPEED"] = delta_meters / delta_time
    df = df.sort_index()

    # Remove unneeded fields
    df = df.drop(columns=['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP', 'OPD_DATE', 'ACT_TIME'])

    col_names = {
        "EVENT_NO_TRIP" : "trip_id",
        "VEHICLE_ID"    : "vehicle_id",
        "GPS_LONGITUDE" : "longitude",
        "GPS_LATITUDE"  : "latitude"
    }
    df = df.rename(columns=col_names)

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
            stats.record_breadcrumb(payload)

        # Validate breadcrumb
        valid, errors = validate_breadcrumb(payload)

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


def create_breadcrumb_table():
    sql = """
        CREATE TABLE IF NOT EXISTS breadcrumb (
            id SERIAL PRIMARY KEY,
            trip_id BIGINT,
            vehicle_id INTEGER,
            timestamp TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            meters DOUBLE PRECISION,
            speed DOUBLE PRECISION
        );
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()


def store_breadcrumbs(df):
    if df.empty:
        return 0

    records = list(df[[
        "trip_id",
        "vehicle_id",
        "TIMESTAMP",
        "latitude",
        "longitude",
        "METERS",
        "SPEED"
    ]].itertuples(index=False, name=None))

    sql = """
        INSERT INTO breadcrumb
        (trip_id, vehicle_id, timestamp, latitude, longitude, meters, speed)
        VALUES %s
    """

    conn = get_connection()
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
            debug_print("Waiting for breadcrumbs...")

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
        debug_print("Finished recieveing breadcrumbs. Validating data...\n")

        # Validation
        write_invalid_records(invalid_records)

        # Transformation
        df = pd.DataFrame(valid_records)
        df = transform(df)

        debug_print(df)
        
        create_breadcrumb_table()
        inserted_count = store_breadcrumbs(df)
        print(f"Inserted {inserted_count} valid breadcrumb records into PostgreSQL.")

        with stats_lock:
            stats.reset()

        debug_print("Finished processing. Stats reset.\n")


if __name__ == '__main__':
    main()

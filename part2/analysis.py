import sys
import time
import datetime as dt
import json
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from threading import Event, Lock


# -- Configuration ---------------------------------------------
PROJECT_ID      = 'triget-data-engineering' 
SUBSCRIPTION_ID = 'analysis_sub'

# Debug settings
DEBUG_PRINT = False

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
    def end_stats(self):
        self.sentinel_received_timestamp = time.ctime()
        if self.first_breadcrumb_wall_time:
            self.total_time = time.time() - self.first_breadcrumb_wall_time
            self.throughput = (self.total_breadcrumbs / self.total_time) if self.total_time > 0 else 0


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
        print(f"------------------------\n")




# -- Globals ---------------------------------------------------
stats           = Statistics()
sentinel_event  = Event()
stats_lock      = Lock()


# -- Helper Functions ------------------------------------------
# Print only if DEBUG_PRINT option is true
def debug_print(val):
    if DEBUG_PRINT:
        #print(val)
        pass


# Handle sentinel message
def handle_sentinel(payload):
    debug_print("Sentinel received!")

    # Wait until remaining messages are processed
    expected_count = payload.get('count')
    timeout = 10
    while stats.total_breadcrumbs < expected_count and timeout > 0:
        timeout -= 1
        time.sleep(1)

    # Report stats
    with stats_lock:
        stats.end_stats()
        stats.report()

    sentinel_event.set()


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
        handle_sentinel(payload)
        message.ack()
        return

    with stats_lock:
        stats.record_breadcrumb(payload)

    message.ack()


# -- Main ------------------------------------------------------
def main():
    global stats

    sub_path = SubscriberClient().subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    debug_print(f"Listening on: {SUBSCRIPTION_ID}")
  
    while True:
        sentinel_event.clear()
        subscriber = SubscriberClient()

        with subscriber:
            streaming_pull = subscriber.subscribe(
                    sub_path, 
                    callback=callback,
                    flow_control=flow_control,
            )
            debug_print("Waiting for breadcrumbs...")

            # Block to wait for sentinel event
            sentinel_event.wait()
            streaming_pull.cancel()
            try:
                streaming_pull.result()
            except Exception:
                pass

        with stats_lock:
            stats.reset()
        debug_print("Finished processing. Stats reset.\n")


if __name__ == '__main__':
    main()

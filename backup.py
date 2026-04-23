import time
import datetime as dt
import json
import gzip
import os
import shutil
from threading import Event, Lock
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.pubsub_v1.subscriber import scheduler


# -- Configuration --------------------------------------------------------
PROJECT_ID      = 'triget-data-engineering'
SUBSCRIPTION_ID = 'backup_sub'

# Debug settings
DEBUG_PRINT = False

# Flow control settings
flow_control = FlowControl(
        max_messages=1000,
        max_bytes=100 * 1024 * 1024  # 100MB
)

# -- Statistics -----------------------------------------------------------
class Statistics:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first_breadcrumb_wall_time  = None  # Wall-clock time when first breadcrumb arrived
        self.first_breadcrumb_timestamp  = None  # Human-readable version of above
        self.vehicle_ids                 = set() # Unique vehicle_id values
        self.total_breadcrumbs           = 0     # Total number of breadcrumbs received
        self.bytes_received              = 0     # Size of backup file before compression
        self.compressed_timestamp        = None  # Timestamp when backup file is compressed
        self.total_time                  = None  # Elapsed time: first breadcrumb -> compression
        self.throughput                  = None  # Backup Throughput (breadcrumbs per second)


    # Update running stats with one breadcrumb
    def record_breadcrumb(self, breadcrumb, raw_bytes):
        if self.first_breadcrumb_wall_time is None:
            debug_print("Received first breadcrumb")
            self.first_breadcrumb_wall_time = time.time()
            self.first_breadcrumb_timestamp = time.ctime()

        self.total_breadcrumbs += 1
        self.vehicle_ids.add(breadcrumb.get('VEHICLE_ID'))
        self.bytes_received += raw_bytes


    # Calculate end-of-run stats
    def end_stats(self):
        self.compressed_timestamp = time.ctime()
        if self.first_breadcrumb_wall_time:
            self.total_time = time.time() - self.first_breadcrumb_wall_time
            self.throughput = (
                self.total_breadcrumbs / self.total_time if self.total_time > 0 else 0
            )

    def report(self):
        print("--- Backup Summary ---")
        print(f"First breadcrumb received at:   {self.first_breadcrumb_timestamp}")
        print(f"Compressed at:                  {self.compressed_timestamp}")
        print(f"Unique vehicle IDs:             {len(self.vehicle_ids)}")
        print(f"Breadcrumbs received:           {self.total_breadcrumbs}")
        print(f"Bytes received (pre-compress):  {self.bytes_received}")
        print(f"Total elapsed time:             {self.total_time:.3f}s")
        print(f"Throughput:                     {self.throughput:.1f} msg/s")
        print("----------------------\n")


# -- Globals --------------------------------------------------------------
stats           = Statistics()
sentinel_event  = Event()
stats_lock      = Lock()
file_lock       = Lock()
backup_file     = None      # backup file object, opened when first breadcrumb arrives
backup_filename = None      # name of today's log file


# -- Helper Functions -----------------------------------------------------
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
    while stats.total_breadcrumbs < expected_count and timeout > 0:
        timeout -= 1
        time.sleep(1)
    
    # Close and compress backup file
    with file_lock:
        if backup_file and not backup_file.closed:
            backup_file.close()
        compress_backup_file()

    # Report stats
    with stats_lock:
        stats.end_stats()
        stats.report()

    sentinel_event.set()
 

# Open today's backup file
def open_backup_file():
    global backup_file, backup_filename
    date_str = dt.datetime.now().strftime('%Y-%m-%d')
    backup_filename = f"breadcrumbs_{date_str}_.log"
    backup_file = open(backup_filename, 'w')
    debug_print(f"Opened backup file: {backup_filename}")


# Compress the backup file
def compress_backup_file():
    gz_filename = backup_filename + '.gz'

    with open(backup_filename, 'rb') as file_in:
        with gzip.open(gz_filename, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)
    try:
        os.remove(backup_filename)
    except OSError:
        pass

    debug_print(f"Compressed backup to: {gz_filename}")



# -- Message Callback -----------------------------------------------------
def callback(message):
    try:
        raw_bytes = message.data.decode('utf-8')
        payload = json.loads(raw_bytes)

    except Exception as ex:
        debug_print(f"Failed to decode message: {ex}")
        message.ack()
        return

    # Sentinel message received
    if payload.get('sentinel'):
        handle_sentinel(payload)
        message.ack()
        return

    # Write to backup file
    with file_lock:
        if backup_file is None:
            open_backup_file()
        line = raw_bytes + '\n'     # separate breadcrumbs with newline
        backup_file.write(line)

    with stats_lock:
        stats.record_breadcrumb(payload, len(line))

    message.ack()

    

# -- Main -----------------------------------------------------------------
def main():
    global stats, backup_file, backup_filename

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

            sentinel_event.wait()   # wait for sentinel to be receieved

            streaming_pull.cancel()
            try:
                streaming_pull.result()
            except Exception:
                pass

        # Reset for next day
        with stats_lock:
            stats.reset()
            backup_file = None
            backup_filename = None
        debug_print("Finished processing. Stats reset.\n")


if __name__ == '__main__':
    main()

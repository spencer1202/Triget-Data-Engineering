import time
import datetime as dt
import json
import gzip
import os
import shutil
import queue
from threading import Event, Lock
from concurrent.futures import ThreadPoolExecutor
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl


# -- Configuration --------------------------------------------------------
PROJECT_ID      = 'triget-data-engineering'
SUBSCRIPTION_ID = 'se_backup_sub'
MESSAGE_QUEUE_MAX = 0
NUM_THREADS = 5

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
        self.first_record_wall_time  = None  # Wall-clock time when first stop event arrived
        self.first_record_timestamp  = None  # Human-readable version of above
        self.vehicle_ids             = set() # Unique vehicle_number values
        self.total_stop_events       = 0     # Total number of stop events received
        self.bytes_received          = 0     # Size of backup file before compression
        self.compressed_timestamp    = None  # Timestamp when backup file is compressed
        self.total_time              = None  # Elapsed time: first record -> compression
        self.throughput              = None  # Backup throughput (records per second)

    # Update running stats with one stop event record
    def record_event(self, record, raw_bytes):
        if self.first_record_wall_time is None:
            debug_print("Received first stop event")
            self.first_record_wall_time = time.time()
            self.first_record_timestamp = time.ctime()

        self.total_stop_events += 1
        self.vehicle_ids.add(record.get('vehicle_number'))
        self.bytes_received += raw_bytes

    # Calculate end-of-run stats
    def end_stats(self):
        self.compressed_timestamp = time.ctime()
        if self.first_record_wall_time:
            self.total_time = time.time() - self.first_record_wall_time
            self.throughput = (
                self.total_stop_events / self.total_time if self.total_time > 0 else 0
            )

    def report(self):
        print("--- SE Backup Summary ---")
        print(f"First stop event received at:   {self.first_record_timestamp}")
        print(f"Compressed at:                  {self.compressed_timestamp}")
        print(f"Unique vehicle IDs:             {len(self.vehicle_ids)}")
        print(f"Stop events received:           {self.total_stop_events}")
        print(f"Bytes received (pre-compress):  {self.bytes_received}")
        print(f"Total elapsed time:             {self.total_time:.3f}s")
        print(f"Throughput:                     {self.throughput:.1f} msg/s")
        print("------------------------\n")


# -- Globals --------------------------------------------------------------
stats           = Statistics()
stats_lock      = Lock()
sentinel_queue  = queue.Queue(maxsize=1)
file_lock       = Lock()
backup_file     = None      # backup file object, opened when first record arrives
backup_filename = None      # name of today's log file

message_queue = queue.Queue(maxsize=MESSAGE_QUEUE_MAX)

# -- Helper Functions -----------------------------------------------------
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
        with stats_lock:
            current = stats.total_stop_events
        if current >= expected_count and message_queue.empty():
            break
        timeout -= 1
        time.sleep(1)

    # Enqueue poison pills to shut down worker threads
    for _ in range(NUM_THREADS):
        message_queue.put(None)

    message_queue.join()

    # Close and compress backup file
    with file_lock:
        if backup_file and not backup_file.closed:
            backup_file.close()
        compress_backup_file()

    # Report stats
    with stats_lock:
        stats.end_stats()
        stats.report()


# Open today's backup file
def open_backup_file():
    global backup_file, backup_filename
    date_str = dt.datetime.now().strftime('%Y-%m-%d')
    backup_filename = f"se_{date_str}_.log"
    backup_file = open(backup_filename, 'w')
    debug_print(f"Opened backup file: {backup_filename}")


# Compress the backup file to .gz and remove the uncompressed original
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


# -- Process Worker --------------------------------------------
# Pulls payloads off shared queue and processes them
def process_worker():
    while True:
        payload = message_queue.get()
        raw_bytes = json.dumps(payload)

        if payload is None:
            message_queue.task_done()
            break
        
        # Write to backup file
        with file_lock:
            if backup_file is None:
                open_backup_file()
            line = raw_bytes + '\n'     # separate records with newline
            backup_file.write(line)

        with stats_lock:
            stats.record_event(payload, len(line))

        message_queue.task_done()
        


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

    message_queue.put(payload)
    message.ack()


# -- Main -----------------------------------------------------------------
def main():
    global stats, backup_file, backup_filename

    sub_path = SubscriberClient().subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    debug_print(f"Listening on: {SUBSCRIPTION_ID}")

    while True:
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

        # Reset for next day
        with stats_lock:
            stats.reset()
            backup_file.close()
            backup_filename = None
        debug_print("Finished processing. Stats reset.\n")


if __name__ == '__main__':
    main()
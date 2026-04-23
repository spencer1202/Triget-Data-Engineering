import sys
import time
import requests
import json
import pandas as pd
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.types import (
        PublishFlowControl,
        LimitExceededBehavior,
        PublisherOptions,
        BatchSettings
)

# -- Configuration ---------------------------------------------
# Debug settings
DEBUG_SAMPLE    = False     # only process a subset of vehicle IDs
SAMPLE_SIZE     = 50        # number of vehicle IDs to process (only if DEBUG_SAMPLE is true)
DEBUG_PRINT     = False     # print debug information

# Main settings
QUERY_TIMEOUT   = 10      # timeout for API request
IDS_FILE        = "vehicle_ids.csv"
URL             = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
PROJECT_ID      = 'triget-data-engineering'
TOPIC_ID        = 'bc_topic'

# Flow control settings: make sure not to overwhelm the publishing client
flow_control_settings = PublishFlowControl(
        message_limit=1000,             # 1000 messages
        byte_limit=10 * 1024 * 1024,    # 10 MiB
        limit_exceeded_behavior=LimitExceededBehavior.BLOCK
)

# -- Statistics ------------------------------------------------
class Statistics:
    def __init__(self):
        self.api_call_begin      = None  # timestamp of API calls starting
        self.total_time          = None  # total elapsed time
        self.throughput          = None  # message publishing throughput
        self.sentinel_sent       = None  # timestamp of sentinel being sent
        self.published_ct        = 0     # number of breadcrumbs published
        self.start               = None  # start time
        self.data_received_ct    = 0     # amount of vehicle IDs with data received
        self.received_ct         = 0     # number of breadcrumbs received from API

    def report(self):
        print(f"--- Publisher Summary ---")
        print(f"Started API calls at:       {self.api_call_begin}")
        print(f"Sentinel message sent at:   {self.sentinel_sent}")
        print(f"Total elapsed time:         {self.total_time:.3f}s")
        print(f"Data recieved for:          {self.data_received_ct} vehicles")
        print(f"Breadcrumbs published:      {self.published_ct}")
        print(f"Breadcrumbs received:       {self.received_ct}")
        print(f"Publishing throughput:      {self.throughput:.1f} msg/s")

    def end_stats(self):
        self.total_time = time.time() - self.start
        self.throughput = self.published_ct / self.total_time


stats = Statistics()


# -- Helper Functions ------------------------------------------
# Print only if DEBUG_PRINT option is true
def debug_print(val):
    if DEBUG_PRINT:
        pass
        #print(val)

# Make API request to get JSON response for one vehicle
def get_breadcrumbs(vehicle_id) -> dict | None:
    global stats
    if stats.api_call_begin is None:
        stats.api_call_begin = time.ctime()

    try:
        response = requests.get(
            url=URL,
            params={"vehicle_id": vehicle_id},
            timeout=QUERY_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as ex:
        code = ex.response.status_code
        if code == 404:
            debug_print("No data for this vehicle")

    except requests.exceptions.ConnectionError:
        debug_print("Connection error")
    
    except requests.exceptions.Timeout:
        debug_print("Request timed out")


# -- Publish Breadcrumbs ---------------------------------------
# Publishes all breadcrumbs for one vehicle
def publish_breadcrumbs(breadcrumbs, publisher: PublisherClient, topic_path):
    global stats
    futures = []

    for record in breadcrumbs:
        payload = json.dumps(record).encode('utf-8')
        future = publisher.publish(topic_path, payload)
        futures.append(future)
    
    for future in futures:
        try:
            future.result()
            stats.published_ct += 1
        except Exception as ex:
            debug_print(f"Error: {ex}")


# -- Publish Sentinel ------------------------------------------
# Publish the sentinel message. It has one key: 'sentinel': True
def publish_sentinel(publisher: PublisherClient, topic_path):
    global stats

    debug_print('Sending sentinel...')
    payload = json.dumps({'sentinel': True, 'count': stats.published_ct}).encode('utf-8')
    stats.sentinel_sent = time.ctime()
    future = publisher.publish(topic_path, payload)

    try:
        future.result()
    except Exception as ex:
        debug_print(f"Failed to send sentinel message: {ex}")



# -- Main ------------------------------------------------------
def main():
    global stats
    stats.start               = time.time()   # get start time
    stats.data_received_ct    = 0             # amount of vehicle IDs with data received
    stats.received_ct         = 0             # number of breadcrumbs received from API

    # Get vehicle IDs
    vehicle_ids = pd.read_csv(IDS_FILE, header=None)[0].tolist()

    # Get subset of vehicle IDs (for testing)
    if DEBUG_SAMPLE:
        vehicle_ids = vehicle_ids[:SAMPLE_SIZE]
    debug_print(vehicle_ids)

    publisher  = pubsub_v1.PublisherClient(
            publisher_options=PublisherOptions(flow_control=flow_control_settings)
    )
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # Get breadcrumb data for each vehicle ID
    for id in vehicle_ids:
        breadcrumbs: dict = get_breadcrumbs(id)

        if breadcrumbs:
            # Publish breadcrumbs
            stats.data_received_ct += 1
            stats.received_ct += len(breadcrumbs)
            publish_breadcrumbs(breadcrumbs, publisher, topic_path)

    debug_print("Finished publishing. Waiting to send sentinel...")
    time.sleep(10)
    publish_sentinel(publisher, topic_path)
    publisher.stop()

    stats.end_stats()
    stats.report()

if __name__ == "__main__": 
    main()

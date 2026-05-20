import requests
from se_publisher import parse_stop_events, URL

vehicle_num = 2929  # swap for any vehicle in your Skittles group

resp = requests.get(URL, params={"vehicle_num": vehicle_num}, timeout=10)
print(f"HTTP status: {resp.status_code}")

records = parse_stop_events(resp.text)
print(f"Records parsed: {len(records)}")

if records:
    print(f"Service date: {records[0].get('service_date')}")
    print(f"First record: {records[0]}")
else:
    print("No records returned — check your vehicle number or network")
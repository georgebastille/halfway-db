# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas",
#     "requests",
#     "sqlite3",
#     "tenacity",
#     "tqdm",
# ]
# ///
import requests
import pandas as pd
import time
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type
)

class TfLToGTFS:
    def __init__(self, app_id, app_key):
        self.base_url = "https://api.tfl.gov.uk"
        self.app_id = app_id
        self.app_key = app_key
        self.rate_limit = 300
        self.request_count = 0
        self.start_time = time.time()

        self.agency_data = []
        self.stops_data = []
        self.routes_data = []
        self.transfers_data = []

    def rate_limit_check(self):
        self.request_count += 1
        elapsed = time.time() - self.start_time

        if elapsed < 60 and self.request_count >= self.rate_limit:
            sleep_time = 60 - elapsed + 1
            print(f"\nRate limit approaching. Sleeping for {sleep_time:.1f}s")
            time.sleep(sleep_time)
            self.request_count = 0
            self.start_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError))
    )
    def fetch_tfl_data(self, endpoint, params=None):
        self.rate_limit_check()

        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params.update({"app_id": self.app_id, "app_key": self.app_key})

        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"\nHTTP Error {response.status_code}: {response.text}")
            return None
        except Exception as e:
            print(f"\nError fetching {url}: {str(e)}")
            return None

    def get_stops(self):
        modes = ["tube", "overground", "dlr", "elizabeth-line"]
        response = self.fetch_tfl_data("/StopPoint/Mode/" + ",".join(modes))

        if not response or "stopPoints" not in response:
            print("\nFailed to fetch stops or invalid response structure")
            return False

        stops = response["stopPoints"]
        print(f"\nRaw API response contains {len(stops)} stop points")

        # First pass: Identify parent stations
        parent_stations = {}
        valid_parent_types = ["NaptanMetroStation", "NaptanRailStation"]

        print("\nIdentifying parent stations:")
        for stop in tqdm(stops, desc="Processing stops"):
            if stop.get("stopType") in valid_parent_types:
                parent_id = stop["id"]
                parent_stations[parent_id] = {
                    "name": stop["commonName"],
                    "lat": stop["lat"],
                    "lon": stop["lon"],
                    "zone": stop.get("zone")
                }
                # Add parent station
                self.stops_data.append({
                    "stop_id": parent_id,
                    "stop_name": stop["commonName"],
                    "stop_lat": stop["lat"],
                    "stop_lon": stop["lon"],
                    "zone_id": stop.get("zone"),
                    "location_type": 1,
                    "parent_station": ""
                })

        print(f"Found {len(parent_stations)} parent stations")

        # Second pass: Add platforms and child stops
        child_count = 0
        print("\nProcessing child stops:")
        for stop in tqdm(stops, desc="Processing children"):
            parent_id = stop.get("parentId")

            if parent_id and parent_id in parent_stations:
                child_count += 1
                parent = parent_stations[parent_id]
                self.stops_data.append({
                    "stop_id": stop["id"],
                    "stop_name": f"{parent['name']} ({stop['commonName']})",
                    "stop_lat": parent["lat"],
                    "stop_lon": parent["lon"],
                    "zone_id": parent.get("zone"),
                    "location_type": 0,
                    "parent_station": parent_id
                })
            elif parent_id:
                print(f"\nOrphaned child stop: {stop['id']} has parent {parent_id} not in parent_stations")

        print(f"\nTotal stops processed: {len(self.stops_data)}")
        print(f" - Parent stations: {len(parent_stations)}")
        print(f" - Child stops: {child_count}")
        return True

    def get_routes(self):
        modes = ["tube", "overground", "dlr", "elizabeth-line"]
        lines = self.fetch_tfl_data("/Line/Mode/" + ",".join(modes))

        if not lines:
            return False

        print("\nProcessing routes:")
        for line in tqdm(lines, desc="Routes"):
            self.routes_data.append({
                "route_id": line["id"],
                "route_short_name": line["name"],
                "route_long_name": line.get("modeName", ""),
                "route_type": self.map_route_type(line.get("modeName")),
                "route_color": line.get("colour"),
                "route_text_color": line.get("textColour")
            })
        return True

    def map_route_type(self, mode_name):
        mode_map = {
            "tube": 1,
            "overground": 2,
            "dlr": 0,
            "elizabeth-line": 2
        }
        return mode_map.get(mode_name.lower(), 3)

    def get_transfers(self):
        if not self.stops_data:
            print("\nNo stops data - skipping transfers")
            return

        # Group platforms by parent station
        parent_stations = {}
        print("\nBuilding transfers:")
        for stop in self.stops_data:
            if stop["parent_station"]:
                parent_id = stop["parent_station"]
                if parent_id not in parent_stations:
                    parent_stations[parent_id] = []
                parent_stations[parent_id].append(stop["stop_id"])

        print(f"\nFound {len(parent_stations)} stations with platforms")

        # Create transfers between platforms in the same station
        print("\nCreating transfer entries:")
        transfer_count = 0
        for station_id, platforms in parent_stations.items():
            if len(platforms) < 2:
                continue  # Need at least 2 platforms for transfers

            for i in range(len(platforms)):
                for j in range(i+1, len(platforms)):
                    self.transfers_data.append({
                        "from_stop_id": platforms[i],
                        "to_stop_id": platforms[j],
                        "transfer_type": 2,
                        "min_transfer_time": 120
                    })
                    transfer_count += 1

        print(f"\nCreated {transfer_count} transfer pairs")

    def save_gtfs(self):
        """Save all data to GTFS files"""
        pd.DataFrame(self.agency_data).to_csv("agency.txt", index=False)

        # Filter stops to required columns
        stops_df = pd.DataFrame(self.stops_data)[[
            "stop_id", "stop_name", "stop_lat", "stop_lon",
            "zone_id", "location_type", "parent_station"
        ]]
        stops_df.to_csv("stops.txt", index=False)

        pd.DataFrame(self.routes_data).to_csv("routes.txt", index=False)

        if self.transfers_data:
            transfers_df = pd.DataFrame(self.transfers_data)
            transfers_df.to_csv("transfers.txt", index=False)
        else:
            print("No transfers data to save")

    def run(self):
        print("Starting GTFS creation...")

        if not self.get_stops():
            print("Failed to get stops data")
            return

        if not self.get_routes():
            print("Failed to get routes data")
            return

        self.get_transfers()
        self.save_gtfs()
        print("\nGTFS creation complete!")

if __name__ == "__main__":
    APP_ID = os.environ.get["TFL_APP_ID"]
    APP_KEY = os.environ.get["TFL_APP_KEY"]

    converter = TfLToGTFS(APP_ID, APP_KEY)
    converter.run()

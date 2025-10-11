# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas",
#     "requests",
#     "tenacity",
#     "tqdm",
# ]
# ///


import requests
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm  # For progress bars
from tenacity import (  # For retry logic
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
        self.rate_limit = 300  # Requests per minute
        self.request_count = 0
        self.start_time = time.time()

        # Initialize data storage
        self.agency_data = []
        self.stops_data = []
        self.routes_data = []
        self.transfers_data = []

    def rate_limit_check(self):
        """Enforce rate limiting"""
        self.request_count += 1
        elapsed = time.time() - self.start_time

        if elapsed < 60 and self.request_count >= self.rate_limit:
            sleep_time = 60 - elapsed + 1
            print(f"\nRate limit approaching. Sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
            self.request_count = 0
            self.start_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError))
    )
    def fetch_tfl_data(self, endpoint, params=None):
        """Generic TfL API fetcher with retry logic"""
        self.rate_limit_check()

        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params.update({"app_id": self.app_id, "app_key": self.app_key})

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 400:
                print(f"\nInvalid request for {url}. Check parameters.")
            elif response.status_code >= 500:
                print(f"\nServer error ({response.status_code}), retrying...")
                raise  # Will trigger retry for 5xx errors
            return None
        except requests.exceptions.RequestException as e:
            print(f"\nError fetching {url}: {str(e)}")
            if isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
                raise  # Trigger retry
            return None

    def get_agency(self):
        """Create agency.txt data"""
        self.agency_data = [{
            "agency_id": "TFL",
            "agency_name": "Transport for London",
            "agency_url": "https://tfl.gov.uk",
            "agency_timezone": "Europe/London"
        }]

    def get_stops(self):
        """Create stops.txt data with progress bar"""
        modes = ["tube", "overground", "dlr", "elizabeth-line"]
        stops = self.fetch_tfl_data("/StopPoint/Mode/" + ",".join(modes))

        if not stops:
            return False

        parent_stations = {}

        # First pass to identify parent stations
        print("\nProcessing stations:")
        for stop in tqdm(stops.get("stopPoints", []), desc="Identifying parent stations"):
            if stop.get("stopType") == "NaptanMetroStation":
                parent_stations[stop["id"]] = {
                    "name": stop["commonName"],
                    "lat": stop["lat"],
                    "lon": stop["lon"],
                    "zone": stop.get("zone")
                }

        # Second pass for platforms
        print("\nProcessing platforms:")
        for stop in tqdm(stops.get("stopPoints", []), desc="Processing platforms"):
            parent_id = stop.get("parentId")
            if parent_id and parent_id in parent_stations:
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

                # Add parent station entry
                if not any(s["stop_id"] == parent_id for s in self.stops_data):
                    self.stops_data.append({
                        "stop_id": parent_id,
                        "stop_name": parent["name"],
                        "stop_lat": parent["lat"],
                        "stop_lon": parent["lon"],
                        "zone_id": parent.get("zone"),
                        "location_type": 1,
                        "parent_station": ""
                    })
        return True

    def get_routes(self):
        """Create routes.txt data with progress bar"""
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
        """Map TfL modes to GTFS route types"""
        mode_map = {
            "tube": 1,         # Subway
            "overground": 2,   # Rail
            "dlr": 0,          # Tram
            "tflrail": 2       # Rail
        }
        return mode_map.get(mode_name.lower(), 3)  # 3 = Bus if unknown

    def get_transfers(self):
        """Create transfers.txt with progress bar"""
        parent_stations = {}

        print("\nGenerating transfers:")
        # Group platforms by parent station
        for stop in tqdm(self.stops_data, desc="Grouping platforms"):
            if stop["parent_station"]:
                if stop["parent_station"] not in parent_stations:
                    parent_stations[stop["parent_station"]] = []
                parent_stations[stop["parent_station"]].append(stop["stop_id"])

        # Create transfers
        for station_id, platforms in tqdm(parent_stations.items(), desc="Creating transfers"):
            for i in range(len(platforms)):
                for j in range(i+1, len(platforms)):
                    self.transfers_data.append({
                        "from_stop_id": platforms[i],
                        "to_stop_id": platforms[j],
                        "transfer_type": 2,
                        "min_transfer_time": 120
                    })


    def save_gtfs(self):
        """Save all data to GTFS files"""
        pd.DataFrame(self.agency_data).to_csv("agency.txt", index=False)
        pd.DataFrame(self.stops_data).to_csv("stops.txt", index=False)
        pd.DataFrame(self.routes_data).to_csv("routes.txt", index=False)
        pd.DataFrame(self.transfers_data).to_csv("transfers.txt", index=False)

    def run(self):
        """Execute full pipeline"""
        print("Fetching agency data...")
        self.get_agency()

        print("Fetching stops data...")
        if not self.get_stops():
            return

        print("Fetching routes data...")
        if not self.get_routes():
            return

        print("Generating transfers...")
        self.get_transfers()

        print("Saving GTFS files...")
        self.save_gtfs()
        print("Done! Created agency.txt, stops.txt, routes.txt, and transfers.txt")

if __name__ == "__main__":
    # Get your API keys from https://api.tfl.gov.uk/
    APP_ID = os.environ.get["TFL_APP_ID"]
    APP_KEY = os.environ.get["TFL_APP_KEY"]

    converter = TfLToGTFS(APP_ID, APP_KEY)
    converter.run()

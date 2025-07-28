import json
import sqlite3
import os


# Function to create the SQLite database
def create_database():
    # Check if database file exists, if yes, remove it
    if os.path.exists("tfl.db"):
        os.remove("tfl.db")

    # Connect to the database (creates a new file)
    conn = sqlite3.connect("tfl.db")
    cursor = conn.cursor()

    # Create tables according to the schema
    cursor.execute(
        'CREATE TABLE STATIONS ("CODE" text, "NAME" text, "LATITUDE" real, "LONGITUDE" real)'
    )
    cursor.execute(
        "CREATE TABLE FULLROUTES(_id INTEGER PRIMARY KEY, STATIONA TEXT, STATIONB TEXT, WEIGHT REAL)"
    )

    # Populate data from TFL data files
    populate_stations(cursor)
    populate_fullroutes(cursor)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print("Database created successfully: tfl.db")


# Function to populate STATIONS table
def populate_stations(cursor):
    stations = []

    # First create a dictionary to deduplicate stations
    station_dict = {}
    with open("./stations.jsonl", "r") as f:
        for line in f:
            data = json.loads(line)
            station_id = data["station_id"]
            station_name = data["station_name"]

            # You might need to fetch lat/long from another source
            # For now, use dummy values or find them in the data if available
            lat = 0.0
            lon = 0.0

            station_dict[station_id] = (station_id, station_name, lat, lon)

    stations = list(station_dict.values())

    cursor.executemany("INSERT INTO STATIONS VALUES (?, ?, ?, ?)", stations)


# Function to populate FULLROUTES table
def populate_fullroutes(cursor):
    fullroutes = []

    with open("./shortest_paths.jsonl", "r") as f:
        for i, line in enumerate(f, 1):
            data = json.loads(line)
            fullroutes.append(
                (i, data["from_station"], data["to_station"], data["time"])
            )

    cursor.executemany("INSERT INTO FULLROUTES VALUES (?, ?, ?, ?)", fullroutes)


if __name__ == "__main__":
    create_database()

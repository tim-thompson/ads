# Example Usage
#
# python police.py data 0.146181 51.57363 --mode 1 --distance 1000


import pymongo
from pymongo import MongoClient
from pprint import pprint
import csv
import argparse

# Define possible modes of operation
modes = ["crimeType", "outcome"]

parser = argparse.ArgumentParser()
parser.add_argument("data", type=str, help="the path to police data dir")
parser.add_argument("longitude", type=float, help="longitude of central point")
parser.add_argument("latitude", type=float, help="latitude of central point")
parser.add_argument(
    "--mode",
    action="store",
    type=str,
    choices=modes,
    default=modes[0],
    help=f"set mode of aggregation, available options are {modes}",
    metavar="",
)
parser.add_argument(
    "--distance",
    type=int,
    help="distance around point to include data in metres",
    default=1000,
    metavar="",
)

args = parser.parse_args()

# Set up mongo connection
client = MongoClient()
db = client.test_db

# Clear and create collection
street = db.street
street.drop()

# Input variables
longitude, latitude = 0.140634, 51.583427

# Open file with context manager
with open("data/2017-01/2017-01-metropolitan-street.csv") as csv_file:
    # Use dict reader to read each line from csv and auto use headers
    reader = list(csv.DictReader(csv_file))

    street_list = []

    # Construct data model from csv data using geospatial constructs
    for row in reader:
        # Catch bad lat/lng data to prevent indexing failures
        try:
            long = float(row["Longitude"])
        except ValueError as ve:
            long = 0

        try:
            lat = float(row["Latitude"])
        except ValueError as ve:
            lat = 0

        data = {
            "crimeId": row["Crime ID"],
            "month": row["Month"],
            "reportedBy": row["Reported by"],
            "fallsWithin": row["Falls within"],
            "location": {"type": "Point", "coordinates": [long, lat]},
            "locationName": row["Location"],
            "lsoaCode": row["LSOA code"],
            "lsoaName": row["LSOA name"],
            "crimeType": row["Crime type"],
            "lastOutcomeCategory": row["Last outcome category"],
            "context": row["Context"],
        }
        street_list.append(data)

    # Insert bulk for speed
    street.insert_many(street_list)

    # Create Geospatial index for aggregation
    street.create_index([("location", pymongo.GEOSPHERE)])

# Dictionary containing options for aggregation pipeline
group_by = {
    "crimeType": {"crimeType": "$crimeType"},
    "outcome": {"lastOutcomeCategory": "$lastOutcomeCategory"},
}

check_exists = {"crimeType": "crimeType", "outcome": "lastOutcomeCategory"}

# Get Total Documents
total_documents = street.aggregate(
    [
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [args.longitude, args.latitude],
                },
                "spherical": True,
                "distanceField": "calcDistance",
                "maxDistance": args.distance,
            },
        },
        {"$group": {"_id": None, "count": {"$sum": 1}}},
    ]
)

for doc in total_documents:
    total_count = doc["count"]

# Define pipeline for aggregating data getting data from dictionaries using input type key
geo_aggregation_pipeline = [
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [args.longitude, args.latitude]},
            "spherical": True,
            "distanceField": "calcDistance",
            "maxDistance": args.distance,
        }
    },
    {"$match": {check_exists.get(args.mode): {"$exists": True, "$ne": ""}}},
    {"$group": {"_id": group_by.get(args.mode), "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$project": {"count": 1, "percentage": {"$multiply": [{"$divide": [100, total_count]}, "$count"]}}}
]

# Run aggregation and display results
result = street.aggregate(geo_aggregation_pipeline)
pprint(list(result))

# pprint(street.find_one())

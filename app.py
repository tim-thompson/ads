import pymongo
from pymongo import MongoClient
from pprint import pprint
import csv
import sys

# Set up mongo connection
client = MongoClient()
db = client.test_db

# Clear and create collection
street = db.street
street.drop()

# Input variables
input_type = "crimeType"
maxDistance = 5000
data_dir = "data"

# Open file with context manager
with open("data/met/2017-01/2017-01-metropolitan-street.csv") as csv_file:
    # Use dict reader to read each line from csv
    reader = list(csv.DictReader(csv_file))

    street_list = []

    # Construct data model from csv data using geospatial constructs
    for row in reader:
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

    street.insert_many(street_list)
    street.create_index([("location", pymongo.GEOSPHERE)])

group_by = {
    "crimeType": {"crimeType": "$crimeType"},
    "outcome:": {"lastOutcomeCategory": "$lastOutcomeCategory"},
}

check_exists = {}

# Define pipeline for aggregating data
aggregation_pipeline = [
    {"$match": {"crimeType": {"$exists": True, "$ne": ""}}},
    {"$match": {"lastOutcomeCategory": {"$exists": True, "$ne": ""}}},
    {
        "$group": {
            "_id": {
                "crimeType": "$crimeType",
                "lastOutcomeCategory": "$lastOutcomeCategory",
            },
            "count": {"$sum": 1},
        }
    },
    {"$sort": {"count": -1}},
]

geo_aggregation_pipeline = [
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [0.140634, 51.583427]},
            "spherical": True,
            "distanceField": "calcDistance",
            "maxDistance": maxDistance,
        }
    },
    {"$match": {input_type: {"$exists": True, "$ne": ""}}},
    {"$group": {"_id": group_by.get(input_type), "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
]

# Run aggregation and display results
result = street.aggregate(geo_aggregation_pipeline)
pprint(list(result))

# pprint(street.find_one())

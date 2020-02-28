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
outcome = db.outcome
outcome.drop()

# Reference for street level data csv headers
headers = ["crimeId", "month", "reportedBy", "fallsWithin", "longitude", "latitude", "location", "lsdaCode", "lsdaName",
           "crimeType", "lastOutcomeCategory", "context"]

# Open file with context manager
with open('data/met/2017-01/2017-01-metropolitan-street.csv') as csv_file:
    # Use dict reader to read each line from csv
    reader = csv.DictReader(csv_file, fieldnames=headers)

    # Insert read dict direct into mongo
    street.insert_many(reader)

with open('data/met/2017-01/2017-01-metropolitan-outcomes.csv') as csv_file:
    reader = csv.DictReader(csv_file)
    outcome.insert_many(reader)

join_pipeline = [
    {
        "$lookup": {
            "from": "outcome",
            "localField": "crimeId",
            "foreignField": "Crime ID",
            "as": "outcome"
        }
    },
    {
        "$out": "street_outcome"
    }
]

result = street.aggregate(join_pipeline)
pprint(list(result))

# Define pipeline for aggregating data
aggregation_pipeline = [
    {"$match": {"crimeType": {"$exists": True, "$ne": ""}}},
    {"$match": {"lastOutcomeCategory": {"$exists": True, "$ne": ""}}},
    {"$group": {
        "_id": {
            "crimeType": "$crimeType",
            "lastOutcomeCategory": "$lastOutcomeCategory"
        },
        "count": {
            "$sum": 1
        }
    }},
    {"$sort": {"count": -1}}
]

# Run aggregation and display results
result = street.aggregate(aggregation_pipeline)
# pprint(list(result))

# Construct results collection
results = db.results
results.drop()

result_list = list(result)
# results.insert_many(list(result))

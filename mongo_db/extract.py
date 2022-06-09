import configparser
import csv
import datetime
from datetime import timedelta

import boto3
from pymongo import MongoClient

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mongo_config", "hostname")
username = parser.get("mongo_config", "username")
password = parser.get("mongo_config", "password")
database_name = parser.get("mongo_config", "database")
collection_name = parser.get("mongo_config", "collection")

mongo_client = MongoClient(
    "mongodb+srv://"
    + username
    + ":"
    + password
    + "@"
    + hostname
    + "/"
    + database_name
    + "?retryWrites=true&"
    + "w=majority&ssl=true&"
    + "ssl_cert_reqs=CERT_NONE"
)

mongo_db = mongo_client[database_name]
mongo_collection = mongo_db[collection_name]

# extract using dates
start_date = datetime.datetime.today() + timedelta(days=-1)
end_date = start_date + timedelta(days=1)

mongo_query = {
    "$and": [
        {"event_timestamp": {"$gte": start_date}},
        {"event_timestamp": {"$lt": end_date}},
    ]
}

event_docs = mongo_collection.find(mongo_query, batch_size=3000)

# the final list to store the events
all_events = []

# iterate thorugh the cursor of event_docs variable
for doc in event_docs:
    # set a -1 or None in case the value doesn't exist
    event_id = str(doc.get("event_id", -1))
    event_timestamp = doc.get("event_timestamp", None)
    event_name = doc.get("event_name", None)

    # add events to a  temporary list
    current_event = []
    current_event.append(event_id)
    current_event.append(event_timestamp)
    current_event.append(event_name)

    # add the temporary list to the final list
    all_events.append(current_event)


# save the final list in a csv
export_file = "from_mongodb.csv"

with open(export_file, "w") as fp:
    csvw = csv.writer(fp, delimiter="|")
    csvw.writerows(all_events)

fp.close()


# load boto3 credentials
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3_file = export_file
s3.upload_file(export_file, bucket_name, s3_file)

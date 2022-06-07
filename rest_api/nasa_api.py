import requests
import json
import configparser
import csv
import boto3

lat = 19.504148607145027
lon = -99.20053932353616


lat_long_params = {"lat": lat, "lon": lon}

api_response = requests.get("http://api.open-notify.org/iss-pass.json", params=lat_long_params)

response_json = json.loads(api_response.content)
passes_above_lat_long = []

for response in response_json["response"]:
    current_pass = []
    if lat not in current_pass and lon not in current_pass:
        # store the lat and lon used in the request
        current_pass.append(lat)
        current_pass.append(lon)
        # store the duration and risetime of the pass
        current_pass.append(response['duration'])
        current_pass.append(response['risetime'])

        passes_above_lat_long.append(current_pass)
    else:
        continue

print(passes_above_lat_long)
print(len(passes_above_lat_long))

export_file = "rest_api.csv"
with open (export_file, "w") as f:
    csvw = csv.writer(f, delimiter="|")
    csvw.writerows(passes_above_lat_long)
f.close()


# get credentials from s3
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials",
                "access_key")
secret_key = parser.get("aws_boto_credentials",
                "secret_key")
bucket_name = parser.get("aws_boto_credentials",
                "bucket_name")

s3 = boto3.client(
    's3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key
)

s3.upload_file(
    export_file,
    bucket_name,
    export_file
)
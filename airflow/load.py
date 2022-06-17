import configparser
import os

import psycopg2

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "..", "pipeline.conf"))
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
print(dbname, user, password, host, port)

# connect to the redshift cluster
rs_conn = psycopg2.connect(
    "dbname="
    + dbname
    + " user="
    + user
    + " password="
    + password
    + " host="
    + host
    + " port="
    + port
)


print("success")

# parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "..", "pipeline.conf"))
account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# run the COPY command to load the file into Redshift
file_path = "s3://" + bucket_name + "/order_extract.csv"
role_string = "arn:aws:iam::" + account_id + ":role/" + iam_role

sql = "COPY public.orders"
sql = sql + " from %s "
sql = sql + " iam_role %s "
sql = sql + " delimiter '|' ;"

# create a cursor object and execute the COPY command
cur = rs_conn.cursor()
cur.execute(sql, (file_path, role_string))

# close the cursor and commit the transaction
cur.close()
rs_conn.commit()

# close the connection
rs_conn.close()

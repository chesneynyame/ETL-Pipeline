import json
import boto3
import csv
import src.transform as transform
import src.create_db as create_db
import src.load_db as load_db
import src.app as app
import os 
import psycopg2
from dotenv import load_dotenv

load_dotenv()
database = os.environ["DB"]
host = os.environ["HOST"]
port = os.environ["PORT"]
user = os.environ["DB_USER"]
password = os.environ["PASSWORD"]
    
def handle(event, context):
    con = psycopg2.connect(database=database, host=host,
                           port=port, user=user, password=password)
    cursor = con.cursor()
    
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # use boto3 library to get object from S3
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket = bucket, Key = key)
    data = s3_object['Body'].read().decode('utf-8')
    
    # read CSV
    csv_file_from_bucket = data.splitlines()
    reader = csv.DictReader(csv_file_from_bucket, fieldnames=('date_time','location','customer_name', 'products_ordered', 'order_total', 'payment_type', 'payment_info'))
    # cleans
    app.extract_transform_load(reader)
    print("The ETL pipeline has been deployed successfully!")

    return {"message": "Team 1's ETL pipeline has been deployed successfully!"}



    
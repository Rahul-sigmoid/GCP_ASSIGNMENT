from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from pyparsing import basestring

credentials=service_account.Credentials.from_service_account_file('/Users/rahul/Downloads/project-1-322814-d155a91c4d05.json')

client = storage.Client.from_service_account_json(json_credentials_path='/Users/rahul/Downloads/project-1-322814-d155a91c4d05.json')

# get the bucket
bucket = client.get_bucket('upload_to_storage')

# get the file 1
blob1 = bucket.blob('Orders.csv')
file1='/Users/rahul/downloads/new_orders.csv'
# download file 1
blob1.download_to_filename(file1)

# get the file 2
blob2 = bucket.blob('Customers.csv')
# download file 2
file2='/Users/rahul/downloads/new_customers.csv'
blob2.download_to_filename(file2)

# read the downloaded files
a = pd.read_csv(file1).astype(basestring)
b = pd.read_csv(file2).astype(basestring)

# merge files
merged= a.merge(b, on='CustomerID', how='left')

cli=bigquery.Client(credentials=credentials, project='project-1-322814')

# create a dataset
dataset=bigquery.Dataset('project-1-322814.upload_to_bigquery')
dataset=cli.create_dataset(dataset)

# create table
query_1=cli.query("""
CREATE TABLE `project-1-322814.upload_to_bigquery.table-1`(
    `OrderID` INTEGER,
    `CustomerID` INTEGER,
    `EmployeeID` INTEGER,
    `OrderDate` DATE,
    `ShipperID` INTEGER,
    `CustomerName` STRING,
    `ContactName` STRING,
    `Address` STRING,
    `City` STRING,
    `PostalCode` STRING,
    `Country` STRING
) """)

result_1=query_1.result()

# import data to the table
merged.to_gbq(
    'upload_to_bigquery.table-1',
    'project-1-322814',
    if_exists='replace'
)

from google.cloud import storage

try:
  
    # Setting credentials using the downloaded JSON file

    client = storage.Client.from_service_account_json(json_credentials_path='/Users/rahul/Downloads/project-1-322814-d155a91c4d05.json')

    # create bucket

    bucket = client.create_bucket('upload_to_storage')

    # get the bucket object

    bucket = client.get_bucket('upload_to_storage')

    # Name of the object-1 to be stored in the bucket

    object1 = bucket.blob('Orders.csv')

    # Name of the object-1 in local file system

    object1.upload_from_filename('/Users/rahul/Downloads/Orders.csv')

    # Name of the object-2 to be stored in the bucket

    object2 = bucket.blob('Customers.csv')

    # Name of the object-2 in local file system

    object2.upload_from_filename('/Users/rahul/Downloads/Customers.csv')
  
except Exception as err:
  
    print(err)
    
    raise Exception("Table load failed for customers table")

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.blob import BlobClient
import psycopg2
import json
from io import BytesIO
import os
from dotenv import load_dotenv
from io import StringIO, BytesIO
import pandas as pd

def run_loading():
    # Load environment variables from a .env file
    load_dotenv()
    
    # Read CSV files into pandas DataFrames
    sales = pd.read_csv('sales.csv')
    location = pd.read_csv('location.csv')
    feature = pd.read_csv('feature.csv')
    fact_table = pd.read_csv('fact_table.csv')
    cleandata = pd.read_csv('cleandata.csv')

    # Load connection string from environment variables
    connection_string = os.getenv('AZURE_BLOB_STORAGE_CONNECTION_STRING').strip()

    if not connection_string:
        raise ValueError("Azure Storage connection string is missing.")

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Define the container name
    container_name = os.getenv('AZURE_BLOB_STORAGE_NAME').strip()

    # Get a container client
    container_client = blob_service_client.get_container_client(container_name)

    # Check if the container exists, if not create it
    if not container_client.exists():
        print(f"Container '{container_name}' does not exist.")
        container_client.create_container()
        print(f"Container '{container_name}' created successfully.")
    else:
        print(f"Container '{container_name}' already exists.")

    # List of dataframes and corresponding blob names
    data = [
        (sales, 'data2/sales_table.csv'),
        (location, 'data2/location_table.csv'),
        (feature, 'data2/feature_table.csv'),
        (fact_table, 'data2/fact_table.csv'),  # Assuming 'date_table' refers to a dataframe
        (cleandata, 'data2/cleandata_table.csv')
    ]

    # Upload each dataframe as a blob to Azure Blob Storage
    for df, blob_name in data:
        try:
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)

            # Get a blob client
            blob_client = container_client.get_blob_client(blob_name)

            # Upload CSV data to blob storage
            blob_client.upload_blob(csv_data, overwrite=True)
            print(f"Uploaded {blob_name} to Azure Blob Storage successfully.")

        except Exception as e:
            print(f"An error occurred while uploading {blob_name}: {e}")
            print('data is load successflly into {blob_name} for loading')
 

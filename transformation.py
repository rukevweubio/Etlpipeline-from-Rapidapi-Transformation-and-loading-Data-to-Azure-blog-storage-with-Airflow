import pandas as pd
import json
import requests
import datetime as dt

def run_transformation():
    url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"
    querystring = {"limit": "1000"}
    headers = {
        "x-rapidapi-key": "YOUR_RAPIDAPI_KEY",  # Ensure to replace with your actual RapidAPI key
        "x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()

    # Create a dataframe from the data
    df = pd.DataFrame(data)
    df.info()

    # Fill the missing data
    fill_values = {
        'addressLine2': 'unknown',
        'assessorID': 'unknown',
        'bedrooms': 0.0,
        'features': 'unknown',
        'legalDescription': 'unknown',
        'squareFootage': 0.0,
        'subdivision': 'unknown',
        'yearBuilt': 0.0,
        'bathrooms': 0.0,
        'lotSize': 0.0,
        'propertyType': 'unknown',
        'taxAssessment': 'unknown',
        'propertyTaxes': 'unknown',
        'lastSalePrice': 0.0,
        'lastSaleDate': 'unknown',
        'zoning': 'unknown',
        'ownerOccupied': 'unknown',
        'owner': 'unknown'
    }
    df.fillna(fill_values, inplace=True)

    # Convert necessary columns to string to avoid conversion issues
    for col in ['features', 'taxAssessment', 'propertyType', 'owner', 'ownerOccupied']:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))

    # Create the date column
    df['lastSaleDate'] = pd.to_datetime(df['lastSaleDate'], errors='coerce')
    df['Date'] = df['lastSaleDate'].dt.date
    df['Day'] = df['lastSaleDate'].dt.day_name()
    df['Month'] = df['lastSaleDate'].dt.month_name()
    df['Year'] = df['lastSaleDate'].dt.year

    # Create the sales table
    sales = df[['lastSalePrice', 'Date', 'Day', 'Month', 'Year']].drop_duplicates().reset_index(drop=True)
    sales['saleid'] = sales.index + 1
    sales = sales[['saleid', 'lastSalePrice', 'Date', 'Day', 'Month', 'Year']].drop_duplicates()

    # Create the location table
    location = df[['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress', 'county', 'longitude', 'latitude']].drop_duplicates().reset_index(drop=True)
    location.index.name = 'locationid'
    location = location.reset_index()

    # Create the feature table
    feature = df[['features', 'propertyType', 'yearBuilt', 'zoning']].drop_duplicates().reset_index(drop=True)
    feature.index.name = 'featureid'
    feature = feature.reset_index()

    # Create the fact table
    fact_table = df.merge(sales, on=['lastSalePrice', 'Date', 'Day', 'Month', 'Year'], how='left') \
        .merge(location, on=['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress', 'county', 'longitude', 'latitude'], how='left') \
        .merge(feature, on=['features', 'propertyType', 'yearBuilt', 'zoning'], how='left')
    fact_table = fact_table[['saleid', 'locationid', 'featureid', 'squareFootage', 'yearBuilt', 'county', 'assessorID', 'legalDescription', 'subdivision', 'bathrooms', 'lotSize', 'id', 'bedrooms']].reset_index(drop=True)
    fact_table.index.name = 'factid'
    fact_table = fact_table.reset_index()
    fact_table = fact_table[['factid', 'saleid', 'locationid', 'featureid', 'squareFootage', 'yearBuilt', 'county', 'assessorID', 'legalDescription', 'subdivision', 'bathrooms', 'lotSize', 'id', 'bedrooms']]

    # Save data to CSV files
    sales.to_csv('sales.csv', index=False)
    location.to_csv('location.csv', index=False)
    feature.to_csv('feature.csv', index=False)
    fact_table.to_csv('fact_table.csv', index=False)
    df.to_csv('cleandata.csv', index=False)

    print('Data cleaning and transformation completed successfully.')

# Remember to replace "YOUR_RAPIDAPI_KEY" with your actual RapidAPI key before running the script.

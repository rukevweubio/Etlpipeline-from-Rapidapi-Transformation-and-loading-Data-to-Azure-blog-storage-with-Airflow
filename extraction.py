import pandas as pd
import os
import requests
import json

def run_extraction():
    url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"
    querystring = {"limit": "1000"}
    headers = {
        "x-rapidapi-key": "YOUR_RAPIDAPI_KEY",  # Replace with your actual RapidAPI key
        "x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
        
        # Convert JSON response to DataFrame
        df = pd.DataFrame(response.json())
        
        print('Data extracted successfully.')
        
        # Optionally, save the DataFrame to a CSV file
        # df.to_csv('extracted_data.csv', index=False)
        
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error occurred: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}")

# Remember to replace "YOUR_RAPIDAPI_KEY" with your actual RapidAPI key before running the script.

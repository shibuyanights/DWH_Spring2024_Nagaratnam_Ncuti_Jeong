import requests
import urllib.request
import json
import pandas as pd
import time
import boto3
from io import StringIO

api_key = 'I TOOK IT OUT BECAUSE GITHUB IS PUBLIC' 
headers = {'x-api-key': api_key}

# Initialize S3 client
s3 = boto3.client('s3')

def get_json_data(url, headers):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print("Unauthorized: Check your API key")
        elif e.code == 429:
            print("Rate limit exceeded. Retrying...")
            time.sleep(60)  # Wait for 60 seconds before retrying
            return get_json_data(url, headers)
        else:
            print(f"HTTP Error {e.code}: {e.reason}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

destinations = {
    "zermatt": {"lat": 46.0207, "lon": 7.7491, "radius": 10000},
    "jungfrau": {"lat": 46.6242, "lon": 8.0414, "radius": 10000},
    "interlaken": {"lat": 46.6863, "lon": 7.8632, "radius": 10000},
    "lucerne": {"lat": 47.0502, "lon": 8.3103, "radius": 10000},
    "montreux": {"lat": 46.4312, "lon": 6.9106, "radius": 10000},
    "geneva": {"lat": 46.2044, "lon": 6.1432, "radius": 10000},
    "zurich": {"lat": 47.3769, "lon": 8.5417, "radius": 10000},
    "bern": {"lat": 46.9481, "lon": 7.4474, "radius": 10000},
    "lugano": {"lat": 46.0050, "lon": 8.9616, "radius": 10000},
    "st_moritz": {"lat": 46.4983, "lon": 9.8380, "radius": 10000},
    "appenzell": {"lat": 47.3302, "lon": 9.4096, "radius": 10000}
}

def fetch_page_data(page_num, lat, lon, radius):
    params = {
        'lang': 'en',
        'page': page_num,
        'hitsPerPage': 20,
        'facet.filter': 'seasons:summer',
        'geo.dist': f'{lat},{lon},{radius}',
        'striphtml': True
    }
    query_string = urllib.parse.urlencode(params)
    current_url = f"https://opendata.myswitzerland.io/v1/attractions?{query_string}"
    return get_json_data(current_url, headers)

def normalize_attractions(data):
    attractions = data.get('data', [])
    normalized_data = []

    for item in attractions:
        classification_name = item.get('classification', [{}])[0].get('name', None)
        classification_values_name = item.get('classification', [{}])[0].get('values', [{}])[0].get('name', None)
        price = item.get('price', {}).get('price', None)
        
        attraction = {
            'identifier': item.get('identifier'),
            'name': item.get('name'),
            'abstract': item.get('abstract'),
            'geo.latitude': item.get('geo', {}).get('latitude'),
            'geo.longitude': item.get('geo', {}).get('longitude'),
            'topAttraction': item.get('topAttraction', False),
            'classification.name': classification_name,
            'classification.values.name': classification_values_name,
            'price': price
        }
        normalized_data.append(attraction)

    return pd.DataFrame(normalized_data)

def fetch_all_data_for_location(location):
    lat = destinations[location]['lat']
    lon = destinations[location]['lon']
    radius = destinations[location]['radius']
    
    initial_params = {
        'lang': 'en',
        'geo.dist': f'{lat},{lon},{radius}',
        'hitsPerPage': 1 
    }
    query_string = urllib.parse.urlencode(initial_params)
    initial_url = f"https://opendata.myswitzerland.io/v1/attractions?{query_string}"
    initial_data = get_json_data(initial_url, headers)

    if initial_data and 'meta' in initial_data and 'page' in initial_data['meta']:
        page_count = initial_data['meta']['page']['totalPages']
        print(f"Total number of pages for {location}: {page_count}")
    else:
        print(f"Failed to retrieve initial data or determine total number of pages for {location}")
        page_count = 0

    data_frames = []
    consecutive_no_data_pages = 0

    for page in range(page_count):  
        data = fetch_page_data(page, lat, lon, radius)
        if data and data.get('data'):
            print(f"Fetched data for page {page} for {location}")
            consecutive_no_data_pages = 0  
            
            df = normalize_attractions(data)
            data_frames.append(df)
        else:
            print(f"No data found for page {page} for {location}")
            consecutive_no_data_pages += 1
            
            if consecutive_no_data_pages >= 5:
                print(f"Stopping early for {location} due to consecutive pages with no data.")
                break
        
        # to prevent api denial because of too many requests in short time
        if (page + 1) % 10 == 0:
            time.sleep(2)  
        else:
            time.sleep(1)  

    if data_frames:
        combined_df = pd.concat(data_frames, axis=0, ignore_index=True)
        return combined_df
    else:
        print(f"No data to combine for {location}")
        return pd.DataFrame() 

dfs = {}
for location in destinations.keys():
    dfs[location] = fetch_all_data_for_location(location)

for location in dfs.keys():
    dfs[location] = dfs[location].reset_index(drop=True)
    dfs[location]['city'] = location

combined_df = pd.concat(dfs.values(), ignore_index=True)

def lambda_handler(event, context):
    try:
        csv_buffer = StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        
        bucket_name = 'myswitzerlandapi'
        s3.put_object(Bucket=bucket_name, Key='combined_data.csv', Body=csv_buffer.getvalue())
        
        return {
            'statusCode': 200,
            'body': 'CSV file created and uploaded to S3!'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error creating or uploading CSV file: {e}'
        }

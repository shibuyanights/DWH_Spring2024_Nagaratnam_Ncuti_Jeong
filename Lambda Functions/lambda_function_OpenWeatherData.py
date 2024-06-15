import boto3
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):

    s3_client = boto3.client('s3')
    bucket_name = 'openweatherdataapi'  
    locations_file_key = 'ch.csv'
    output_file_key = 'weather_data/5_day_3hour_weather_forecast.csv'
    
    print(f"Trying to access file at: {bucket_name}/{locations_file_key}")

    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=locations_file_key)
        locations_df = pd.read_csv(response['Body'], encoding='ISO-8859-1')
    except Exception as e:
        print(f"Error loading locations file: {e}")
        return {
            'statusCode': 500,
            'body': f"Error loading locations file: {e}"
        }
    
    locations = locations_df[['lat', 'lng', 'city']].to_dict(orient='records')
    api_key = 'I TOOK IT OUT BECAUSE GITHUB IS PUBLIC'

    weather_data = []
    for location in locations:
        lat = location['lat']
        lng = location['lng']
        city = location['city']
        data = get_weather(api_key, lat, lng)
        if data:
            for entry in data['list']:
                weather_data.append({
                    'city': city,
                    'lat': lat,
                    'lng': lng,
                    'datetime': datetime.fromtimestamp(entry['dt']).isoformat(),
                    'temperature_celsius': entry['main']['temp'] - 273.15,
                    'weather': entry['weather'][0]['description']
                })

    new_weather_df = pd.DataFrame(weather_data)

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=output_file_key)
        existing_weather_df = pd.read_csv(response['Body'])
        updated_weather_df = pd.concat([existing_weather_df, new_weather_df], ignore_index=True)
    except Exception:
        updated_weather_df = new_weather_df  

    csv_buffer = StringIO()
    updated_weather_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=output_file_key, Body=csv_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': 'Weather data updated successfully in S3'
    }

def get_weather(api_key, lat, lng):
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lng}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to get weather data for lat: {lat}, lng: {lng}. HTTP Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        return None


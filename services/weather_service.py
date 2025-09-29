import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import numpy as np
import logging
import requests
import json

logger = logging.getLogger(__name__)

class WeatherService:
    def __init__(self):
        try:
            cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            self.openmeteo = openmeteo_requests.Client(session=retry_session)
            logger.info("Weather service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize weather api: {e}")
            raise
    
    def fetch_weather_data(self, latitude, longitude, days=2):
        
        logger.info(f"Starting weather data fetch for: lat={latitude}, lon={longitude}")
        
        if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
            raise ValueError(f"Invalid coordinate types: lat={type(latitude)}, lon={type(longitude)}")
        
        if not (-90 <= latitude <= 90):
            raise ValueError(f"Invalid latitude: {latitude} (must be between -90 and 90)")
        
        if not (-180 <= longitude <= 180):
            raise ValueError(f"Invalid longitude: {longitude} (must be between -180 and 180)")
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"Date range: {start_date} to {end_date}")
        
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": float(latitude),
            "longitude": float(longitude),
            "hourly": ["temperature_2m", "relative_humidity_2m"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": "auto"
        }
        
        logger.info(f"API URL: {url}")
        logger.info(f"Parameters: {json.dumps(params, indent=2)}")
        
        try:
            logger.info("Testing with direct requests...")
            test_response = requests.get(url, params=params, timeout=30)
            logger.info(f"Direct request status: {test_response.status_code}")
            
            if test_response.status_code == 200:
                test_data = test_response.json()
                logger.info(f"Direct request successful. Data keys: {list(test_data.keys())}")
                if 'hourly' in test_data:
                    hourly_keys = list(test_data['hourly'].keys()) if test_data['hourly'] else []
                    logger.info(f"Hourly data keys: {hourly_keys}")
                else:
                    logger.warning("No 'hourly' key in direct response")
            else:
                logger.error(f"Direct request failed: {test_response.status_code} - {test_response.text}")
                raise Exception(f"API request failed with status {test_response.status_code}")
            
            logger.info("Making API call with openmeteo client...")
            responses = self.openmeteo.weather_api(url, params=params)
            
            if not responses:
                raise Exception("No responses received from API")
            
            response = responses[0]
            
            logger.info(f"API Response received")
            logger.info(f"Coordinates: {response.Latitude():.4f}°N, {response.Longitude():.4f}°E")
            logger.info(f"Elevation: {response.Elevation()}m")
            logger.info(f"Timezone offset: {response.UtcOffsetSeconds()}s")
            
            return self._process_response(response, latitude, longitude)
            
        except Exception as e:
            logger.error(f"API Error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise Exception(f"Weather API error: {str(e)}")
    
    def _process_response(self, response, lat, lon):
        
        logger.info("Processing API response...")
        
        hourly = response.Hourly()
        if not hourly:
            raise Exception("No hourly data in response")
        
        hourly_time = range(hourly.Time(), hourly.TimeEnd(), hourly.Interval())
        time_count = len(list(hourly_time))
        logger.info(f"Time range: {time_count} time points")
        
        hourly_time = range(hourly.Time(), hourly.TimeEnd(), hourly.Interval())
        
        variables_length = hourly.VariablesLength()
        logger.info(f"Variables count: {variables_length}")
        
        if variables_length < 2:
            raise Exception(f"Expected 2 variables (temperature, humidity), got {variables_length}")
        
        hourly_variables = list(map(lambda i: hourly.Variables(i), range(0, variables_length)))
        
        
        temp_data = hourly_variables[0].ValuesAsNumpy()
        humidity_data = hourly_variables[1].ValuesAsNumpy()
        
        logger.info(f"Temperature data points: {len(temp_data)}")
        logger.info(f"Humidity data points: {len(humidity_data)}")
        
        if len(temp_data) == 0 or len(humidity_data) == 0:
            raise Exception("No temperature or humidity data received")
        
        records = []
        valid_records = []
        
        for i, timestamp in enumerate(hourly_time):
            if i < len(temp_data) and i < len(humidity_data):
                temp = float(temp_data[i]) if not np.isnan(temp_data[i]) else None
                humidity = float(humidity_data[i]) if not np.isnan(humidity_data[i]) else None
                
                record = {
                    "timestamp": datetime.fromtimestamp(timestamp).isoformat(),
                    "temperature_2m": round(temp, 2) if temp is not None else None,
                    "relative_humidity_2m": round(humidity, 1) if humidity is not None else None,
                    "latitude": lat,
                    "longitude": lon
                }
                
                records.append(record)
                
                if temp is not None and humidity is not None:
                    valid_records.append(record)
                    
                if i < 3:
                    logger.info(f"Sample record {i}: temp={temp}, humidity={humidity}")
        
        logger.info(f"Processed {len(records)} total records, {len(valid_records)} valid records")
        
        if len(valid_records) == 0:
            logger.warning("No valid records found (all data contains NaN values)")
        
        return {
            "status": "success",
            "metadata": {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(), 
                "elevation": response.Elevation(),
                "timezone_offset": response.UtcOffsetSeconds(),
                "total_records": len(records),
                "valid_records": len(valid_records),
                "date_range": {
                    "start": records[0]["timestamp"] if records else None,
                    "end": records[-1]["timestamp"] if records else None
                }
            },
            "data": valid_records,
            "sample_data": valid_records[:5] if valid_records else []
        }

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    service = WeatherService()
    
    test_coordinates = [
        (47.37, 8.0),
    ]
    
    for lat, lon in test_coordinates:
        print(f"\nTesting coordinates: {lat}, {lon}")
        try:
            result = service.fetch_weather_data(lat, lon)
            print("Weather Service Test Successful!")
            print(f"Got {result['metadata']['valid_records']} valid records")
            if result['sample_data']:
                print("Sample data:", result['sample_data'][0])
            else:
                print("No valid data found")
        except Exception as e:
            print(f"Test failed for {lat}, {lon}: {e}")

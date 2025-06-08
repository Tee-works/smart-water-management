"""
Fixed OpenWeatherMap ingestion to get more air quality data
"""

import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
import os
import time

logger = logging.getLogger(__name__)

class EnhancedOpenWeatherIngestion:
    """Enhanced OpenWeatherMap data ingestion with historical and forecast data"""
    
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5"
        
        # Multiple London locations for better coverage
        self.london_locations = [
            {"name": "Westminster", "lat": 51.4975, "lon": -0.1357, "district": "Central"},
            {"name": "Greenwich", "lat": 51.4892, "lon": 0.0648, "district": "South"},
            {"name": "Camden", "lat": 51.5290, "lon": -0.1255, "district": "North"},
            {"name": "Canary Wharf", "lat": 51.5055, "lon": -0.0196, "district": "East"},
            {"name": "Heathrow", "lat": 51.4700, "lon": -0.4543, "district": "West"}
        ]
    
    def get_current_air_quality(self, location):
        """Get current air quality for a location"""
        url = f"{self.base_url}/air_pollution"
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'appid': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get air quality for {location['name']}: {e}")
            return None
    
    def get_air_quality_forecast(self, location):
        """Get air quality forecast (up to 5 days)"""
        url = f"{self.base_url}/air_pollution/forecast"
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'appid': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get air quality forecast for {location['name']}: {e}")
            return None
    
    def get_air_quality_history(self, location, start_unix, end_unix):
        """Get historical air quality data"""
        url = f"{self.base_url}/air_pollution/history"
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'start': start_unix,
            'end': end_unix,
            'appid': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get air quality history for {location['name']}: {e}")
            return None
    
    def get_current_weather(self, location):
        """Get current weather data including temperature, humidity, pressure"""
        url = f"{self.base_url}/weather"
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get weather for {location['name']}: {e}")
            return None
    
    def transform_air_quality_data(self, data, location, data_type='current'):
        """Transform air quality data to standard format"""
        records = []
        
        if not data or 'list' not in data:
            return records
        
        for item in data['list']:
            # Air Quality Index
            record = {
                'timestamp': datetime.fromtimestamp(item['dt']),
                'sensor_id': f"AQ_{location['name'].upper()}_{item['dt']}",
                'sensor_type': 'air_quality',
                'district': location['district'],
                'location_name': location['name'],
                'latitude': location['lat'],
                'longitude': location['lon'],
                'value': item['main']['aqi'],
                'unit': 'AQI',
                'quality_score': min(1.0, (6 - item['main']['aqi']) / 5),
                'anomaly_flag': 1 if item['main']['aqi'] > 4 else 0,
                'data_source': 'OpenWeatherMap_API',
                'ingestion_timestamp': datetime.now(),
                'data_type': data_type
            }
            records.append(record)
            
            # Also extract individual pollutants
            components = item.get('components', {})
            pollutants = {
                'co': ('carbon_monoxide', 'μg/m³'),
                'no': ('nitric_oxide', 'μg/m³'),
                'no2': ('nitrogen_dioxide', 'μg/m³'),
                'o3': ('ozone', 'μg/m³'),
                'so2': ('sulfur_dioxide', 'μg/m³'),
                'pm2_5': ('pm2.5', 'μg/m³'),
                'pm10': ('pm10', 'μg/m³'),
                'nh3': ('ammonia', 'μg/m³')
            }
            
            for key, (pollutant_name, unit) in pollutants.items():
                if key in components:
                    records.append({
                        'timestamp': datetime.fromtimestamp(item['dt']),
                        'sensor_id': f"{key.upper()}_{location['name'].upper()}_{item['dt']}",
                        'sensor_type': pollutant_name,
                        'district': location['district'],
                        'location_name': location['name'],
                        'latitude': location['lat'],
                        'longitude': location['lon'],
                        'value': components[key],
                        'unit': unit,
                        'quality_score': 0.95,  # Generally good quality data
                        'anomaly_flag': 0,
                        'data_source': 'OpenWeatherMap_API',
                        'ingestion_timestamp': datetime.now(),
                        'data_type': data_type
                    })
        
        return records
    
    def transform_weather_data(self, data, location):
        """Transform weather data to standard format"""
        records = []
        
        if not data:
            return records
        
        timestamp = datetime.fromtimestamp(data['dt'])
        
        # Temperature
        if 'temp' in data['main']:
            records.append({
                'timestamp': timestamp,
                'sensor_id': f"TEMP_{location['name'].upper()}_{data['dt']}",
                'sensor_type': 'temperature',
                'district': location['district'],
                'location_name': location['name'],
                'latitude': location['lat'],
                'longitude': location['lon'],
                'value': data['main']['temp'],
                'unit': '°C',
                'quality_score': 0.98,
                'anomaly_flag': 0,
                'data_source': 'OpenWeatherMap_API',
                'ingestion_timestamp': datetime.now()
            })
        
        # Humidity
        if 'humidity' in data['main']:
            records.append({
                'timestamp': timestamp,
                'sensor_id': f"HUM_{location['name'].upper()}_{data['dt']}",
                'sensor_type': 'humidity',
                'district': location['district'],
                'location_name': location['name'],
                'latitude': location['lat'],
                'longitude': location['lon'],
                'value': data['main']['humidity'],
                'unit': '%',
                'quality_score': 0.98,
                'anomaly_flag': 0,
                'data_source': 'OpenWeatherMap_API',
                'ingestion_timestamp': datetime.now()
            })
        
        # Pressure
        if 'pressure' in data['main']:
            records.append({
                'timestamp': timestamp,
                'sensor_id': f"PRES_{location['name'].upper()}_{data['dt']}",
                'sensor_type': 'pressure',
                'district': location['district'],
                'location_name': location['name'],
                'latitude': location['lat'],
                'longitude': location['lon'],
                'value': data['main']['pressure'],
                'unit': 'hPa',
                'quality_score': 0.98,
                'anomaly_flag': 0,
                'data_source': 'OpenWeatherMap_API',
                'ingestion_timestamp': datetime.now()
            })
        
        return records
    
    def ingest_comprehensive_data(self, include_history=True, history_days=7):
        """Ingest comprehensive data from OpenWeatherMap"""
        all_records = []
        
        if not self.api_key:
            logger.error("No OpenWeatherMap API key found")
            return pd.DataFrame()
        
        for location in self.london_locations:
            logger.info(f"Ingesting data for {location['name']}")
            
            # Current air quality
            current_aq = self.get_current_air_quality(location)
            if current_aq:
                records = self.transform_air_quality_data(current_aq, location, 'current')
                all_records.extend(records)
            
            # Air quality forecast
            forecast_aq = self.get_air_quality_forecast(location)
            if forecast_aq:
                records = self.transform_air_quality_data(forecast_aq, location, 'forecast')
                all_records.extend(records)
            
            # Historical air quality (if requested)
            if include_history:
                end_time = datetime.now()
                start_time = end_time - timedelta(days=history_days)
                
                history_aq = self.get_air_quality_history(
                    location,
                    int(start_time.timestamp()),
                    int(end_time.timestamp())
                )
                if history_aq:
                    records = self.transform_air_quality_data(history_aq, location, 'historical')
                    all_records.extend(records)
            
            # Current weather
            current_weather = self.get_current_weather(location)
            if current_weather:
                records = self.transform_weather_data(current_weather, location)
                all_records.extend(records)
            
            # Rate limiting
            time.sleep(0.5)
        
        # Convert to DataFrame
        if all_records:
            df = pd.DataFrame(all_records)
            logger.info(f"Successfully ingested {len(df)} OpenWeatherMap records")
            return df
        else:
            logger.warning("No OpenWeatherMap data retrieved")
            return pd.DataFrame()

# Update the main ingestion class to use this enhanced version
def enhance_openweather_ingestion(df_existing):
    """Enhance existing data with more OpenWeatherMap data"""
    logger.info("Enhancing with comprehensive OpenWeatherMap data...")
    
    ingester = EnhancedOpenWeatherIngestion()
    openweather_df = ingester.ingest_comprehensive_data(include_history=True, history_days=7)
    
    if not openweather_df.empty:
        # Combine with existing data
        combined_df = pd.concat([df_existing, openweather_df], ignore_index=True)
        logger.info(f"Added {len(openweather_df)} OpenWeatherMap records")
        return combined_df
    else:
        return df_existing

if __name__ == "__main__":
    # Test the enhanced ingestion
    logging.basicConfig(level=logging.INFO)
    
    ingester = EnhancedOpenWeatherIngestion()
    df = ingester.ingest_comprehensive_data(include_history=False, history_days=1)
    
    if not df.empty:
        print(f"\nSuccessfully ingested {len(df)} records")
        print("\nSensor type breakdown:")
        print(df['sensor_type'].value_counts())
        print("\nLocation breakdown:")
        print(df['location_name'].value_counts())
        print("\nSample data:")
        print(df.head())
        
        # Save to CSV
        output_file = f"data/openweather_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
    else:
        print("No data retrieved")
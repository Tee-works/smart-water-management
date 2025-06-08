"""
Enhanced Unified Production Data Ingestion System
Phase 1: More Data - Historical backfill and multi-location support
"""

import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import time
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class UnifiedDataIngestionPlatform:
    """
    Enhanced production-grade unified data ingestion platform
    - Orchestrates multiple data sources with historical support
    - Multi-location weather data collection
    - Implements circuit breakers and retry logic
    - Provides comprehensive monitoring
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WaterManagementPlatform/1.0',
            'Accept': 'application/json'
        })
        
        # Enhanced data source configurations
        self.data_sources = {
            'london_air_quality': {
                'enabled': True,
                'refresh_minutes': 60,
                'priority': 'medium',
                'api_key_required': True,
                'fallback_to_mock': True
            },
            'london_weather': {
                'enabled': True,
                'refresh_minutes': 30,
                'priority': 'high',
                'api_key_required': False,
                'fallback_to_mock': False
            },
            'thames_water_monitoring': {
                'enabled': True,
                'refresh_minutes': 15,
                'priority': 'high',
                'api_key_required': False,
                'fallback_to_mock': True
            },
            'london_traffic_sensors': {
                'enabled': False,
                'refresh_minutes': 10,
                'priority': 'low',
                'api_key_required': True,
                'fallback_to_mock': True
            }
        }
        
        # Enhanced configuration
        self.historical_mode = False
        self.historical_days = 365
        self.multi_location_mode = True
        
        # London locations for expanded data collection
        self.london_locations = [
            {'name': 'Westminster', 'lat': 51.4975, 'lon': -0.1357, 'district': 'Central'},
            {'name': 'Camden', 'lat': 51.5290, 'lon': -0.1255, 'district': 'North'},
            {'name': 'Islington', 'lat': 51.5362, 'lon': -0.1033, 'district': 'North'},
            {'name': 'Tower_Hamlets', 'lat': 51.5203, 'lon': -0.0293, 'district': 'East'},
            {'name': 'Greenwich', 'lat': 51.4892, 'lon': 0.0648, 'district': 'South'},
            {'name': 'Southwark', 'lat': 51.5032, 'lon': -0.0851, 'district': 'South'},
            {'name': 'Hackney', 'lat': 51.5450, 'lon': -0.0553, 'district': 'North'},
            {'name': 'Lambeth', 'lat': 51.4816, 'lon': -0.1200, 'district': 'South'},
            {'name': 'Wandsworth', 'lat': 51.4571, 'lon': -0.1915, 'district': 'South'},
            {'name': 'Hammersmith', 'lat': 51.4927, 'lon': -0.2339, 'district': 'West'},
            {'name': 'Kensington', 'lat': 51.5020, 'lon': -0.1947, 'district': 'West'},
            {'name': 'Brent', 'lat': 51.5673, 'lon': -0.2714, 'district': 'West'},
            {'name': 'Ealing', 'lat': 51.5130, 'lon': -0.3089, 'district': 'West'},
            {'name': 'Barnet', 'lat': 51.6252, 'lon': -0.2000, 'district': 'North'},
            {'name': 'Enfield', 'lat': 51.6538, 'lon': -0.0799, 'district': 'North'},
            {'name': 'Bromley', 'lat': 51.4039, 'lon': 0.0140, 'district': 'South'},
            {'name': 'Croydon', 'lat': 51.3727, 'lon': -0.1099, 'district': 'South'},
            {'name': 'Redbridge', 'lat': 51.5590, 'lon': 0.0741, 'district': 'East'},
            {'name': 'Havering', 'lat': 51.5812, 'lon': 0.2120, 'district': 'East'},
            {'name': 'Newham', 'lat': 51.5077, 'lon': 0.0469, 'district': 'East'}
        ]
        
        # Circuit breaker states
        self.circuit_breakers = {}
        self.initialize_circuit_breakers()
    
    def initialize_circuit_breakers(self):
        """Initialize circuit breakers for each data source"""
        for source_name in self.data_sources.keys():
            self.circuit_breakers[source_name] = {
                'state': 'CLOSED',
                'failure_count': 0,
                'last_failure_time': None,
                'failure_threshold': 3,
                'recovery_timeout': 300
            }
    
    def check_circuit_breaker(self, source_name: str) -> bool:
        """Check if circuit breaker allows requests"""
        breaker = self.circuit_breakers[source_name]
        
        if breaker['state'] == 'OPEN':
            if (datetime.now() - breaker['last_failure_time']).seconds > breaker['recovery_timeout']:
                breaker['state'] = 'HALF_OPEN'
                self.logger.info(f"Circuit breaker for {source_name} moved to HALF_OPEN")
                return True
            return False
        
        return True
    
    def record_success(self, source_name: str):
        """Record successful API call"""
        breaker = self.circuit_breakers[source_name]
        breaker['failure_count'] = 0
        breaker['state'] = 'CLOSED'
    
    def record_failure(self, source_name: str):
        """Record failed API call"""
        breaker = self.circuit_breakers[source_name]
        breaker['failure_count'] += 1
        breaker['last_failure_time'] = datetime.now()
        
        if breaker['failure_count'] >= breaker['failure_threshold']:
            breaker['state'] = 'OPEN'
            self.logger.warning(f"Circuit breaker for {source_name} opened due to failures")
    
    def ingest_london_air_quality(self) -> pd.DataFrame:
        """Ingest London air quality data"""
        source_name = 'london_air_quality'
        
        if not self.check_circuit_breaker(source_name):
            self.logger.warning(f"Circuit breaker open for {source_name}, skipping")
            return self._generate_mock_air_quality()
        
        try:
            lat, lon = 51.5074, -0.1278
            api_key = "api_key"
         
            if not api_key:
                self.logger.warning("No OpenWeather API key, using mock data")
                return self._generate_mock_air_quality()
            

            url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}'
            params = {'lat': lat, 'lon': lon, 'appid': api_key}
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            result_df = self._transform_air_quality_data(data)
            
            self.record_success(source_name)
            self.logger.info(f"Successfully ingested {len(result_df)} air quality records")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Air quality ingestion failed: {e}")
            self.record_failure(source_name)
            
            if self.data_sources[source_name]['fallback_to_mock']:
                self.logger.info("Falling back to mock air quality data")
                return self._generate_mock_air_quality()
            else:
                return pd.DataFrame()
    
    def ingest_london_weather(self) -> pd.DataFrame:
        """Ingest current London weather data (original method)"""
        source_name = 'london_weather'
        
        if not self.check_circuit_breaker(source_name):
            self.logger.warning(f"Circuit breaker open for {source_name}, skipping")
            return pd.DataFrame()
        
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': 51.5074,
                'longitude': -0.1278,
                'current_weather': True,
                'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m',
                'forecast_days': 1
            }
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            result_df = self._transform_weather_data(data)
            
            self.record_success(source_name)
            self.logger.info(f"Successfully ingested {len(result_df)} weather records")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Weather ingestion failed: {e}")
            self.record_failure(source_name)
            return pd.DataFrame()
    
    def ingest_london_weather_historical(self, days_back: int = 365) -> pd.DataFrame:
        """Ingest historical weather data from OpenMeteo"""
        source_name = 'london_weather'
        
        if not self.check_circuit_breaker(source_name):
            return pd.DataFrame()
        
        try:
            all_records = []
            batch_size = 7  # Process 7 days at a time
            
            # Calculate safe date range (avoid future dates and API limits)
            today = datetime.now().date()
            max_allowed_date = datetime(2025, 6, 5).date()  # API's max date
            
            # Use the earlier of today or API max date
            end_date = min(today, max_allowed_date)
            
            for batch_start in range(0, days_back, batch_size):
                batch_end = min(batch_start + batch_size, days_back)
                
                # Process batch
                for i in range(batch_start, batch_end):
                    # Calculate date going backwards from safe end date
                    target_date = end_date - timedelta(days=i+1)  # +1 to avoid today
                    
                    # Skip if date is too old (before 1940)
                    if target_date.year < 1940:
                        self.logger.warning(f"Skipping date {target_date} - before API minimum (1940-01-01)")
                        continue
                    
                    date_str = target_date.strftime('%Y-%m-%d')
                    
                    # OpenMeteo Historical API
                    url = "https://archive-api.open-meteo.com/v1/archive"
                    params = {
                        'latitude': 51.5074,
                        'longitude': -0.1278,
                        'start_date': date_str,
                        'end_date': date_str,
                        'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,pressure_msl'
                    }
                    
                    self.logger.debug(f"Fetching historical data for {date_str}")
                    
                    response = self.session.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    daily_records = self._transform_historical_weather_data(data, target_date)
                    all_records.extend(daily_records)
                    
                    # Rate limiting
                    time.sleep(0.1)
                
                # Progress logging
                self.logger.info(f"Processed {batch_end}/{days_back} days of historical data")
                time.sleep(1)  # Longer pause between batches
            
            result_df = pd.DataFrame(all_records)
            self.record_success(source_name)
            self.logger.info(f"Successfully ingested {len(result_df)} historical weather records")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Historical weather ingestion failed: {e}")
            self.record_failure(source_name)
            return pd.DataFrame()
    
    def ingest_multi_location_weather(self) -> pd.DataFrame:
        """Ingest weather data from multiple London locations"""
        source_name = 'london_weather'
        
        if not self.check_circuit_breaker(source_name):
            return pd.DataFrame()
        
        try:
            all_records = []
            
            for location in self.london_locations:
                url = "https://api.open-meteo.com/v1/forecast"
                params = {
                    'latitude': location['lat'],
                    'longitude': location['lon'],
                    'current_weather': True,
                    'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m',
                    'forecast_days': 1
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                location_records = self._transform_location_weather_data(data, location)
                all_records.extend(location_records)
                
                # Rate limiting
                time.sleep(0.2)
            
            result_df = pd.DataFrame(all_records)
            self.record_success(source_name)
            self.logger.info(f"Successfully ingested {len(result_df)} multi-location weather records")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Multi-location weather ingestion failed: {e}")
            self.record_failure(source_name)
            return pd.DataFrame()
    
    def ingest_thames_water_monitoring(self, max_stations: int = 50) -> pd.DataFrame:
        """Ingest Thames water monitoring data (enhanced with more stations)"""
        source_name = 'thames_water_monitoring'
        
        if not self.check_circuit_breaker(source_name):
            self.logger.warning(f"Circuit breaker open for {source_name}, skipping")
            return self._generate_mock_water_data()
        
        try:
            self.logger.info("Ingesting Thames water monitoring data...")
            
            # Get Thames stations
            stations_url = "https://environment.data.gov.uk/flood-monitoring/id/stations"
            response = self.session.get(stations_url, timeout=30)
            response.raise_for_status()
            
            stations_data = response.json()
            all_stations = stations_data.get('items', [])
            
            # Filter for Thames stations
            thames_stations = [
                station for station in all_stations
                if self._is_thames_station(station)
            ][:max_stations]
            
            all_records = []
            
            # Process each station
            for station in thames_stations:
                try:
                    station_records = self._get_station_data(station)
                    all_records.extend(station_records)
                    time.sleep(0.3)  # Rate limiting
                except Exception as e:
                    self.logger.warning(f"Failed to process station {station.get('label', 'Unknown')}: {e}")
                    continue
            
            if all_records:
                result_df = pd.DataFrame(all_records)
                result_df['batch_id'] = f"thames_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                self.record_success(source_name)
                self.logger.info(f"Successfully ingested {len(result_df)} Thames monitoring records")
                return result_df
            else:
                raise Exception("No Thames data retrieved")
                
        except Exception as e:
            self.logger.error(f"Thames water ingestion failed: {e}")
            self.record_failure(source_name)
            
            if self.data_sources[source_name]['fallback_to_mock']:
                self.logger.info("Falling back to mock Thames data")
                return self._generate_mock_water_data()
            else:
                return pd.DataFrame()
    
    def _is_thames_station(self, station: Dict) -> bool:
        """Check if station is Thames-related"""
        try:
            river_name = str(station.get('riverName', '')).lower()
            catchment_name = str(station.get('catchmentName', '')).lower()
            label = str(station.get('label', '')).lower()
            station_ref = str(station.get('stationReference', ''))
            
            thames_indicators = [
                'thames' in river_name,
                'thames' in catchment_name,
                'thames' in label,
                station_ref.endswith('TH')
            ]
            
            return any(thames_indicators)
            
        except Exception as e:
            self.logger.warning(f"Error checking station {station.get('stationReference', 'unknown')}: {e}")
            return False
    
    def _get_station_data(self, station: Dict) -> List[Dict]:
        """Get data for a specific Thames station"""
        station_ref = station.get('stationReference')
        
        readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_ref}/readings"
        params = {'latest': 'true', '_limit': 5}
        
        response = self.session.get(readings_url, params=params, timeout=20)
        response.raise_for_status()
        
        readings_data = response.json()
        readings = readings_data.get('items', [])
        
        records = []
        for reading in readings:
            record = {
                'timestamp': self._parse_datetime(reading.get('dateTime')),
                'sensor_id': f"UK_EA_{station_ref}",
                'sensor_type': 'water_level',
                'district': self._map_location_to_district(station),
                'value': reading.get('value'),
                'unit': 'meters',
                'quality_score': 0.95,
                'anomaly_flag': 0,
                'data_source': 'UK_Environment_Agency',
                'ingestion_timestamp': datetime.now(),
                'station_name': station.get('label', 'Unknown'),
                'catchment': station.get('catchmentName', 'Unknown')
            }
            records.append(record)
        
        return records
    
    def run_unified_ingestion(self) -> pd.DataFrame:
        """Enhanced unified data ingestion with historical and multi-location support"""
        self.logger.info("🌊 Starting enhanced unified data ingestion pipeline")
        
        ingestion_start = datetime.now()
        all_dataframes = []
        source_stats = {}
        
        enabled_sources = {
            name: config for name, config in self.data_sources.items() 
            if config['enabled']
        }
        
        priority_order = {'high': 1, 'medium': 2, 'low': 3}
        sorted_sources = sorted(
            enabled_sources.items(), 
            key=lambda x: priority_order.get(x[1]['priority'], 4)
        )
        
        self.logger.info(f"Processing {len(sorted_sources)} data sources in priority order")
        
        # Process each source
        for source_name, source_config in sorted_sources:
            try:
                self.logger.info(f"📡 Ingesting from: {source_name}")
                source_start = datetime.now()
                
                # Enhanced routing with more options
                if source_name == 'london_air_quality':
                    df = self.ingest_london_air_quality()
                
                elif source_name == 'london_weather':
                    dfs_to_combine = []
                    
                    # Historical data if enabled
                    if self.historical_mode:
                        self.logger.info(f"📅 Fetching {self.historical_days} days of historical data...")
                        hist_df = self.ingest_london_weather_historical(self.historical_days)
                        if not hist_df.empty:
                            dfs_to_combine.append(hist_df)
                    
                    # Current data (original method)
                    current_df = self.ingest_london_weather()
                    if not current_df.empty:
                        dfs_to_combine.append(current_df)
                    
                    # Multi-location data if enabled
                    if self.multi_location_mode:
                        multi_df = self.ingest_multi_location_weather()
                        if not multi_df.empty:
                            dfs_to_combine.append(multi_df)
                    
                    # Combine all weather data
                    df = pd.concat(dfs_to_combine, ignore_index=True) if dfs_to_combine else pd.DataFrame()
                
                elif source_name == 'thames_water_monitoring':
                    df = self.ingest_thames_water_monitoring(max_stations=50)
                
                else:
                    self.logger.warning(f"Unknown source: {source_name}")
                    continue
                
                # Record stats
                source_duration = (datetime.now() - source_start).total_seconds()
                source_stats[source_name] = {
                    'records': len(df),
                    'duration_seconds': source_duration,
                    'status': 'success' if not df.empty else 'no_data'
                }
                
                if not df.empty:
                    df['ingestion_source'] = source_name
                    df['ingestion_priority'] = source_config['priority']
                    all_dataframes.append(df)
                    
                    self.logger.info(f"✅ {source_name}: {len(df)} records in {source_duration:.1f}s")
                else:
                    self.logger.warning(f"⚠️ {source_name}: No data retrieved")
                
            except Exception as e:
                self.logger.error(f"❌ {source_name} failed: {e}")
                source_stats[source_name] = {
                    'records': 0,
                    'duration_seconds': 0,
                    'status': 'failed',
                    'error': str(e)
                }
        
        # Combine all data
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            combined_df['unified_batch_id'] = f"unified_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            combined_df['pipeline_version'] = '2.0.0'
            
            total_duration = (datetime.now() - ingestion_start).total_seconds()
            total_records = len(combined_df)
            
            self.logger.info("🎯 ENHANCED UNIFIED INGESTION COMPLETE")
            self.logger.info(f"📊 Total Records: {total_records:,}")
            self.logger.info(f"⏱️ Total Duration: {total_duration:.1f} seconds")
            self.logger.info(f"🔧 Data Sources: {list(combined_df['ingestion_source'].unique())}")
            self.logger.info(f"🌐 Sensor Types: {list(combined_df['sensor_type'].unique())}")
            self.logger.info(f"📍 Districts: {list(combined_df['district'].unique())}")
            
            # Enhanced source breakdown
            for source, stats in source_stats.items():
                status_emoji = "✅" if stats['status'] == 'success' else "⚠️" if stats['status'] == 'no_data' else "❌"
                self.logger.info(f"  {status_emoji} {source}: {stats['records']} records")
            
            return combined_df
        else:
            self.logger.error("❌ No data ingested from any source")
            return pd.DataFrame()
    
    # Helper transformation methods
    def _transform_air_quality_data(self, data: Dict) -> pd.DataFrame:
        """Transform air quality API response"""
        records = []
        if 'list' in data:
            for reading in data['list']:
                record = {
                    'timestamp': datetime.fromtimestamp(reading['dt']),
                    'sensor_id': f'AQ_LONDON_{reading.get("dt", "unknown")}',
                    'sensor_type': 'air_quality',
                    'district': 'Central',
                    'value': reading['main']['aqi'],
                    'unit': 'AQI',
                    'quality_score': min(1.0, 6 - reading['main']['aqi']) / 5,
                    'anomaly_flag': 1 if reading['main']['aqi'] > 4 else 0,
                    'data_source': 'OpenWeatherMap_API',
                    'ingestion_timestamp': datetime.now()
                }
                records.append(record)
        return pd.DataFrame(records)
    
    def _transform_weather_data(self, data: Dict) -> pd.DataFrame:
        """Transform weather API response"""
        records = []
        if 'hourly' in data:
            hourly_data = data['hourly']
            times = hourly_data.get('time', [])
            temps = hourly_data.get('temperature_2m', [])
            
            for i, time_str in enumerate(times[:24]):
                if i < len(temps):
                    record = {
                        'timestamp': datetime.fromisoformat(time_str.replace('T', ' ')),
                        'sensor_id': f'WX_LONDON_{i:02d}',
                        'sensor_type': 'temperature',
                        'district': 'Central',
                        'value': temps[i],
                        'unit': '°C',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if temps[i] > 35 or temps[i] < -10 else 0,
                        'data_source': 'OpenMeteo_API',
                        'ingestion_timestamp': datetime.now()
                    }
                    records.append(record)
        return pd.DataFrame(records)
    
    def _transform_historical_weather_data(self, data: Dict, target_date: datetime.date) -> List[Dict]:
        """Transform historical weather API response"""
        records = []
        if 'hourly' in data:
            hourly_data = data['hourly']
            times = hourly_data.get('time', [])
            temps = hourly_data.get('temperature_2m', [])
            humidity = hourly_data.get('relative_humidity_2m', [])
            precipitation = hourly_data.get('precipitation', [])
            wind_speed = hourly_data.get('wind_speed_10m', [])
            pressure = hourly_data.get('pressure_msl', [])
            
            for i, time_str in enumerate(times):
                timestamp = datetime.fromisoformat(time_str.replace('T', ' '))
                
                # Temperature sensor
                if i < len(temps) and temps[i] is not None:
                    records.append({
                        'timestamp': timestamp,
                        'sensor_id': f'WX_HIST_TEMP_{target_date.strftime("%Y%m%d")}_{i:02d}',
                        'sensor_type': 'temperature',
                        'district': 'Central',
                        'value': temps[i],
                        'unit': '°C',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if temps[i] > 35 or temps[i] < -10 else 0,
                        'data_source': 'OpenMeteo_Historical_API',
                        'ingestion_timestamp': datetime.now()
                    })
                
                # Humidity sensor
                if i < len(humidity) and humidity[i] is not None:
                    records.append({
                        'timestamp': timestamp,
                        'sensor_id': f'WX_HIST_HUMID_{target_date.strftime("%Y%m%d")}_{i:02d}',
                        'sensor_type': 'humidity',
                        'district': 'Central',
                        'value': humidity[i],
                        'unit': '%',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if humidity[i] > 95 or humidity[i] < 10 else 0,
                        'data_source': 'OpenMeteo_Historical_API',
                        'ingestion_timestamp': datetime.now()
                    })
                
                # Precipitation sensor
                if i < len(precipitation) and precipitation[i] is not None:
                    records.append({
                        'timestamp': timestamp,
                        'sensor_id': f'WX_HIST_RAIN_{target_date.strftime("%Y%m%d")}_{i:02d}',
                        'sensor_type': 'precipitation',
                        'district': 'Central',
                        'value': precipitation[i],
                        'unit': 'mm',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if precipitation[i] > 50 else 0,
                        'data_source': 'OpenMeteo_Historical_API',
                        'ingestion_timestamp': datetime.now()
                    })
        
        return records
    
    def _transform_location_weather_data(self, data: Dict, location: Dict) -> List[Dict]:
        """Transform weather data for specific location"""
        records = []
        if 'hourly' in data:
            hourly_data = data['hourly']
            times = hourly_data.get('time', [])
            temps = hourly_data.get('temperature_2m', [])
            humidity = hourly_data.get('relative_humidity_2m', [])
            precipitation = hourly_data.get('precipitation', [])
            
            for i, time_str in enumerate(times[:24]):
                timestamp = datetime.fromisoformat(time_str.replace('T', ' '))
                
                # Temperature
                if i < len(temps):
                    records.append({
                        'timestamp': timestamp,
                        'sensor_id': f'WX_{location["name"].upper()}_TEMP_{i:02d}',
                        'sensor_type': 'temperature',
                        'district': location['district'],
                        'value': temps[i],
                        'unit': '°C',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if temps[i] > 35 or temps[i] < -10 else 0,
                        'data_source': 'OpenMeteo_MultiLocation_API',
                        'ingestion_timestamp': datetime.now(),
                        'location_name': location['name'],
                        'latitude': location['lat'],
                        'longitude': location['lon']
                    })
                
                # Humidity
                if i < len(humidity):
                    records.append({
                        'timestamp': timestamp,
                        'sensor_id': f'WX_{location["name"].upper()}_HUMID_{i:02d}',
                        'sensor_type': 'humidity',
                        'district': location['district'],
                        'value': humidity[i],
                        'unit': '%',
                        'quality_score': 0.98,
                        'anomaly_flag': 1 if humidity[i] > 95 or humidity[i] < 10 else 0,
                        'data_source': 'OpenMeteo_MultiLocation_API',
                        'ingestion_timestamp': datetime.now(),
                        'location_name': location['name'],
                        'latitude': location['lat'],
                        'longitude': location['lon']
                    })
        
        return records
    
    def _parse_datetime(self, datetime_str: str) -> datetime:
        """Parse datetime string from API"""
        try:
            if datetime_str.endswith('Z'):
                return datetime.fromisoformat(datetime_str[:-1])
            else:
                return datetime.fromisoformat(datetime_str)
        except Exception:
            return datetime.now()
    
    def _map_location_to_district(self, station: Dict) -> str:
        """Map station location to district"""
        try:
            town = str(station.get('town', '')).lower()
            label = str(station.get('label', '')).lower()
            
            location_text = f"{town} {label}"
            
            if any(keyword in location_text for keyword in ['central', 'westminster', 'london']):
                return 'Central'
            elif 'kingston' in location_text:
                return 'South'
            elif 'reading' in location_text:
                return 'West'
            elif any(keyword in location_text for keyword in ['windsor', 'bray']):
                return 'West'
            else:
                return 'Thames Valley'
                
        except Exception as e:
            self.logger.warning(f"Error mapping location for station: {e}")
            return 'Thames Valley'
    
    # Mock data generators (fallbacks)
    def _generate_mock_air_quality(self) -> pd.DataFrame:
        """Generate mock air quality data"""
        import random
        records = []
        for i in range(5):
            record = {
                'timestamp': datetime.now() - timedelta(hours=i),
                'sensor_id': f'AQ_MOCK_{i:02d}',
                'sensor_type': 'air_quality',
                'district': random.choice(['Central', 'North', 'South']),
                'value': random.randint(1, 5),
                'unit': 'AQI',
                'quality_score': random.uniform(0.8, 1.0),
                'anomaly_flag': random.choices([0, 1], weights=[0.95, 0.05])[0],
                'data_source': 'MOCK_DATA',
                'ingestion_timestamp': datetime.now()
            }
            records.append(record)
        return pd.DataFrame(records)
    
    def _generate_mock_water_data(self) -> pd.DataFrame:
        """Generate mock water data"""
        import random
        records = []
        for i in range(10):
            record = {
                'timestamp': datetime.now() - timedelta(minutes=i*15),
                'sensor_id': f'WL_MOCK_{i:02d}',
                'sensor_type': 'water_level',
                'district': random.choice(['Central', 'South', 'West']),
                'value': random.uniform(1.0, 5.0),
                'unit': 'meters',
                'quality_score': 0.95,
                'anomaly_flag': 0,
                'data_source': 'MOCK_DATA',
                'ingestion_timestamp': datetime.now()
            }
            records.append(record)
        return pd.DataFrame(records)

def main():
    """Main entry point for enhanced unified data ingestion"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🌊 Smart Water Management - Enhanced Unified Data Ingestion Pipeline")
    print("=" * 75)
    
    # Initialize platform
    platform = UnifiedDataIngestionPlatform()
    
    # Run unified ingestion
    result_df = platform.run_unified_ingestion()
    
    if not result_df.empty:
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"data/unified_sensor_data_{timestamp}.csv"
        result_df.to_csv(output_file, index=False)
        
        print(f"\n🎉 ENHANCED INGESTION SUCCESS!")
        print(f"📁 File: {output_file}")
        print(f"📊 Records: {len(result_df):,}")
        print(f"🔧 Sources: {', '.join(result_df['ingestion_source'].unique())}")
        print(f"🌐 Sensor Types: {', '.join(result_df['sensor_type'].unique())}")
        print(f"📍 Districts: {', '.join(result_df['district'].unique())}")
        
        # Enhanced statistics
        source_breakdown = result_df.groupby('data_source').size()
        print(f"\n📋 Source Breakdown:")
        for source, count in source_breakdown.items():
            print(f"  • {source}: {count:,} records")
        
        district_breakdown = result_df.groupby('district').size()
        print(f"\n🌍 District Breakdown:")
        for district, count in district_breakdown.items():
            print(f"  • {district}: {count:,} records")
        
        # Show sample data
        print(f"\n📋 Sample Data:")
        sample_cols = ['timestamp', 'sensor_id', 'sensor_type', 'value', 'unit', 'data_source', 'district']
        print(result_df[sample_cols].head(10).to_string(index=False))
        
        return result_df
    else:
        print("❌ No data was successfully ingested")
        return None

if __name__ == "__main__":
    main()
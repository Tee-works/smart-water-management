"""
Unified Production Data Ingestion System
Combines all data sources into a single, orchestrated ingestion pipeline
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
    Production-grade unified data ingestion platform
    - Orchestrates multiple data sources
    - Handles different refresh frequencies
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
        
        # Data source configurations
        self.data_sources = {
            'london_air_quality': {
                'enabled': True,
                'refresh_minutes': 60,  # Hourly
                'priority': 'medium',
                'api_key_required': True,
                'fallback_to_mock': True
            },
            'london_weather': {
                'enabled': True,
                'refresh_minutes': 30,  # Every 30 minutes
                'priority': 'high',
                'api_key_required': False,
                'fallback_to_mock': False
            },
            'thames_water_monitoring': {
                'enabled': True,
                'refresh_minutes': 15,  # Every 15 minutes (government real-time data)
                'priority': 'high',
                'api_key_required': False,
                'fallback_to_mock': True
            },
            'london_traffic_sensors': {
                'enabled': False,  # Optional additional source
                'refresh_minutes': 10,
                'priority': 'low',
                'api_key_required': True,
                'fallback_to_mock': True
            }
        }
        
        # Circuit breaker states
        self.circuit_breakers = {}
        self.initialize_circuit_breakers()
    
    def initialize_circuit_breakers(self):
        """Initialize circuit breakers for each data source"""
        for source_name in self.data_sources.keys():
            self.circuit_breakers[source_name] = {
                'state': 'CLOSED',  # CLOSED, OPEN, HALF_OPEN
                'failure_count': 0,
                'last_failure_time': None,
                'failure_threshold': 3,
                'recovery_timeout': 300  # 5 minutes
            }
    
    def check_circuit_breaker(self, source_name: str) -> bool:
        """Check if circuit breaker allows requests"""
        breaker = self.circuit_breakers[source_name]
        
        if breaker['state'] == 'OPEN':
            # Check if recovery timeout has passed
            if (datetime.now() - breaker['last_failure_time']).seconds > breaker['recovery_timeout']:
                breaker['state'] = 'HALF_OPEN'
                self.logger.info(f"Circuit breaker for {source_name} moved to HALF_OPEN")
                return True
            return False
        
        return True  # CLOSED or HALF_OPEN
    
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
            # London coordinates
            lat, lon = 51.5074, -0.1278
            api_key = os.getenv('OPENWEATHER_API_KEY')
            
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
        """Ingest London weather data"""
        source_name = 'london_weather'
        
        if not self.check_circuit_breaker(source_name):
            self.logger.warning(f"Circuit breaker open for {source_name}, skipping")
            return pd.DataFrame()
        
        try:
            # Open-Meteo API (free, no key required)
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
    
    def ingest_thames_water_monitoring(self, max_stations: int = 10) -> pd.DataFrame:
        """Ingest Thames water monitoring data (comprehensive)"""
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
                    time.sleep(0.5)  # Rate limiting
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
        """Check if station is Thames-related (bug-fixed version)"""
        try:
            # Safely get string values with fallbacks
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
        
        # Get latest readings
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
                'quality_score': 0.95,  # Government data is high quality
                'anomaly_flag': 0,
                'data_source': 'UK_Environment_Agency',
                'ingestion_timestamp': datetime.now(),
                'station_name': station.get('label', 'Unknown'),
                'catchment': station.get('catchmentName', 'Unknown')
            }
            records.append(record)
        
        return records
    
    def run_unified_ingestion(self) -> pd.DataFrame:
        """
        Run comprehensive data ingestion from all enabled sources
        This is the main entry point for production use
        """
        self.logger.info("🌊 Starting unified data ingestion pipeline")
        
        # Track ingestion metrics
        ingestion_start = datetime.now()
        all_dataframes = []
        source_stats = {}
        
        # Get enabled sources sorted by priority
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
                
                # Route to appropriate ingestion method
                if source_name == 'london_air_quality':
                    df = self.ingest_london_air_quality()
                elif source_name == 'london_weather':
                    df = self.ingest_london_weather()
                elif source_name == 'thames_water_monitoring':
                    df = self.ingest_thames_water_monitoring()
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
                    # Add source metadata
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
            
            # Add unified metadata
            combined_df['unified_batch_id'] = f"unified_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            combined_df['pipeline_version'] = '1.0.0'
            
            # Calculate overall metrics
            total_duration = (datetime.now() - ingestion_start).total_seconds()
            total_records = len(combined_df)
            
            self.logger.info("🎯 UNIFIED INGESTION COMPLETE")
            self.logger.info(f"📊 Total Records: {total_records:,}")
            self.logger.info(f"⏱️ Total Duration: {total_duration:.1f} seconds")
            self.logger.info(f"🔧 Data Sources: {list(combined_df['ingestion_source'].unique())}")
            self.logger.info(f"🌐 Sensor Types: {list(combined_df['sensor_type'].unique())}")
            
            # Log source breakdown
            for source, stats in source_stats.items():
                status_emoji = "✅" if stats['status'] == 'success' else "⚠️" if stats['status'] == 'no_data' else "❌"
                self.logger.info(f"  {status_emoji} {source}: {stats['records']} records")
            
            return combined_df
        else:
            self.logger.error("❌ No data ingested from any source")
            return pd.DataFrame()
    
    # Helper methods from previous implementations
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
        """Map station location to district (bug-fixed version)"""
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
            return 'Thames Valley'  # Default fallback
    
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
    """Main entry point for unified data ingestion"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🌊 Smart Water Management - Unified Data Ingestion Pipeline")
    print("=" * 65)
    
    # Initialize platform
    platform = UnifiedDataIngestionPlatform()
    
    # Run unified ingestion
    result_df = platform.run_unified_ingestion()
    
    if not result_df.empty:
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"data/unified_sensor_data_{timestamp}.csv"
        result_df.to_csv(output_file, index=False)
        
        print(f"\n🎉 INGESTION SUCCESS!")
        print(f"📁 File: {output_file}")
        print(f"📊 Records: {len(result_df):,}")
        print(f"🔧 Sources: {', '.join(result_df['ingestion_source'].unique())}")
        print(f"🌐 Sensor Types: {', '.join(result_df['sensor_type'].unique())}")
        print(f"📍 Districts: {', '.join(result_df['district'].unique())}")
        
        # Show sample data
        print(f"\n📋 Sample Data:")
        sample_cols = ['timestamp', 'sensor_id', 'sensor_type', 'value', 'unit', 'data_source']
        print(result_df[sample_cols].head(10).to_string(index=False))
        
        return result_df
    else:
        print("❌ No data was successfully ingested")
        return None

if __name__ == "__main__":
    main()
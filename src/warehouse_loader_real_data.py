import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import glob
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealDataWarehouseLoader:
    """
    Loads your REAL API data (CSV files) into the existing dimensional warehouse
    Uses your existing schema - no changes needed!
    """
    
    def __init__(self):
        # Database connection (same as before)
        self.db_engine = create_engine(
            'postgresql://dataeng:pipeline123@localhost:5432/water_analytics'
        )
    
    def find_latest_data_file(self):
        """Find the latest unified sensor data file"""
        data_files = glob.glob('data/unified_sensor_data_enhanced_*.csv')
        if not data_files:
            logger.error("No unified sensor data files found!")
            return None
        
        # Get the most recent file
        latest_file = max(data_files, key=os.path.getctime)
        logger.info(f"Found latest data file: {latest_file}")
        return latest_file
    
    def create_schema_tables(self):
        """Create the dimensional schema in PostgreSQL"""
        logger.info("Creating dimensional schema tables")
        
        try:
            # First drop existing tables
            if not self.drop_existing_tables():
                return False
                
            # Read schema file and execute
            with open('sql/real_data_schema.sql', 'r') as f:
                schema_sql = f.read()
            
            # Split into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            # Use transaction context manager
            with self.db_engine.begin() as conn:
                for statement in statements:
                    if statement:
                        conn.execute(text(statement))
                # Transaction is automatically committed when exiting the context
            
            logger.info("Dimensional schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    def load_dimensions_from_real_data(self, df):
        """Load dimensions from your real CSV data"""
        logger.info("Loading dimensions from real data")
        
        try:
            # 1. Time dimension
            unique_dates = df['timestamp'].dt.date.unique()
            time_data = []
            for i, date in enumerate(sorted(unique_dates), 1):
                dt = datetime.combine(date, datetime.min.time())
                time_data.append({
                    'time_key': i,
                    'date_actual': date,
                    'year': dt.year,
                    'month': dt.month,
                    'day_of_month': dt.day,
                    'day_of_week': dt.weekday() + 1,
                    'day_name': dt.strftime('%A'),
                    'is_weekend': dt.weekday() >= 5
                })
            
            time_df = pd.DataFrame(time_data)
            time_df.to_sql('dim_time', self.db_engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(time_data)} time records")
            
            # 2. Locations dimension
            unique_districts = df['district'].unique()
            location_data = []
            for i, district in enumerate(unique_districts, 1):
                location_data.append({
                    'location_key': i,
                    'district_name': district,
                    'city': 'London'
                })
            
            location_df = pd.DataFrame(location_data)
            location_df.to_sql('dim_locations', self.db_engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(location_data)} location records")
            
            # 3. Sensor types dimension
            unique_sensor_types = df[['sensor_type', 'unit']].drop_duplicates()
            sensor_type_data = []
            for i, (_, row) in enumerate(unique_sensor_types.iterrows(), 1):
                sensor_type_data.append({
                    'sensor_type_key': i,
                    'sensor_type': row['sensor_type'],
                    'measurement_unit': row['unit']
                })
            
            sensor_type_df = pd.DataFrame(sensor_type_data)
            sensor_type_df.to_sql('dim_sensor_types', self.db_engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(sensor_type_data)} sensor type records")
            
            # 4. Sensors dimension
            unique_sensors = df[['sensor_id', 'sensor_type', 'district']].drop_duplicates()
            sensor_data = []
            for i, (_, row) in enumerate(unique_sensors.iterrows(), 1):
                sensor_data.append({
                    'sensor_key': i,
                    'sensor_id': row['sensor_id'],
                    'sensor_type': row['sensor_type'],
                    'district': row['district'],
                    'status': 'active'
                })
            
            sensor_df = pd.DataFrame(sensor_data)
            sensor_df.to_sql('dim_sensors', self.db_engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(sensor_data)} sensor records")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load dimensions: {e}")
            return False
    
    def load_fact_data_from_real_data(self, df):
        """Load fact data from your real CSV"""
        logger.info("Loading fact data from real CSV")
        
        try:
            # Get dimension mappings
            with self.db_engine.connect() as conn:
                sensors_dim = pd.read_sql("SELECT sensor_key, sensor_id FROM dim_sensors", conn)
                locations_dim = pd.read_sql("SELECT location_key, district_name FROM dim_locations", conn)
                sensor_types_dim = pd.read_sql("SELECT sensor_type_key, sensor_type FROM dim_sensor_types", conn)
                time_dim = pd.read_sql("SELECT time_key, date_actual FROM dim_time", conn)
            
            # Convert date column for matching
            time_dim['date_actual'] = pd.to_datetime(time_dim['date_actual']).dt.date
            
            fact_data = []
            
            for i, (_, row) in enumerate(df.iterrows(), 1):
                try:
                    # Get foreign keys
                    sensor_key = sensors_dim[sensors_dim['sensor_id'] == row['sensor_id']]['sensor_key'].iloc[0]
                    location_key = locations_dim[locations_dim['district_name'] == row['district']]['location_key'].iloc[0]
                    sensor_type_key = sensor_types_dim[sensor_types_dim['sensor_type'] == row['sensor_type']]['sensor_type_key'].iloc[0]
                    
                    # Find time key
                    reading_date = row['timestamp'].date()
                    time_key = time_dim[time_dim['date_actual'] == reading_date]['time_key'].iloc[0]
                    
                    fact_data.append({
                        'reading_key': i,
                        'sensor_key': sensor_key,
                        'time_key': time_key,
                        'location_key': location_key,
                        'sensor_type_key': sensor_type_key,
                        'reading_value': row['value'],
                        'quality_score': row.get('quality_score', 0.8),
                        'anomaly_flag': row.get('anomaly_flag', 0),
                        'reading_timestamp': row['timestamp'],
                        'data_source': row.get('data_source', 'API'),
                        'station_name': row.get('station_name', None),
                        'unified_batch_id': row.get('unified_batch_id', 'batch_001')
                    })
                    
                except (IndexError, KeyError) as e:
                    logger.warning(f"Skipping row {i}: {e}")
                    continue
            
            # Insert fact data
            if fact_data:
                fact_df = pd.DataFrame(fact_data)
                fact_df.to_sql('fact_sensor_readings', self.db_engine, if_exists='append', index=False)
                logger.info(f"Loaded {len(fact_data)} fact records")
                return True
            else:
                logger.error("No fact data to load")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load fact data: {e}")
            return False
    
    def generate_reports_from_real_data(self):
        """Generate reports from your real data"""
        logger.info("Generating reports from real data")
        
        try:
            with self.db_engine.connect() as conn:
                # District performance with real data
                district_report = pd.read_sql(text("""
                    SELECT 
                        l.district_name,
                        st.sensor_type,
                        COUNT(f.reading_key) as total_readings,
                        AVG(f.reading_value) as avg_reading,
                        AVG(f.quality_score) as avg_quality,
                        SUM(f.anomaly_flag) as anomaly_count,
                        f.data_source
                    FROM fact_sensor_readings f
                    JOIN dim_locations l ON f.location_key = l.location_key
                    JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
                    GROUP BY l.district_name, st.sensor_type, f.data_source
                    ORDER BY total_readings DESC
                """), conn)
                
                print("\n" + "="*80)
                print("🌊 REAL DATA DISTRICT PERFORMANCE REPORT")
                print("="*80)
                print(district_report.round(3))
                
                # Thames stations report
                thames_report = pd.read_sql(text("""
                    SELECT 
                        f.station_name,
                        l.district_name,
                        COUNT(f.reading_key) as readings,
                        AVG(f.reading_value) as avg_water_level,
                        f.data_source
                    FROM fact_sensor_readings f
                    JOIN dim_locations l ON f.location_key = l.location_key
                    WHERE f.station_name IS NOT NULL
                    GROUP BY f.station_name, l.district_name, f.data_source
                    ORDER BY avg_water_level DESC
                """), conn)
                
                if not thames_report.empty:
                    print("\n" + "="*80)
                    print("🏞️ THAMES WATER STATIONS REPORT")
                    print("="*80)
                    print(thames_report.round(3))
                
                # Real API sources summary
                sources_report = pd.read_sql(text("""
                    SELECT 
                        f.data_source,
                        COUNT(f.reading_key) as total_readings,
                        COUNT(DISTINCT s.sensor_id) as unique_sensors,
                        AVG(f.quality_score) as avg_quality
                    FROM fact_sensor_readings f
                    JOIN dim_sensors s ON f.sensor_key = s.sensor_key
                    GROUP BY f.data_source
                    ORDER BY total_readings DESC
                """), conn)
                
                print("\n" + "="*80)
                print("📡 REAL API DATA SOURCES SUMMARY")
                print("="*80)
                print(sources_report.round(3))
                
            return True
                
        except Exception as e:
            logger.error(f"Failed to generate reports: {e}")
            return False
    
    def execute_real_data_warehouse_load(self):
        """Execute warehouse loading for your real API data"""
        logger.info("🌊 Starting REAL DATA warehouse loading")
        
        # Find latest data file
        data_file = self.find_latest_data_file()
        if not data_file:
            return False
        
        # Load and prepare data
        try:
            df = pd.read_csv(data_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"Loaded {len(df)} records from {data_file}")
        except Exception as e:
            logger.error(f"Failed to load data file: {e}")
            return False
        
        # Execute steps
        steps = [
            ("Creating simple schema", lambda: self.create_simple_schema()),
            ("Loading dimensions", lambda: self.load_dimensions_from_real_data(df)),
            ("Loading fact data", lambda: self.load_fact_data_from_real_data(df)),
            ("Generating reports", lambda: self.generate_reports_from_real_data())
        ]
        
        for step_name, step_func in steps:
            logger.info(f"Executing: {step_name}")
            try:
                result = step_func()
                if not result:
                    logger.error(f"Step failed: {step_name}")
                    return False
            except Exception as e:
                logger.error(f"Step failed: {step_name} - {e}")
                return False
        
        logger.info("✅ Real data warehouse loading completed successfully!")
        return True

def main():
    """Load your real API data into the warehouse"""
    
    print("🌊 REAL DATA WAREHOUSE LOADER")
    print("="*60)
    print("Loading your actual API data into dimensional warehouse")
    print()
    
    loader = RealDataWarehouseLoader()
    success = loader.execute_real_data_warehouse_load()
    
    if success:
        print("\n🎉 SUCCESS: Real Data Warehouse Created!")
        print("\n✅ What was loaded:")
        print("• Your 41 real API records")
        print("• OpenMeteo weather data")
        print("• UK Environment Agency Thames data")
        print("• OpenWeather air quality data")
        print("• Proper dimensional modeling")
        print("• Business intelligence reports")
        print("\n🚀 Ready for dashboard!")
    else:
        print("❌ Real data warehouse loading failed")

if __name__ == "__main__":
    main()
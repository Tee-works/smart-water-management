import pandas as pd
import boto3
from sqlalchemy import create_engine, text
import psycopg2
from datetime import datetime, timedelta
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataWarehouseLoader:
    """
    Loads processed data from S3 into dimensional data warehouse.
    Demonstrates proper data modeling and ETL practices.
    """
    
    def __init__(self):
        # Database connection
        self.db_engine = create_engine(
            'postgresql://dataeng:pipeline123@localhost:5433/water_analytics'
        )
        self.s3_client = boto3.client('s3')
        self.processed_bucket = "water-project-processed"
    
    def drop_existing_tables(self):
        """Drop existing tables if they exist"""
        logger.info("Dropping existing tables")
        
        try:
            drop_statements = [
                "DROP TABLE IF EXISTS fact_sensor_readings CASCADE",
                "DROP TABLE IF EXISTS dim_sensors CASCADE",
                "DROP TABLE IF EXISTS dim_sensor_types CASCADE", 
                "DROP TABLE IF EXISTS dim_locations CASCADE",
                "DROP TABLE IF EXISTS dim_time CASCADE"
            ]
            
            with self.db_engine.begin() as conn:
                for statement in drop_statements:
                    conn.execute(text(statement))
            
            logger.info("Existing tables dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")
            return False

    def create_schema_tables(self):
        """Create the dimensional schema in PostgreSQL"""
        logger.info("Creating dimensional schema tables")
        
        try:
            # First drop existing tables
            if not self.drop_existing_tables():
                return False
                
            # Read schema file and execute
            with open('sql/dimensional_schema.sql', 'r') as f:
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
    
    def load_time_dimension(self):
        """Populate time dimension with date hierarchy"""
        logger.info("Loading time dimension")
        
        # Generate 2 years of dates
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2025, 12, 31)
        
        dates = []
        current_date = start_date
        time_key = 1
        
        while current_date <= end_date:
            dates.append({
                'time_key': time_key,
                'date_actual': current_date.date(),
                'year': current_date.year,
                'quarter': (current_date.month - 1) // 3 + 1,
                'month': current_date.month,
                'week_of_year': current_date.isocalendar()[1],
                'day_of_month': current_date.day,
                'day_of_week': current_date.weekday() + 1,
                'day_name': current_date.strftime('%A'),
                'month_name': current_date.strftime('%B'),
                'is_weekend': current_date.weekday() >= 5,
                'is_business_day': current_date.weekday() < 5,
                'season': self._get_season(current_date.month),
                'fiscal_year': current_date.year if current_date.month >= 4 else current_date.year - 1,
                'fiscal_quarter': ((current_date.month - 4) % 12) // 3 + 1
            })
            
            current_date += timedelta(days=1)
            time_key += 1
        
        # Load to database
        time_df = pd.DataFrame(dates)
        time_df.to_sql('dim_time', self.db_engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"Loaded {len(dates)} time dimension records")
        return True
    
    def _get_season(self, month):
        """Determine season based on month"""
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Autumn'
    
    def load_sensor_dimensions(self):
        """Load sensor and related dimensions from S3 data"""
        logger.info("Loading sensor dimensions")
        
        try:
            # Load sensor locations from S3
            today = datetime.now()
            date_path = f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
            
            # Get sensor data from your existing S3 files
            response = self.s3_client.get_object(
                Bucket=self.processed_bucket,
                Key=f'silver/clean_readings/{date_path}/cleaned_readings.csv'
            )
            
            readings_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            
            # Create sensor types dimension
            sensor_types = readings_df['sensor_type'].unique()
            sensor_types_data = []
            
            for i, sensor_type in enumerate(sensor_types, 1):
                sensor_data = readings_df[readings_df['sensor_type'] == sensor_type]
                
                sensor_types_data.append({
                    'sensor_type_key': i,
                    'sensor_type': sensor_type,
                    'category': 'IoT Sensor',
                    'measurement_unit': sensor_data['unit'].iloc[0] if 'unit' in sensor_data.columns else 'units',
                    'normal_min_value': sensor_data['value'].quantile(0.1),
                    'normal_max_value': sensor_data['value'].quantile(0.9),
                    'warning_min_value': sensor_data['value'].quantile(0.05),
                    'warning_max_value': sensor_data['value'].quantile(0.95),
                    'critical_min_value': sensor_data['value'].min(),
                    'critical_max_value': sensor_data['value'].max(),
                    'calibration_frequency_days': 30,
                    'description': f'{sensor_type} sensor for water management monitoring'
                })
            
            sensor_types_df = pd.DataFrame(sensor_types_data)
            sensor_types_df.to_sql('dim_sensor_types', self.db_engine, if_exists='append', index=False, method='multi')
            
            # Create locations dimension
            districts = readings_df['district'].unique()
            locations_data = []
            
            for i, district in enumerate(districts, 1):
                locations_data.append({
                    'location_key': i,
                    'district_id': district.lower().replace(' ', '_'),
                    'district_name': district,
                    'city': 'London',
                    'country': 'United Kingdom',
                    'infrastructure_type': 'Urban Water Network',
                    'priority_level': 'High'
                })
            
            locations_df = pd.DataFrame(locations_data)
            locations_df.to_sql('dim_locations', self.db_engine, if_exists='append', index=False, method='multi')
            
            # Create sensors dimension
            sensors = readings_df.groupby(['sensor_id', 'sensor_type', 'district']).first().reset_index()
            sensors_data = []
            
            for i, (_, sensor) in enumerate(sensors.iterrows(), 1):
                sensors_data.append({
                    'sensor_key': i,
                    'sensor_id': sensor['sensor_id'],
                    'sensor_type': sensor['sensor_type'],
                    'district': sensor['district'],
                    'latitude': 51.5074 + (i * 0.01),  # Simulate London coordinates
                    'longitude': -0.1278 + (i * 0.01),
                    'installation_date': datetime(2024, 1, 1).date(),
                    'manufacturer': 'WaterTech Systems',
                    'model': f'WT-{sensor["sensor_type"].upper()}-2024',
                    'status': 'active',
                    'effective_date': datetime(2024, 1, 1).date(),
                    'is_current': True
                })
            
            sensors_df = pd.DataFrame(sensors_data)
            sensors_df.to_sql('dim_sensors', self.db_engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Loaded {len(sensor_types_data)} sensor types, {len(locations_data)} locations, {len(sensors_data)} sensors")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load sensor dimensions: {e}")
            return False
    
    def load_fact_data(self):
        """Load fact tables from processed S3 data"""
        logger.info("Loading fact data")
        
        try:
            today = datetime.now()
            date_path = f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
            
            # Load clean readings
            response = self.s3_client.get_object(
                Bucket=self.processed_bucket,
                Key=f'silver/clean_readings/{date_path}/cleaned_readings.csv'
            )
            
            readings_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            readings_df['timestamp'] = pd.to_datetime(readings_df['timestamp'])
            
            # Get dimension keys for foreign key mapping
            with self.db_engine.connect() as conn:
                sensors_dim = pd.read_sql("SELECT sensor_key, sensor_id FROM dim_sensors", conn)
                locations_dim = pd.read_sql("SELECT location_key, district_name FROM dim_locations", conn)
                sensor_types_dim = pd.read_sql("SELECT sensor_type_key, sensor_type FROM dim_sensor_types", conn)
                time_dim = pd.read_sql("SELECT time_key, date_actual FROM dim_time", conn)
            
            # Convert time_dim date column to datetime for comparison
            time_dim['date_actual'] = pd.to_datetime(time_dim['date_actual'])
            
            # Prepare fact data in batches
            batch_size = 1000
            total_records = 0
            
            for i in range(0, len(readings_df), batch_size):
                batch_df = readings_df.iloc[i:i+batch_size].copy()
                fact_data = []
                
                for _, row in batch_df.iterrows():
                    try:
                        # Get foreign keys
                        sensor_key = sensors_dim[sensors_dim['sensor_id'] == row['sensor_id']]['sensor_key'].iloc[0]
                        location_key = locations_dim[locations_dim['district_name'] == row['district']]['location_key'].iloc[0]
                        sensor_type_key = sensor_types_dim[sensor_types_dim['sensor_type'] == row['sensor_type']]['sensor_type_key'].iloc[0]
                        
                        # Find matching time key
                        reading_date = row['timestamp'].date()
                        time_key_match = time_dim[time_dim['date_actual'].dt.date == reading_date]
                        if not time_key_match.empty:
                            time_key = time_key_match['time_key'].iloc[0]
                        else:
                            continue  # Skip if no matching time dimension
                        
                        fact_data.append({
                            'reading_key': total_records + len(fact_data) + 1,
                            'sensor_key': sensor_key,
                            'time_key': time_key,
                            'location_key': location_key,
                            'sensor_type_key': sensor_type_key,
                            'reading_value': row['value'],
                            'quality_score': row.get('quality_score', 0.8),
                            'anomaly_flag': row.get('anomaly_flag', 0),
                            'reading_timestamp': row['timestamp'],
                            'batch_id': f"batch_{today.strftime('%Y%m%d')}",
                            'data_source': 'IoT_Sensors'
                        })
                    except (IndexError, KeyError) as e:
                        continue  # Skip problematic records
                
                # Insert batch
                if fact_data:
                    fact_df = pd.DataFrame(fact_data)
                    fact_df.to_sql('fact_sensor_readings', self.db_engine, if_exists='append', index=False, method='multi')
                    total_records += len(fact_data)
            
            logger.info(f"Loaded {total_records} fact records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load fact data: {e}")
            return False
    
    def generate_business_reports(self):
        """Generate business intelligence reports from dimensional model"""
        logger.info("Generating business intelligence reports")
        
        try:
            with self.db_engine.connect() as conn:
                # District performance report
                district_report = pd.read_sql(text("""
                    SELECT 
                        l.district_name,
                        st.sensor_type,
                        COUNT(f.reading_key) as total_readings,
                        AVG(f.reading_value) as avg_reading,
                        AVG(f.quality_score) as avg_quality,
                        SUM(f.anomaly_flag) as anomaly_count,
                        (SUM(f.anomaly_flag) * 100.0 / COUNT(f.reading_key)) as anomaly_rate_pct
                    FROM fact_sensor_readings f
                    JOIN dim_locations l ON f.location_key = l.location_key
                    JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
                    GROUP BY l.district_name, st.sensor_type
                    ORDER BY anomaly_rate_pct DESC
                """), conn)
                
                print("\n" + "="*60)
                print("DISTRICT PERFORMANCE REPORT")
                print("="*60)
                print(district_report.round(3))
                
                # Time-based analysis
                time_report = pd.read_sql(text("""
                    SELECT 
                        t.day_name,
                        t.is_weekend,
                        COUNT(f.reading_key) as total_readings,
                        AVG(f.reading_value) as avg_reading,
                        SUM(f.anomaly_flag) as anomaly_count
                    FROM fact_sensor_readings f
                    JOIN dim_time t ON f.time_key = t.time_key
                    GROUP BY t.day_name, t.is_weekend, t.day_of_week
                    ORDER BY t.day_of_week
                """), conn)
                
                print("\n" + "="*60)
                print("TIME-BASED CONSUMPTION PATTERNS")
                print("="*60)
                print(time_report.round(3))
                
                # Sensor reliability report
                sensor_report = pd.read_sql(text("""
                    SELECT 
                        s.sensor_id,
                        s.sensor_type,
                        l.district_name,
                        COUNT(f.reading_key) as uptime_readings,
                        AVG(f.quality_score) as reliability_score,
                        SUM(f.anomaly_flag) as failure_events,
                        CASE 
                            WHEN AVG(f.quality_score) > 0.9 THEN 'Excellent'
                            WHEN AVG(f.quality_score) > 0.8 THEN 'Good'
                            WHEN AVG(f.quality_score) > 0.7 THEN 'Fair'
                            ELSE 'Needs Maintenance'
                        END as reliability_grade
                    FROM fact_sensor_readings f
                    JOIN dim_sensors s ON f.sensor_key = s.sensor_key
                    JOIN dim_locations l ON f.location_key = l.location_key
                    GROUP BY s.sensor_id, s.sensor_type, l.district_name
                    ORDER BY reliability_score DESC
                """), conn)
                
                print("\n" + "="*60)
                print("SENSOR RELIABILITY ANALYSIS")
                print("="*60)
                print(sensor_report.round(3))
                
            return True
                
        except Exception as e:
            logger.error(f"Failed to generate reports: {e}")
            return False
    
    def execute_warehouse_load(self):
        """Execute complete data warehouse loading process"""
        logger.info("Starting data warehouse loading process")
        
        steps = [
            ("Creating schema", self.create_schema_tables),
            ("Loading time dimension", self.load_time_dimension),
            ("Loading sensor dimensions", self.load_sensor_dimensions),
            ("Loading fact data", self.load_fact_data),
            ("Generating reports", self.generate_business_reports)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"Executing: {step_name}")
            try:
                result = step_func()
                if result is False:
                    logger.error(f"Step failed: {step_name}")
                    return False
            except Exception as e:
                logger.error(f"Step failed: {step_name} - {e}")
                return False
        
        logger.info("Data warehouse loading completed successfully")
        return True

def main():
    """Execute the data warehouse loading process"""
    
    print("Starting Data Warehouse Construction")
    print("="*60)
    
    loader = DataWarehouseLoader()
    success = loader.execute_warehouse_load()
    
    if success:
        print("\nSUCCESS: Data Warehouse Created!")
        print("• Dimensional modeling (Star schema)")
        print("• Slowly Changing Dimensions (SCD Type 2)")
        print("• Fact and dimension table design")
        print("• ETL processes for data warehouse loading")
        print("• Business intelligence reporting")
        print("• Performance optimization with indexes")
    else:
        print("Data warehouse loading failed")

if __name__ == "__main__":
    main()
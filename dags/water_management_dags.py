"""
Fixed Airflow DAGs for Smart Water Management Pipeline
Handles permission issues and ensures data persistence
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os
import shutil

# Default arguments for all DAGs
default_args = {
    'owner': 'water-management',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@waterproject.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# ===============================
# DAG 1: Hourly Data Ingestion (FIXED)
# ===============================
hourly_dag = DAG(
    'water_data_ingestion_hourly',
    default_args=default_args,
    description='Hourly ingestion from all data sources',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['ingestion', 'hourly']
)

def run_data_ingestion(**context):
    """Run the enhanced data ingestion - FIXED VERSION"""
    import sys
    sys.path.append('/app/src')
    from enhanced_ingestion import UnifiedDataIngestionPlatform
    from openweather_ingestion import enhance_openweather_ingestion
    
    platform = UnifiedDataIngestionPlatform()
    platform.historical_mode = False
    platform.multi_location_mode = True
    
    result_df = platform.run_unified_ingestion()
    
    # Enhance with OpenWeather data if available
    if result_df is not None and not result_df.empty:
        try:
            result_df = enhance_openweather_ingestion(result_df)
        except Exception as e:
            logging.warning(f"OpenWeather enhancement skipped: {e}")
    
    if result_df is not None and not result_df.empty:
        # FIX: Save to /tmp first (always writable in Airflow)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_file = f"/tmp/unified_sensor_data_{timestamp}.csv"
        result_df.to_csv(temp_file, index=False)
        
        # Try multiple locations for permanent storage
        saved_locations = []
        
        # Option 1: Try Airflow home directory
        try:
            airflow_data = os.path.expanduser("~/data")
            os.makedirs(airflow_data, exist_ok=True)
            airflow_file = f"{airflow_data}/unified_sensor_data_{timestamp}.csv"
            shutil.copy2(temp_file, airflow_file)
            saved_locations.append(airflow_file)
            logging.info(f"Saved to Airflow home: {airflow_file}")
        except Exception as e:
            logging.warning(f"Could not save to Airflow home: {e}")
        
        # Option 2: Try /opt/airflow/data (common Airflow setup)
        try:
            opt_data = "/opt/airflow/data"
            os.makedirs(opt_data, exist_ok=True)
            opt_file = f"{opt_data}/unified_sensor_data_{timestamp}.csv"
            shutil.copy2(temp_file, opt_file)
            saved_locations.append(opt_file)
            logging.info(f"Saved to opt: {opt_file}")
        except Exception as e:
            logging.warning(f"Could not save to /opt/airflow/data: {e}")
        
        # Option 3: Keep in /tmp as fallback
        saved_locations.append(temp_file)
        
        # Push all file locations to XCom
        context['task_instance'].xcom_push(key='data_file', value=saved_locations[0])
        context['task_instance'].xcom_push(key='all_file_locations', value=saved_locations)
        context['task_instance'].xcom_push(key='record_count', value=len(result_df))
        context['task_instance'].xcom_push(key='temp_file', value=temp_file)
        
        logging.info(f"Ingested {len(result_df)} records")
        logging.info(f"Files saved to: {saved_locations}")
        return True
    else:
        raise Exception("No data ingested")

def run_data_validation(**context):
    """Run data validation - FIXED VERSION"""
    import sys
    sys.path.append('/app/src')
    from data_validator import ProductionDataValidator
    
    # Get file locations from previous task
    file_locations = context['task_instance'].xcom_pull(task_ids='ingest_data', key='all_file_locations')
    
    # Try to read from any available location
    df = None
    data_file = None
    
    for file_path in file_locations:
        try:
            df = pd.read_csv(file_path)
            data_file = file_path
            logging.info(f"Successfully read data from: {file_path}")
            break
        except Exception as e:
            logging.warning(f"Could not read from {file_path}: {e}")
    
    if df is None:
        raise Exception("Could not read data from any location")
    
    validator = ProductionDataValidator()
    report = validator.validate_dataset(df)
    
    # Save validation report to /tmp
    import json
    report_file = f"/tmp/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    # Push validation results
    context['task_instance'].xcom_push(key='validation_score', value=report['overall_score'])
    context['task_instance'].xcom_push(key='validation_status', value=report['status'])
    context['task_instance'].xcom_push(key='validation_report', value=report_file)
    
    if report['status'] == 'FAILED':
        raise Exception(f"Data validation failed with score: {report['overall_score']}")
    
    return report['overall_score']

def upload_to_s3(**context):
    """Upload to S3/MinIO data lake - FIXED VERSION"""
    import sys
    sys.path.append('/app/src')
    
    # Try to use S3, but don't fail if not available
    try:
        from s3_integration import S3DataLakeManager
        
        file_locations = context['task_instance'].xcom_pull(task_ids='ingest_data', key='all_file_locations')
        
        # Read data from available location
        df = None
        for file_path in file_locations:
            try:
                df = pd.read_csv(file_path)
                break
            except:
                continue
        
        if df is not None:
            s3_manager = S3DataLakeManager(use_minio=True)
            s3_path = s3_manager.upload_to_data_lake(df, layer='bronze', data_type='sensor_readings')
            context['task_instance'].xcom_push(key='s3_path', value=s3_path)
            logging.info(f"Uploaded to S3: {s3_path}")
        else:
            logging.warning("No data to upload to S3")
            
    except Exception as e:
        logging.warning(f"S3 upload skipped: {e}")
        # Don't fail the task if S3 is not configured

def load_to_warehouse(**context):
    """Load data to warehouse - FIXED VERSION"""
    import sys
    sys.path.append('/app/src')
    from enhanced_warehouse_loader import EnhancedDataWarehouseLoader
    
    # Try to get data from multiple sources
    file_locations = context['task_instance'].xcom_pull(task_ids='ingest_data', key='all_file_locations')
    temp_file = context['task_instance'].xcom_pull(task_ids='ingest_data', key='temp_file')
    
    # Read data
    df = None
    for file_path in [temp_file] + (file_locations or []):
        try:
            df = pd.read_csv(file_path)
            logging.info(f"Read data from: {file_path}")
            break
        except Exception as e:
            logging.warning(f"Could not read from {file_path}: {e}")
    
    if df is None:
        raise Exception("No data file found to load")
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Create a temporary loader that handles permissions
    loader = EnhancedDataWarehouseLoader()
    
    # Override the file finding method
    loader.find_latest_enhanced_data_file = lambda: file_path
    
    # Load fact data only (dimensions should already exist)
    success = loader.load_enhanced_fact_data(df)
    
    if not success:
        raise Exception("Failed to load data to warehouse")
    
    logging.info(f"Successfully loaded {len(df)} records to warehouse")

# Create directories with proper permissions (runs once at start)
create_directories = BashOperator(
    task_id='create_directories',
    bash_command="""
    mkdir -p /tmp/airflow_data /opt/airflow/data ~/data || true
    chmod 777 /tmp/airflow_data || true
    echo "Directories created"
    """,
    dag=hourly_dag
)

# Define tasks for hourly DAG
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=run_data_ingestion,
    provide_context=True,
    dag=hourly_dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=run_data_validation,
    provide_context=True,
    dag=hourly_dag
)

upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    trigger_rule='all_done',  # Run even if previous task fails
    dag=hourly_dag
)

load_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    provide_context=True,
    dag=hourly_dag
)

refresh_views_task = PostgresOperator(
    task_id='refresh_materialized_views',
    postgres_conn_id='water_postgres',
    sql="""
        -- Only refresh if views exist
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_sensor_current_status') THEN
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sensor_current_status;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_district_performance_daily') THEN
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_district_performance_daily;
            END IF;
        END $$;
    """,
    trigger_rule='all_done',
    dag=hourly_dag
)

# Set task dependencies
create_directories >> ingest_task >> validate_task >> [upload_s3_task, load_warehouse_task] >> refresh_views_task

# ===============================
# DAG 2: Data Persistence Check
# ===============================
persistence_dag = DAG(
    'water_data_persistence',
    default_args=default_args,
    description='Ensure data is persisted properly',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    tags=['monitoring', 'persistence']
)

def check_and_persist_data(**context):
    """Check for data in temp locations and persist it"""
    import glob
    import shutil
    
    # Look for CSV files in temp directory
    temp_files = glob.glob('/tmp/unified_sensor_data_*.csv')
    
    if temp_files:
        # Create persistent directory
        persist_dir = os.path.expanduser("~/airflow_persistent_data")
        os.makedirs(persist_dir, exist_ok=True)
        
        for temp_file in temp_files:
            try:
                filename = os.path.basename(temp_file)
                dest_file = os.path.join(persist_dir, filename)
                
                if not os.path.exists(dest_file):
                    shutil.copy2(temp_file, dest_file)
                    logging.info(f"Persisted {temp_file} to {dest_file}")
                    
                    # Also try to load to warehouse if not already loaded
                    # Check file age - if recent, it might not be in warehouse yet
                    file_age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(temp_file))).seconds / 3600
                    if file_age_hours < 2:  # Less than 2 hours old
                        context['task_instance'].xcom_push(key='new_file', value=dest_file)
                        
            except Exception as e:
                logging.error(f"Failed to persist {temp_file}: {e}")
    
    # Report status
    logging.info(f"Found {len(temp_files)} temp files")
    return len(temp_files)

persist_task = PythonOperator(
    task_id='check_and_persist',
    python_callable=check_and_persist_data,
    provide_context=True,
    dag=persistence_dag
)

# ===============================
# DAG 3: System Health Check
# ===============================
health_dag = DAG(
    'water_system_health',
    default_args=default_args,
    description='Check overall system health',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['monitoring', 'health']
)

def check_system_health(**context):
    """Check if data is flowing through the system"""
    hook = PostgresHook(postgres_conn_id='water_postgres')
    
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    # Check 1: Recent data in warehouse
    try:
        recent_data = hook.get_pandas_df("""
            SELECT COUNT(*) as count, MAX(reading_timestamp) as latest
            FROM fact_sensor_readings
            WHERE reading_timestamp > NOW() - INTERVAL '2 hours'
        """)
        
        health_status['checks']['recent_data'] = {
            'status': 'OK' if recent_data.iloc[0]['count'] > 0 else 'WARNING',
            'count': int(recent_data.iloc[0]['count']),
            'latest': str(recent_data.iloc[0]['latest'])
        }
    except Exception as e:
        health_status['checks']['recent_data'] = {
            'status': 'ERROR',
            'error': str(e)
        }
    
    # Check 2: Data files exist
    import glob
    data_files = glob.glob('/tmp/unified_sensor_data_*.csv') + \
                 glob.glob(os.path.expanduser('~/data/unified_sensor_data_*.csv')) + \
                 glob.glob('/opt/airflow/data/unified_sensor_data_*.csv')
    
    health_status['checks']['data_files'] = {
        'status': 'OK' if data_files else 'WARNING',
        'count': len(data_files),
        'locations': list(set(os.path.dirname(f) for f in data_files))
    }
    
    # Log health status
    import json
    logging.info(f"System health: {json.dumps(health_status, indent=2)}")
    
    return health_status

health_check_task = PythonOperator(
    task_id='system_health_check',
    python_callable=check_system_health,
    provide_context=True,
    dag=health_dag
)
"""
Airflow DAGs for Smart Water Management Pipeline
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
# DAG 1: Hourly Data Ingestion
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
    """Run the enhanced data ingestion"""
    import sys
    sys.path.append('/app/src')
    from enhanced_ingestion import UnifiedDataIngestionPlatform
    from openweather_ingestion import enhance_openweather_ingestion
    
    platform = UnifiedDataIngestionPlatform()
    # Set to get only current data for hourly runs
    platform.historical_mode = False
    platform.multi_location_mode = True
    
    result_df = platform.run_unified_ingestion()
    
    # Enhance with more OpenWeather data
    if result_df is not None and not result_df.empty:
        result_df = enhance_openweather_ingestion(result_df)
    
    if result_df is not None and not result_df.empty:
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"/app/data/unified_sensor_data_{timestamp}.csv"
        result_df.to_csv(output_file, index=False)
        
        # Push file path to XCom for next tasks
        context['task_instance'].xcom_push(key='data_file', value=output_file)
        context['task_instance'].xcom_push(key='record_count', value=len(result_df))
        
        logging.info(f"Ingested {len(result_df)} records")
        return True
    else:
        raise Exception("No data ingested")

def run_data_validation(**context):
    """Run data validation"""
    import sys
    sys.path.append('/app/src')
    from data_validator import ProductionDataValidator
    
    # Get file path from previous task
    data_file = context['task_instance'].xcom_pull(task_ids='ingest_data', key='data_file')
    
    df = pd.read_csv(data_file)
    validator = ProductionDataValidator()
    report = validator.validate_dataset(df)
    
    # Push validation results
    context['task_instance'].xcom_push(key='validation_score', value=report['overall_score'])
    context['task_instance'].xcom_push(key='validation_status', value=report['status'])
    
    if report['status'] == 'FAILED':
        raise Exception(f"Data validation failed with score: {report['overall_score']}")
    
    return report['overall_score']

def upload_to_s3(**context):
    """Upload to S3/MinIO data lake"""
    import sys
    sys.path.append('/app/src')
    from s3_integration import S3DataLakeManager
    
    data_file = context['task_instance'].xcom_pull(task_ids='ingest_data', key='data_file')
    
    df = pd.read_csv(data_file)
    s3_manager = S3DataLakeManager(use_minio=True)
    
    # Upload to bronze layer
    s3_path = s3_manager.upload_to_data_lake(df, layer='bronze', data_type='sensor_readings')
    
    context['task_instance'].xcom_push(key='s3_path', value=s3_path)
    logging.info(f"Uploaded to S3: {s3_path}")

def load_to_warehouse(**context):
    """Load data to warehouse"""
    import sys
    sys.path.append('/app/src')
    from enhanced_warehouse_loader import EnhancedDataWarehouseLoader
    
    loader = EnhancedDataWarehouseLoader()
    data_file = context['task_instance'].xcom_pull(task_ids='ingest_data', key='data_file')
    
    df = pd.read_csv(data_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Load fact data only (dimensions already loaded)
    success = loader.load_enhanced_fact_data(df)
    
    if not success:
        raise Exception("Failed to load data to warehouse")

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
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sensor_current_status;
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_district_performance_daily;
    """,
    dag=hourly_dag
)

# Set task dependencies
ingest_task >> validate_task >> [upload_s3_task, load_warehouse_task] >> refresh_views_task

# ===============================
# DAG 2: Daily Analytics Pipeline
# ===============================
daily_dag = DAG(
    'water_analytics_daily',
    default_args=default_args,
    description='Daily analytics and aggregations',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['analytics', 'daily']
)

def run_spark_analytics(**context):
    """Run Spark analytics pipeline"""
    import sys
    sys.path.append('/app/src')
    from spark_s3_integration import SparkS3ETL
    
    spark_etl = SparkS3ETL(use_minio=True)
    
    # Read from bronze
    df = spark_etl.read_from_bronze()
    
    if df:
        # Process to silver
        silver_df = spark_etl.process_to_silver(df)
        
        # Create gold aggregations
        hourly_agg, daily_summary = spark_etl.create_gold_aggregations(silver_df)
        
        logging.info("Spark analytics completed")
        return True
    else:
        raise Exception("No data found in bronze layer")

def generate_daily_reports(**context):
    """Generate daily reports"""
    # Query warehouse for daily metrics
    hook = PostgresHook(postgres_conn_id='water_postgres')
    
    # Get daily summary
    daily_summary = hook.get_pandas_df("""
        SELECT 
            l.district_name,
            st.sensor_type,
            COUNT(f.reading_key) as total_readings,
            AVG(f.reading_value) as avg_value,
            SUM(f.anomaly_flag) as anomaly_count,
            AVG(f.quality_score) as avg_quality
        FROM fact_sensor_readings f
        JOIN dim_locations l ON f.location_key = l.location_key
        JOIN dim_sensor_types st ON f.sensor_type_key = st.sensor_type_key
        WHERE DATE(f.reading_timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY l.district_name, st.sensor_type
    """)
    
    # Save report
    report_file = f"/app/data/daily_report_{datetime.now().strftime('%Y%m%d')}.csv"
    daily_summary.to_csv(report_file, index=False)
    
    logging.info(f"Daily report saved to {report_file}")
    return report_file

def check_data_quality_alerts(**context):
    """Check for data quality issues and send alerts"""
    hook = PostgresHook(postgres_conn_id='water_postgres')
    
    # Check for sensors with low quality scores
    problem_sensors = hook.get_pandas_df("""
        SELECT 
            s.sensor_id,
            s.sensor_type,
            l.district_name,
            AVG(f.quality_score) as avg_quality,
            COUNT(f.reading_key) as reading_count
        FROM fact_sensor_readings f
        JOIN dim_sensors s ON f.sensor_key = s.sensor_key
        JOIN dim_locations l ON f.location_key = l.location_key
        WHERE f.reading_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        GROUP BY s.sensor_id, s.sensor_type, l.district_name
        HAVING AVG(f.quality_score) < 0.7
    """)
    
    if not problem_sensors.empty:
        # In production, this would send emails/Slack notifications
        logging.warning(f"Found {len(problem_sensors)} sensors with quality issues")
        context['task_instance'].xcom_push(key='problem_sensors', value=problem_sensors.to_dict())
    
    return len(problem_sensors)

# Define tasks for daily DAG
spark_analytics_task = PythonOperator(
    task_id='run_spark_analytics',
    python_callable=run_spark_analytics,
    provide_context=True,
    dag=daily_dag
)

generate_reports_task = PythonOperator(
    task_id='generate_daily_reports',
    python_callable=generate_daily_reports,
    provide_context=True,
    dag=daily_dag
)

quality_alerts_task = PythonOperator(
    task_id='check_quality_alerts',
    python_callable=check_data_quality_alerts,
    provide_context=True,
    dag=daily_dag
)

cleanup_old_data_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command='find /app/data -name "*.csv" -mtime +7 -delete',
    dag=daily_dag
)

# Set task dependencies
spark_analytics_task >> [generate_reports_task, quality_alerts_task] >> cleanup_old_data_task

# ===============================
# DAG 3: Weekly Historical Backfill
# ===============================
weekly_dag = DAG(
    'water_historical_backfill_weekly',
    default_args=default_args,
    description='Weekly historical data backfill',
    schedule_interval='0 3 * * 0',  # 3 AM on Sundays
    catchup=False,
    tags=['backfill', 'weekly']
)

def run_historical_backfill(**context):
    """Run historical data backfill"""
    import sys
    sys.path.append('/app/src')
    from enhanced_ingestion import UnifiedDataIngestionPlatform
    
    platform = UnifiedDataIngestionPlatform()
    # Configure for historical data
    platform.historical_mode = True
    platform.historical_days = 30  # Last 30 days
    platform.multi_location_mode = True
    
    result_df = platform.run_unified_ingestion()
    
    if result_df is not None and not result_df.empty:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"/app/data/historical_backfill_{timestamp}.csv"
        result_df.to_csv(output_file, index=False)
        
        logging.info(f"Backfilled {len(result_df)} historical records")
        return output_file
    else:
        raise Exception("Historical backfill failed")

backfill_task = PythonOperator(
    task_id='historical_backfill',
    python_callable=run_historical_backfill,
    provide_context=True,
    dag=weekly_dag
)

# ===============================
# DAG 4: Real-time Monitoring
# ===============================
monitoring_dag = DAG(
    'water_monitoring_5min',
    default_args=default_args,
    description='5-minute monitoring for critical sensors',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['monitoring', 'real-time']
)

def check_sensor_health(**context):
    """Check sensor health and anomalies"""
    hook = PostgresHook(postgres_conn_id='water_postgres')
    
    # Check for offline sensors
    offline_sensors = hook.get_pandas_df("""
        SELECT 
            s.sensor_id,
            s.sensor_type,
            l.district_name,
            MAX(f.reading_timestamp) as last_reading,
            EXTRACT(EPOCH FROM (NOW() - MAX(f.reading_timestamp)))/3600 as hours_offline
        FROM dim_sensors s
        LEFT JOIN fact_sensor_readings f ON s.sensor_key = f.sensor_key
        LEFT JOIN dim_locations l ON s.location_key = l.location_key
        WHERE s.is_current = TRUE
        GROUP BY s.sensor_id, s.sensor_type, l.district_name
        HAVING MAX(f.reading_timestamp) < NOW() - INTERVAL '2 hours'
    """)
    
    # Check for anomalies in last 5 minutes
    recent_anomalies = hook.get_pandas_df("""
        SELECT 
            s.sensor_id,
            f.reading_value,
            f.anomaly_score,
            f.reading_timestamp
        FROM fact_sensor_readings f
        JOIN dim_sensors s ON f.sensor_key = s.sensor_key
        WHERE f.reading_timestamp >= NOW() - INTERVAL '5 minutes'
        AND f.anomaly_flag = 1
    """)
    
    if not offline_sensors.empty or not recent_anomalies.empty:
        logging.warning(f"Offline sensors: {len(offline_sensors)}, Recent anomalies: {len(recent_anomalies)}")
        # In production, trigger alerts via email/Slack/PagerDuty
    
    return {
        'offline_count': len(offline_sensors),
        'anomaly_count': len(recent_anomalies)
    }

monitor_task = PythonOperator(
    task_id='check_sensor_health',
    python_callable=check_sensor_health,
    provide_context=True,
    dag=monitoring_dag
)

# ===============================
# DAG 5: Setup Connections (One-time)
# ===============================
setup_dag = DAG(
    'setup_connections',
    default_args=default_args,
    description='One-time setup of Airflow connections',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'admin']
)

def create_connections():
    """Create all necessary Airflow connections"""
    from airflow import settings
    from airflow.models import Connection
    
    session = settings.Session()
    
    # PostgreSQL connection
    postgres_conn = Connection(
        conn_id='water_postgres',
        conn_type='postgres',
        host='postgres',
        schema='water_analytics',
        login='dataeng',
        password='pipeline123',
        port=5432
    )
    
    # MinIO S3 connection
    minio_conn = Connection(
        conn_id='minio_s3',
        conn_type='s3',
        extra={
            'aws_access_key_id': 'minioadmin',
            'aws_secret_access_key': 'minioadmin123',
            'host': 'http://minio:9000'
        }
    )
    
    # Add connections if they don't exist
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'water_postgres').first()
    if not existing_conn:
        session.add(postgres_conn)
        logging.info("Created PostgreSQL connection")
    
    existing_minio = session.query(Connection).filter(Connection.conn_id == 'minio_s3').first()
    if not existing_minio:
        session.add(minio_conn)
        logging.info("Created MinIO S3 connection")
    
    session.commit()
    session.close()
    
    return "Connections created successfully"

setup_connections_task = PythonOperator(
    task_id='create_connections',
    python_callable=create_connections,
    dag=setup_dag
)

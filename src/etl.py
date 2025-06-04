import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WaterManagementETL:
    """
    ETL pipeline for processing water management IoT sensor data.
    Implements bronze-silver-gold data lake architecture.
    """
    
    def __init__(self, raw_bucket, processed_bucket):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.s3_client = boto3.client('s3')
    
    def extract_from_s3(self, bucket, key):
        """Extract CSV data from S3 bucket"""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            logger.info(f"Successfully extracted {len(df)} records from {key}")
            return df
        except Exception as e:
            logger.error(f"Failed to extract data from {key}: {str(e)}")
            return None
    
    def load_to_s3(self, df, bucket, key):
        """Load DataFrame to S3 as CSV"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=bucket, 
                Key=key, 
                Body=csv_buffer.getvalue()
            )
            logger.info(f"Successfully loaded {len(df)} records to {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to load data to {key}: {str(e)}")
            return False
    
    def clean_sensor_data(self, df):
        """Apply data quality rules and cleaning transformations"""
        logger.info("Starting data cleaning process")
        
        initial_row_count = len(df)
        
        # Parse timestamp column
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Remove duplicate records
        df = df.drop_duplicates()
        
        # Remove records with missing critical values
        df = df.dropna(subset=['value', 'sensor_id', 'sensor_type'])
        
        # Add derived time dimensions
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        df['date'] = df['timestamp'].dt.date
        
        final_row_count = len(df)
        records_removed = initial_row_count - final_row_count
        
        logger.info(f"Data cleaning completed: {records_removed} records removed")
        logger.info(f"Final dataset: {final_row_count} records")
        
        return df
    
    def build_aggregations(self, df):
        """Create district and time-based aggregations for analytics"""
        logger.info("Building aggregation tables")
        
        # District-hour-sensor type aggregations
        district_hourly = df.groupby(['district', 'hour', 'sensor_type']).agg({
            'value': ['mean', 'min', 'max', 'count'],
            'quality_score': 'mean',
            'anomaly_flag': 'sum'
        }).round(3)
        
        # Flatten multi-level column names
        district_hourly.columns = ['_'.join(col) for col in district_hourly.columns]
        district_hourly = district_hourly.reset_index()
        
        # Daily district summaries
        daily_summary = df.groupby(['district', 'sensor_type']).agg({
            'value': ['mean', 'std'],
            'anomaly_flag': 'sum',
            'quality_score': 'mean'
        }).round(3)
        
        daily_summary.columns = ['_'.join(col) for col in daily_summary.columns]
        daily_summary = daily_summary.reset_index()
        
        logger.info(f"Generated {len(district_hourly)} hourly aggregates")
        logger.info(f"Generated {len(daily_summary)} daily aggregates")
        
        return district_hourly, daily_summary
    
    def calculate_performance_metrics(self, df):
        """Calculate operational KPIs and system performance metrics"""
        logger.info("Calculating system performance metrics")
        
        metrics = {
            'total_sensors': df['sensor_id'].nunique(),
            'total_readings': len(df),
            'avg_quality_score': round(df['quality_score'].mean(), 3),
            'anomaly_rate_percent': round((df['anomaly_flag'].sum() / len(df)) * 100, 2),
            'districts_monitored': df['district'].nunique(),
            'date_range_start': df['timestamp'].min().strftime('%Y-%m-%d'),
            'date_range_end': df['timestamp'].max().strftime('%Y-%m-%d')
        }
        
        # Flow rate metrics
        flow_readings = df[df['sensor_type'] == 'flow']
        if not flow_readings.empty:
            metrics['avg_flow_rate_lps'] = round(flow_readings['value'].mean(), 2)
            metrics['peak_flow_rate_lps'] = round(flow_readings['value'].max(), 2)
            metrics['flow_std_deviation'] = round(flow_readings['value'].std(), 2)
        
        # Pressure metrics
        pressure_readings = df[df['sensor_type'] == 'pressure']
        if not pressure_readings.empty:
            metrics['avg_pressure_psi'] = round(pressure_readings['value'].mean(), 2)
            metrics['min_pressure_psi'] = round(pressure_readings['value'].min(), 2)
            metrics['low_pressure_events'] = len(pressure_readings[pressure_readings['value'] < 30])
        
        # Water quality metrics
        quality_readings = df[df['sensor_type'] == 'quality']
        if not quality_readings.empty:
            metrics['avg_ph_level'] = round(quality_readings['value'].mean(), 2)
            ph_violations = len(quality_readings[
                (quality_readings['value'] < 6.5) | (quality_readings['value'] > 8.5)
            ])
            metrics['ph_violations'] = ph_violations
        
        # District performance comparison
        district_performance = df.groupby('district').agg({
            'quality_score': 'mean',
            'anomaly_flag': 'mean'
        }).round(3)
        
        logger.info("Performance metrics calculation completed")
        
        return metrics, district_performance
    
    def execute_pipeline(self):
        """Main ETL pipeline execution"""
        logger.info("Initiating water management ETL pipeline")
        
        # Generate date-based partition path
        execution_date = datetime.now()
        partition_path = f"year={execution_date.year}/month={execution_date.month:02d}/day={execution_date.day:02d}"
        
        try:
            # Extract phase
            logger.info("Phase 1: Data extraction")
            raw_data = self.extract_from_s3(
                self.raw_bucket, 
                f'bronze/readings/{partition_path}/readings.csv'
            )
            
            if raw_data is None:
                logger.error("No raw data available for processing")
                return False
            
            # Transform phase
            logger.info("Phase 2: Data transformation")
            cleaned_data = self.clean_sensor_data(raw_data)
            hourly_agg, daily_agg = self.build_aggregations(cleaned_data)
            system_metrics, district_metrics = self.calculate_performance_metrics(cleaned_data)
            
            # Load phase
            logger.info("Phase 3: Data loading")
            
            # Define output datasets and S3 paths
            output_datasets = [
                (cleaned_data, f'silver/clean_readings/{partition_path}/cleaned_readings.csv'),
                (hourly_agg, f'gold/hourly_aggregates/{partition_path}/hourly_agg.csv'),
                (daily_agg, f'gold/daily_summary/{partition_path}/daily_summary.csv'),
                (district_metrics, f'gold/district_performance/{partition_path}/district_perf.csv')
            ]
            
            # Load datasets to S3
            successful_loads = 0
            for dataset, s3_path in output_datasets:
                if self.load_to_s3(dataset, self.processed_bucket, s3_path):
                    successful_loads += 1
            
            # Load system metrics
            metrics_df = pd.DataFrame([system_metrics])
            if self.load_to_s3(metrics_df, self.processed_bucket, f'gold/system_metrics/{partition_path}/metrics.csv'):
                successful_loads += 1
            
            # Pipeline completion summary
            total_datasets = len(output_datasets) + 1
            logger.info(f"ETL pipeline completed successfully")
            logger.info(f"Datasets loaded: {successful_loads}/{total_datasets}")
            logger.info(f"Records processed: {len(cleaned_data)}")
            logger.info(f"Districts analyzed: {system_metrics['districts_monitored']}")
            logger.info(f"Data quality score: {system_metrics['avg_quality_score']}")
            logger.info(f"Anomaly detection rate: {system_metrics['anomaly_rate_percent']}%")
            
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline execution failed: {str(e)}")
            return False

def main():
    """Pipeline entry point"""
    
    # Configuration
    RAW_DATA_BUCKET = "water-project-raw"
    PROCESSED_DATA_BUCKET = "water-project-processed"
    
    # Initialize and execute ETL pipeline
    etl_pipeline = WaterManagementETL(RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET)
    pipeline_success = etl_pipeline.execute_pipeline()
    
    if pipeline_success:
        logger.info("Water management ETL pipeline completed successfully")
        print("\nPipeline execution summary:")
        print("- Raw sensor data extracted from bronze layer")
        print("- Data quality validation and cleaning applied")
        print("- Analytical aggregations created")
        print("- Performance metrics calculated")
        print("- Processed data loaded to silver and gold layers")
        print("\nReady for downstream analytics and reporting")
    else:
        logger.error("Pipeline execution failed - review logs for details")
        print("Pipeline execution failed. Check configuration and data availability.")

if __name__ == "__main__":
    main()
"""
Fixed S3 Integration with MinIO for Local Development
Allows the project to work with local S3-compatible storage
"""

import boto3
import pandas as pd
import os
from datetime import datetime
import logging
from botocore.exceptions import NoCredentialsError, ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3DataLakeManager:
    """
    Manages data lake operations with S3/MinIO
    Works with both AWS S3 and local MinIO
    """
    
    def __init__(self, use_minio=True):
        """
        Initialize S3 client with MinIO or AWS
        
        Args:
            use_minio: If True, use local MinIO. If False, use AWS S3
        """
        self.use_minio = use_minio
        
        if use_minio:
            # MinIO configuration
            self.s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9001',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin123',
                region_name='us-east-1',
                use_ssl=False
            )
            self.raw_bucket = 'water-project-raw'
            self.processed_bucket = 'water-project-processed'
            logger.info("Using MinIO for S3 storage")
        else:
            # AWS S3 configuration
            self.s3_client = boto3.client('s3')
            self.raw_bucket = os.getenv('AWS_S3_BUCKET_RAW', 'water-project-raw')
            self.processed_bucket = os.getenv('AWS_S3_BUCKET_PROCESSED', 'water-project-processed')
            logger.info("Using AWS S3 for storage")
        
        # Create buckets if they don't exist
        self._ensure_buckets_exist()
    
    def _ensure_buckets_exist(self):
        """Create buckets if they don't exist"""
        for bucket in [self.raw_bucket, self.processed_bucket]:
            try:
                self.s3_client.head_bucket(Bucket=bucket)
                logger.info(f"Bucket {bucket} exists")
            except ClientError:
                try:
                    if self.use_minio:
                        self.s3_client.create_bucket(Bucket=bucket)
                    else:
                        # AWS requires location constraint for non us-east-1
                        self.s3_client.create_bucket(
                            Bucket=bucket,
                            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
                        )
                    logger.info(f"Created bucket {bucket}")
                except Exception as e:
                    logger.error(f"Failed to create bucket {bucket}: {e}")
    
    def upload_to_data_lake(self, df, layer='bronze', data_type='sensor_readings', partition_by_date=True):
        """
        Upload DataFrame to S3 data lake with proper structure
        
        Args:
            df: DataFrame to upload
            layer: Data lake layer (bronze, silver, gold)
            data_type: Type of data (sensor_readings, aggregations, etc.)
            partition_by_date: Whether to partition by date
        """
        try:
            # Generate partition path
            if partition_by_date and 'timestamp' in df.columns:
                # Get date from first record
                first_date = pd.to_datetime(df['timestamp'].iloc[0])
                partition_path = f"year={first_date.year}/month={first_date.month:02d}/day={first_date.day:02d}"
            else:
                partition_path = f"year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}"
            
            # Determine bucket based on layer
            bucket = self.raw_bucket if layer == 'bronze' else self.processed_bucket
            
            # Create S3 key
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"{layer}/{data_type}/{partition_path}/{data_type}_{timestamp}.parquet"
            
            # Convert to parquet for efficiency
            parquet_buffer = df.to_parquet(index=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_buffer
            )
            
            logger.info(f"Uploaded {len(df)} records to s3://{bucket}/{s3_key}")
            return f"s3://{bucket}/{s3_key}"
            
        except Exception as e:
            logger.error(f"Failed to upload to data lake: {e}")
            return None
    
    def read_from_data_lake(self, layer='bronze', data_type='sensor_readings', date=None):
        """
        Read data from S3 data lake
        
        Args:
            layer: Data lake layer
            data_type: Type of data
            date: Specific date to read (optional)
        """
        try:
            bucket = self.raw_bucket if layer == 'bronze' else self.processed_bucket
            
            # Build prefix
            if date:
                prefix = f"{layer}/{data_type}/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
            else:
                prefix = f"{layer}/{data_type}/"
            
            # List objects
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No data found in {bucket}/{prefix}")
                return pd.DataFrame()
            
            # Read all parquet files
            dfs = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    response = self.s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                    df = pd.read_parquet(response['Body'])
                    dfs.append(df)
                    logger.info(f"Read {len(df)} records from {obj['Key']}")
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                logger.info(f"Total records read: {len(combined_df)}")
                return combined_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to read from data lake: {e}")
            return pd.DataFrame()
    
    def list_data_lake_contents(self):
        """List contents of data lake for inspection"""
        contents = {'raw': {}, 'processed': {}}
        
        for bucket_name, bucket_key in [(self.raw_bucket, 'raw'), (self.processed_bucket, 'processed')]:
            try:
                response = self.s3_client.list_objects_v2(Bucket=bucket_name)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        size_mb = obj['Size'] / (1024 * 1024)
                        contents[bucket_key][obj['Key']] = {
                            'size_mb': round(size_mb, 2),
                            'last_modified': obj['LastModified'].isoformat()
                        }
                        
            except Exception as e:
                logger.error(f"Error listing {bucket_name}: {e}")
        
        return contents

# Updated ETL class to use S3/MinIO
class EnhancedETL:
    """Enhanced ETL with proper S3 integration"""
    
    def __init__(self, use_minio=True):
        self.s3_manager = S3DataLakeManager(use_minio=use_minio)
        self.db_engine = None  # Initialize as needed
    
    def ingest_to_bronze(self, csv_file_path):
        """Ingest raw data to bronze layer"""
        logger.info(f"Ingesting {csv_file_path} to bronze layer")
        
        # Read CSV
        df = pd.read_csv(csv_file_path)
        
        # Upload to bronze layer
        s3_path = self.s3_manager.upload_to_data_lake(
            df, 
            layer='bronze', 
            data_type='sensor_readings'
        )
        
        return s3_path
    
    def process_to_silver(self, date=None):
        """Process bronze data to silver layer"""
        logger.info("Processing bronze to silver layer")
        
        # Read from bronze
        bronze_df = self.s3_manager.read_from_data_lake(
            layer='bronze',
            data_type='sensor_readings',
            date=date
        )
        
        if bronze_df.empty:
            logger.warning("No bronze data to process")
            return None
        
        # Data quality checks and cleaning
        silver_df = bronze_df.copy()
        
        # Convert timestamp
        silver_df['timestamp'] = pd.to_datetime(silver_df['timestamp'])
        
        # Remove duplicates
        silver_df = silver_df.drop_duplicates()
        
        # Remove invalid readings
        silver_df = silver_df[silver_df['value'].notna()]
        silver_df = silver_df[silver_df['quality_score'] > 0.5]
        
        # Add processing metadata
        silver_df['processed_timestamp'] = datetime.now()
        silver_df['processing_version'] = '2.0'
        
        # Upload to silver layer
        s3_path = self.s3_manager.upload_to_data_lake(
            silver_df,
            layer='silver',
            data_type='clean_readings'
        )
        
        logger.info(f"Processed {len(silver_df)} records to silver layer")
        return s3_path
    
    def aggregate_to_gold(self, date=None):
        """Create business-ready aggregations in gold layer"""
        logger.info("Creating gold layer aggregations")
        
        # Read from silver
        silver_df = self.s3_manager.read_from_data_lake(
            layer='silver',
            data_type='clean_readings',
            date=date
        )
        
        if silver_df.empty:
            logger.warning("No silver data to aggregate")
            return None
        
        # Ensure timestamp is datetime
        silver_df['timestamp'] = pd.to_datetime(silver_df['timestamp'])
        
        # Hourly aggregations
        hourly_agg = silver_df.groupby([
            pd.Grouper(key='timestamp', freq='H'),
            'sensor_type',
            'district'
        ]).agg({
            'value': ['mean', 'min', 'max', 'std', 'count'],
            'quality_score': 'mean',
            'anomaly_flag': 'sum'
        }).reset_index()
        
        # Flatten column names
        hourly_agg.columns = ['_'.join(col).strip('_') for col in hourly_agg.columns.values]
        
        # Upload to gold layer
        s3_path = self.s3_manager.upload_to_data_lake(
            hourly_agg,
            layer='gold',
            data_type='hourly_aggregations'
        )
        
        logger.info(f"Created {len(hourly_agg)} hourly aggregations in gold layer")
        return s3_path

def test_s3_integration():
    """Test S3/MinIO integration"""
    print("🧪 Testing S3/MinIO Integration")
    print("=" * 50)
    
    # Initialize manager
    s3_manager = S3DataLakeManager(use_minio=True)
    
    # List current contents
    print("\n📦 Current Data Lake Contents:")
    contents = s3_manager.list_data_lake_contents()
    
    for bucket_type, files in contents.items():
        print(f"\n{bucket_type.upper()} Bucket:")
        if files:
            for file_key, file_info in files.items():
                print(f"  - {file_key}: {file_info['size_mb']} MB")
        else:
            print("  - Empty")
    
    # Test upload
    print("\n📤 Testing Upload...")
    test_df = pd.DataFrame({
        'timestamp': [datetime.now()],
        'sensor_id': ['TEST_001'],
        'value': [25.5],
        'quality_score': [0.95]
    })
    
    result = s3_manager.upload_to_data_lake(test_df, layer='bronze', data_type='test_data')
    if result:
        print(f"✅ Upload successful: {result}")
    else:
        print("❌ Upload failed")
    
    # Test read
    print("\n📥 Testing Read...")
    read_df = s3_manager.read_from_data_lake(layer='bronze', data_type='test_data')
    if not read_df.empty:
        print(f"✅ Read successful: {len(read_df)} records")
    else:
        print("❌ Read failed or no data")
    
    print("\n✅ S3/MinIO integration test complete!")

if __name__ == "__main__":
    # Run integration test
    test_s3_integration()
    
    # Example: Process your real data
    print("\n" + "="*50)
    print("📊 Processing Real Sensor Data")
    print("="*50)
    
    etl = EnhancedETL(use_minio=True)
    
    # Find latest CSV file
    import glob
    csv_files = glob.glob("data/unified_sensor_data_enhanced_*.csv")
    if csv_files:
        latest_csv = max(csv_files, key=os.path.getmtime)
        print(f"Processing: {latest_csv}")
        
        # Ingest to bronze
        bronze_path = etl.ingest_to_bronze(latest_csv)
        
        # Process to silver
        silver_path = etl.process_to_silver()
        
        # Aggregate to gold
        gold_path = etl.aggregate_to_gold()
        
        print("\n✅ ETL Pipeline Complete!")
        print(f"Bronze: {bronze_path}")
        print(f"Silver: {silver_path}")
        print(f"Gold: {gold_path}")
    else:
        print("❌ No CSV files found to process")
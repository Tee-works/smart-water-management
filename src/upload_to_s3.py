import boto3
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_sensor_data():
    """Upload generated sensor data to S3 bronze layer"""
    
    # Configuration
    RAW_BUCKET = "water-project-raw"
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Generate partition path based on current date
    current_date = datetime.now()
    partition_path = f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}"
    
    # Define files to upload
    upload_tasks = [
        {
            'local_file': 'data/unified_sensor_data_enhanced_*.csv',
            's3_key': f'bronze/sensors/{partition_path}/sensors.csv',
            'description': 'Sensor location metadata'
        }
    ]
    
    logger.info(f"Starting data upload to S3 bucket: {RAW_BUCKET}")
    
    successful_uploads = 0
    for task in upload_tasks:
        local_file = task['local_file']
        s3_key = task['s3_key']
        description = task['description']
        
        if not os.path.exists(local_file):
            logger.warning(f"Local file not found: {local_file}")
            continue
        
        try:
            # Get file size for logging
            file_size = os.path.getsize(local_file)
            size_mb = file_size / (1024 * 1024)
            
            # Upload file to S3
            s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
            
            logger.info(f"Uploaded {description}: {local_file} -> s3://{RAW_BUCKET}/{s3_key}")
            logger.info(f"File size: {size_mb:.2f} MB")
            
            successful_uploads += 1
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file}: {str(e)}")
    
    # Verify uploads by listing bucket contents
    try:
        logger.info("Verifying uploaded files...")
        response = s3_client.list_objects_v2(Bucket=RAW_BUCKET)
        
        if 'Contents' in response:
            logger.info(f"Files currently in bucket {RAW_BUCKET}:")
            for obj in response['Contents']:
                file_size_mb = obj['Size'] / (1024 * 1024)
                logger.info(f"  {obj['Key']} ({file_size_mb:.2f} MB)")
        else:
            logger.warning("No files found in bucket")
            
    except Exception as e:
        logger.error(f"Failed to list bucket contents: {str(e)}")
    
    # Upload summary
    total_tasks = len(upload_tasks)
    logger.info(f"Upload completed: {successful_uploads}/{total_tasks} files uploaded successfully")
    
    if successful_uploads == total_tasks:
        print("Data upload completed successfully")
        print(f"Files uploaded to s3://{RAW_BUCKET}/bronze/")
        return True
    else:
        print("Some uploads failed - check logs for details")
        return False

if __name__ == "__main__":
    upload_sensor_data()
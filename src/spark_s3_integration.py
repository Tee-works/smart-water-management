"""
Enhanced Spark ETL with S3/MinIO Integration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SparkS3ETL:
    """Spark ETL with S3/MinIO data lake integration"""
    
    def __init__(self, use_minio=True):
        # Configure Spark for S3/MinIO
        builder = SparkSession.builder \
            .appName("WaterManagementS3ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        if use_minio:
            # MinIO configuration
            builder = builder \
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001") \
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        else:
            # AWS S3 configuration
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        # S3 paths
        if use_minio:
            self.bronze_path = "s3a://water-project-raw/bronze/"
            self.silver_path = "s3a://water-project-processed/silver/"
            self.gold_path = "s3a://water-project-processed/gold/"
        else:
            self.bronze_path = f"s3a://{os.getenv('AWS_S3_BUCKET_RAW')}/bronze/"
            self.silver_path = f"s3a://{os.getenv('AWS_S3_BUCKET_PROCESSED')}/silver/"
            self.gold_path = f"s3a://{os.getenv('AWS_S3_BUCKET_PROCESSED')}/gold/"
    
    def read_from_bronze(self, data_type="sensor_readings", date=None):
        """Read data from bronze layer"""
        if date:
            path = f"{self.bronze_path}{data_type}/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
        else:
            path = f"{self.bronze_path}{data_type}/"
        
        logger.info(f"Reading from bronze: {path}")
        
        try:
            df = self.spark.read.parquet(path)
            logger.info(f"Read {df.count()} records from bronze layer")
            return df
        except Exception as e:
            logger.error(f"Failed to read from bronze: {e}")
            # Fallback to local CSV
            return self.read_local_fallback()
    
    def read_local_fallback(self):
        """Fallback to reading local CSV files"""
        import glob
        csv_files = glob.glob("data/unified_sensor_data*.csv")
        if csv_files:
            latest_file = max(csv_files, key=os.path.getmtime)
            logger.info(f"Reading from local file: {latest_file}")
            return self.spark.read.option("header", "true").csv(latest_file, inferSchema=True)
        return None
    
    def process_to_silver(self, df):
        """Process bronze data to silver layer with quality checks"""
        logger.info("Processing data to silver layer")
        
        # Data quality transformations
        silver_df = df.filter(col("value").isNotNull()) \
            .filter(col("quality_score") > 0.5) \
            .dropDuplicates(["sensor_id", "timestamp"])
        
        # Add processing metadata
        silver_df = silver_df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processing_date", current_date())
        
        # Write to silver layer with partitioning
        output_path = f"{self.silver_path}clean_readings/"
        
        silver_df.write \
            .mode("overwrite") \
            .partitionBy("processing_date", "sensor_type") \
            .parquet(output_path)
        
        logger.info(f"Wrote {silver_df.count()} records to silver layer")
        return silver_df
    
    def create_gold_aggregations(self, df):
        """Create business-ready aggregations in gold layer"""
        logger.info("Creating gold layer aggregations")
        
        # Hourly aggregations
        hourly_agg = df.groupBy(
            window(col("timestamp"), "1 hour"),
            "sensor_type",
            "district"
        ).agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
            stddev("value").alias("std_value"),
            sum("anomaly_flag").alias("anomaly_count")
        ).withColumn("hour_start", col("window.start")) \
         .withColumn("hour_end", col("window.end")) \
         .drop("window")
        
        # Write to gold layer
        hourly_path = f"{self.gold_path}hourly_aggregations/"
        hourly_agg.write \
            .mode("overwrite") \
            .partitionBy("sensor_type") \
            .parquet(hourly_path)
        
        # Daily summaries
        daily_summary = df.groupBy(
            to_date("timestamp").alias("date"),
            "sensor_type",
            "district"
        ).agg(
            count("*").alias("total_readings"),
            avg("value").alias("daily_avg"),
            avg("quality_score").alias("avg_quality"),
            sum("anomaly_flag").alias("anomaly_count")
        )
        
        daily_path = f"{self.gold_path}daily_summaries/"
        daily_summary.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(daily_path)
        
        logger.info("Gold layer aggregations created")
        return hourly_agg, daily_summary
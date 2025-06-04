from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, avg, min, max, stddev, count, sum, lag, abs, greatest, countDistinct
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
import boto3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkWaterETL:
    """
    Advanced ETL pipeline using PySpark for scalable water management data processing.
    Handles large volumes of IoT sensor data with distributed computing.
    """
    
    def __init__(self, app_name="WaterManagementETL"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.s3_client = boto3.client('s3')
        
    def define_schema(self):
        """Define optimized schema for sensor data"""
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("district", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("quality_score", DoubleType(), True),
            StructField("anomaly_flag", IntegerType(), True)
        ])
    
    def read_sensor_data(self, s3_path):
        """Read sensor data from S3 with optimized schema"""
        logger.info(f"Reading sensor data from {s3_path}")
        
        schema = self.define_schema()
        
        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(s3_path)
        
        # Cache for multiple operations
        df.cache()
        
        record_count = df.count()
        logger.info(f"Loaded {record_count} sensor records")
        
        return df
    
    def advanced_data_cleaning(self, df):
        """Advanced data quality checks and feature engineering"""
        logger.info("Starting advanced data cleaning and feature engineering")
        
        # Add time-based features
        df = df.withColumn("year", year("timestamp")) \
              .withColumn("month", month("timestamp")) \
              .withColumn("day", dayofmonth("timestamp")) \
              .withColumn("hour", hour("timestamp")) \
              .withColumn("day_of_week", dayofweek("timestamp")) \
              .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
              .withColumn("is_peak_hour", when(col("hour").between(6, 9) | col("hour").between(18, 21), 1).otherwise(0))
        
        # Add business hour classification
        df = df.withColumn("time_period", 
                          when(col("hour").between(6, 11), "morning")
                          .when(col("hour").between(12, 17), "afternoon") 
                          .when(col("hour").between(18, 22), "evening")
                          .otherwise("night"))
        
        # Advanced outlier detection using statistical methods
        sensor_stats = df.groupBy("sensor_type") \
                        .agg(mean("value").alias("mean_val"),
                             stddev("value").alias("std_val"))
        
        # Join stats back to main dataframe
        df = df.join(sensor_stats, "sensor_type")
        
        # Calculate z-score and flag statistical outliers
        df = df.withColumn("z_score", abs((col("value") - col("mean_val")) / col("std_val"))) \
              .withColumn("is_statistical_outlier", when(col("z_score") > 3, 1).otherwise(0))
        
        # Drop temporary columns
        df = df.drop("mean_val", "std_val")
        
        logger.info("Advanced cleaning and feature engineering completed")
        return df
    
    def sliding_window_analytics(self, df):
        """Advanced time series analytics using sliding windows"""
        logger.info("Computing sliding window analytics")
        
        # Define window specifications
        sensor_window = Window.partitionBy("sensor_id").orderBy("timestamp")
        rolling_window = sensor_window.rowsBetween(-23, 0)  # 24-hour rolling window
        
        # Calculate rolling statistics
        df = df.withColumn("rolling_avg_24h", avg("value").over(rolling_window)) \
              .withColumn("rolling_std_24h", stddev("value").over(rolling_window)) \
              .withColumn("rolling_min_24h", min("value").over(rolling_window)) \
              .withColumn("rolling_max_24h", max("value").over(rolling_window))
        
        # Calculate rate of change
        df = df.withColumn("prev_value", lag("value", 1).over(sensor_window)) \
              .withColumn("value_change", col("value") - col("prev_value")) \
              .withColumn("value_change_pct", 
                         when(col("prev_value") != 0, 
                              (col("value_change") / col("prev_value")) * 100)
                         .otherwise(0))
        
        # Advanced anomaly detection
        df = df.withColumn("rolling_anomaly_score", 
                          when(col("rolling_std_24h") > 0,
                               abs((col("value") - col("rolling_avg_24h")) / col("rolling_std_24h")))
                          .otherwise(0))
        
        df = df.withColumn("is_trend_anomaly", 
                          when(col("rolling_anomaly_score") > 2.5, 1).otherwise(0))
        
        # Combine anomaly flags
        df = df.withColumn("combined_anomaly", 
                          greatest(col("anomaly_flag"), 
                                  col("is_statistical_outlier"), 
                                  col("is_trend_anomaly")))
        
        logger.info("Sliding window analytics completed")
        return df
    
    def district_performance_analytics(self, df):
        """Advanced district-level performance analytics"""
        logger.info("Computing district performance analytics")
        
        # District-level aggregations with multiple time granularities
        district_hourly = df.groupBy("district", "sensor_type", "year", "month", "day", "hour") \
                           .agg(
                               avg("value").alias("avg_value"),
                               min("value").alias("min_value"),
                               max("value").alias("max_value"),
                               stddev("value").alias("std_value"),
                               count("value").alias("reading_count"),
                               sum("combined_anomaly").alias("anomaly_count"),
                               avg("quality_score").alias("avg_quality"),
                               sum("is_peak_hour").alias("peak_hour_readings"),
                               avg("rolling_avg_24h").alias("trend_avg"),
                               avg("value_change_pct").alias("avg_change_pct")
                           )
        
        # Calculate efficiency metrics
        district_hourly = district_hourly.withColumn("efficiency_score",
                                                   when(col("anomaly_count") == 0, col("avg_quality"))
                                                   .otherwise(col("avg_quality") * 0.7))
        
        district_hourly = district_hourly.withColumn("volatility", 
                                                   col("std_value") / col("avg_value"))
        
        # Daily district summaries
        district_daily = df.groupBy("district", "sensor_type", "year", "month", "day") \
                          .agg(
                              avg("value").alias("daily_avg"),
                              min("value").alias("daily_min"),
                              max("value").alias("daily_max"),
                              sum("combined_anomaly").alias("daily_anomalies"),
                              avg("quality_score").alias("daily_quality"),
                              countDistinct("sensor_id").alias("active_sensors"),
                              sum(when(col("is_peak_hour") == 1, col("value"))).alias("peak_consumption"),
                              sum(when(col("is_peak_hour") == 0, col("value"))).alias("off_peak_consumption")
                          )
        
        # Calculate peak vs off-peak ratio
        district_daily = district_daily.withColumn("peak_ratio",
                                                 col("peak_consumption") / 
                                                 (col("peak_consumption") + col("off_peak_consumption")))
        
        logger.info("District performance analytics completed")
        return district_hourly, district_daily
    
    def machine_learning_insights(self, df):
        """Apply machine learning for sensor behavior clustering and insights"""
        logger.info("Starting machine learning analysis")
        
        # Prepare sensor behavior features
        sensor_features = df.groupBy("sensor_id", "sensor_type", "district") \
                           .agg(
                               avg("value").alias("avg_reading"),
                               stddev("value").alias("std_reading"),
                               avg("quality_score").alias("avg_quality"),
                               sum("combined_anomaly").alias("total_anomalies"),
                               avg("rolling_avg_24h").alias("trend_average"),
                               count("*").alias("total_readings")
                           )
        
        # Fill nulls and create feature vector
        sensor_features = sensor_features.fillna(0)
        
        feature_cols = ["avg_reading", "std_reading", "avg_quality", "total_anomalies", "trend_average"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(sensor_features)
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(feature_df)
        scaled_df = scaler_model.transform(feature_df)
        
        # K-means clustering to identify sensor behavior patterns
        kmeans = KMeans(k=5, seed=42, featuresCol="scaled_features", predictionCol="behavior_cluster")
        kmeans_model = kmeans.fit(scaled_df)
        clustered_df = kmeans_model.transform(scaled_df)
        
        # Analyze cluster characteristics
        cluster_summary = clustered_df.groupBy("behavior_cluster") \
                                    .agg(
                                        avg("avg_reading").alias("cluster_avg_reading"),
                                        avg("avg_quality").alias("cluster_avg_quality"),
                                        avg("total_anomalies").alias("cluster_avg_anomalies"),
                                        count("*").alias("sensors_in_cluster")
                                    )
        
        logger.info("Machine learning analysis completed")
        return clustered_df, cluster_summary
    
    def save_results_to_s3(self, df, bucket, key):
        """Save DataFrame results to S3 in parquet format"""
        temp_path = f"/tmp/spark_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Write to local temp first (for demo - in production use direct S3 write)
        df.coalesce(1).write.mode("overwrite").parquet(temp_path)
        
        # Upload to S3
        # Note: In production, use spark.write.option("path", "s3a://bucket/path").save()
        logger.info(f"Results prepared for S3 upload to {bucket}/{key}")
        
    def execute_advanced_pipeline(self, raw_bucket, processed_bucket):
        """Execute the complete advanced Spark ETL pipeline"""
        logger.info("Starting advanced Spark ETL pipeline")
        
        try:
            # Generate paths
            current_date = datetime.now()
            date_path = f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}"
            
            # Read data (in production, this would be s3a:// path)
            s3_path = f"s3a://{raw_bucket}/bronze/readings/{date_path}/readings.csv"
            # For demo, we'll read from local file
            local_path = "data/sensor_readings.csv"
            
            df = self.spark.read.option("header", "true").csv(local_path, inferSchema=True)
            
            # Convert timestamp column
            df = df.withColumn("timestamp", to_timestamp("timestamp"))
            
            # Execute pipeline stages
            cleaned_df = self.advanced_data_cleaning(df)
            enriched_df = self.sliding_window_analytics(cleaned_df)
            hourly_agg, daily_agg = self.district_performance_analytics(enriched_df)
            sensor_clusters, cluster_summary = self.machine_learning_insights(enriched_df)
            
            # Show sample results
            logger.info("Pipeline Results Summary:")
            logger.info(f"Total records processed: {enriched_df.count()}")
            logger.info(f"Unique sensors: {enriched_df.select('sensor_id').distinct().count()}")
            logger.info(f"Date range: {enriched_df.agg(min('timestamp'), max('timestamp')).collect()[0]}")
            
            print("\nCluster Analysis Summary:")
            cluster_summary.show()
            
            print("\nDaily District Performance (Sample):")
            daily_agg.show(10)
            
            print("\nAdvanced Anomaly Detection Results:")
            anomaly_summary = enriched_df.groupBy("district", "sensor_type") \
                                       .agg(sum("combined_anomaly").alias("total_anomalies"),
                                           avg("rolling_anomaly_score").alias("avg_anomaly_score")) \
                                       .orderBy(desc("total_anomalies"))
            anomaly_summary.show()
            
            logger.info("Advanced Spark ETL pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Advanced pipeline failed: {str(e)}")
            return False
        
        finally:
            self.spark.stop()

def main():
    """Entry point for Spark ETL pipeline"""
    
    # Configuration
    RAW_BUCKET = "water-project-raw"
    PROCESSED_BUCKET = "water-project-processed"
    
    # Initialize and run pipeline
    spark_etl = SparkWaterETL()
    success = spark_etl.execute_advanced_pipeline(RAW_BUCKET, PROCESSED_BUCKET)
    
    if success:
        print("\nAdvanced Spark ETL Pipeline Completed Successfully!")
        print("\nCapabilities demonstrated:")
        print("- Distributed data processing with PySpark")
        print("- Advanced time series analytics")
        print("- Statistical anomaly detection")
        print("- Machine learning clustering")
        print("- Scalable aggregations")
        print("- Production-ready error handling")
    else:
        print("Pipeline execution failed")

if __name__ == "__main__":
    main()
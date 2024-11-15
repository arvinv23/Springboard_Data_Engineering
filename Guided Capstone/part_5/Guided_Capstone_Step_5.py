#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install psycopg2-binary


# In[3]:


# First run these installation commands
get_ipython().system('pip install psycopg2-binary')
get_ipython().system('pip install azure-storage-blob')
get_ipython().system('pip install pyarrow')


# In[12]:


from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import pyarrow
import pandas as pd

class PreprocessingWorkflow:
    def __init__(self, storage_account_name, container_name, storage_account_key):
        self.storage_account = storage_account_name
        self.container = container_name
        self.access_key = storage_account_key
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("Data Preprocessing") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) \
            .getOrCreate()

    def ingest_raw_data(self, source_path, data_type):
        """Ingest raw data from source"""
        try:
            df = self.spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            # Basic data cleaning
            df = df.dropDuplicates()
            df = df.na.drop(subset=[col for col in df.columns if "_id" in col.lower()])
            
            return df
        except Exception as e:
            print(f"Error ingesting {data_type} data: {str(e)}")
            raise

    def validate_data(self, df, data_type):
        """Validate data quality"""
        validation_results = {
            "total_records": df.count(),
            "null_counts": {col: df.filter(df[col].isNull()).count() 
                          for col in df.columns},
            "duplicate_records": df.count() - df.dropDuplicates().count()
        }
        
        return validation_results

    def batch_load(self, df, destination_path, partition_cols=None):
        """Load data in batch mode to Delta Lake format"""
        try:
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            writer.save(destination_path)
            
        except Exception as e:
            print(f"Error in batch loading: {str(e)}")
            raise

    def process_daily_load(self, trade_date, source_paths):
        """Main method to process daily data load"""
        try:
            # Process trades
            trades_df = self.ingest_raw_data(
                f"{source_paths['trades']}/date={trade_date}", 
                "trades"
            )
            trade_validation = self.validate_data(trades_df, "trades")
            
            # Process quotes
            quotes_df = self.ingest_raw_data(
                f"{source_paths['quotes']}/date={trade_date}", 
                "quotes"
            )
            quote_validation = self.validate_data(quotes_df, "quotes")
            
            # Batch load to Delta Lake
            self.batch_load(
                trades_df,
                f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/processed/trades",
                ["trade_date"]
            )
            
            self.batch_load(
                quotes_df,
                f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/processed/quotes",
                ["quote_date"]
            )
            
            return {
                "trade_validation": trade_validation,
                "quote_validation": quote_validation
            }
            
        except Exception as e:
            print(f"Error in daily load process: {str(e)}")
            raise


# In[13]:


from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, lag, first, last, when, lit, avg
from datetime import datetime, timedelta

class AnalyticalWorkflow:
    def __init__(self, storage_account_name, container_name, storage_account_key):
        self.storage_account = storage_account_name
        self.container = container_name
        self.access_key = storage_account_key
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("Trade Analytics") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) \
            .getOrCreate()

    def calculate_price_analytics(self, df):
        """Calculate price-related analytics"""
        window_spec = Window.partitionBy("symbol").orderBy("trade_time")
        
        return df.withColumn("price_change", 
                           col("price") - lag("price").over(window_spec)) \
                .withColumn("daily_high", 
                           last("price").over(window_spec)) \
                .withColumn("daily_low", 
                           first("price").over(window_spec))

    def calculate_volume_analytics(self, df):
        """Calculate volume-related analytics"""
        window_spec = Window.partitionBy("symbol").orderBy("trade_time")
        
        return df.withColumn("cumulative_volume", 
                           sum("size").over(window_spec)) \
                .withColumn("vwap", 
                           expr("sum(price * size) over (partition by symbol) / sum(size) over (partition by symbol)"))

    def calculate_market_metrics(self, df):
        """Calculate market metrics"""
        window_30min = Window.partitionBy("symbol") \
            .orderBy("trade_time") \
            .rangeBetween(-30 * 60, 0)  # 30 minutes in seconds

        return df.withColumn("volatility", 
                           expr("stddev(price)").over(window_30min)) \
                .withColumn("moving_avg_price", 
                           avg("price").over(window_30min))

    def process_analytics(self, trade_date):
        """Main method to process analytics"""
        try:
            # Read processed data
            trades_path = f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/processed/trades/trade_date={trade_date}"
            quotes_path = f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/processed/quotes/quote_date={trade_date}"
            
            trades_df = self.spark.read.format("delta").load(trades_path)
            quotes_df = self.spark.read.format("delta").load(quotes_path)
            
            # Calculate analytics
            trades_df = self.calculate_price_analytics(trades_df)
            trades_df = self.calculate_volume_analytics(trades_df)
            trades_df = self.calculate_market_metrics(trades_df)
            
            # Join with quotes
            analytics_df = trades_df.join(
                quotes_df,
                (trades_df.symbol == quotes_df.symbol) &
                (trades_df.trade_time == quotes_df.quote_time),
                "left"
            )
            
            # Save analytics results
            output_path = f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/analytics/date={trade_date}"
            analytics_df.write.format("delta").mode("overwrite").save(output_path)
            
            return analytics_df
            
        except Exception as e:
            print(f"Error in analytics processing: {str(e)}")
            raise

    def generate_summary_metrics(self, df):
        """Generate summary metrics for reporting"""
        summary_metrics = df.groupBy("symbol").agg(
            avg("price").alias("avg_price"),
            avg("vwap").alias("avg_vwap"),
            avg("volatility").alias("avg_volatility"),
            sum("size").alias("total_volume")
        )
        return summary_metrics


# In[14]:


# Configuration
storage_account = "av_account"
container = "av_container"
access_key = "av_access_key"
trade_date = "2020-08-06"

source_paths = {
    "trades": "raw_data/trades",
    "quotes": "raw_data/quotes"
}

# Initialize workflows
preprocessing = PreprocessingWorkflow(storage_account, container, access_key)
analytics = AnalyticalWorkflow(storage_account, container, access_key)

# Execute preprocessing workflow
preprocessing_results = preprocessing.process_daily_load(trade_date, source_paths)

# Execute analytical workflow
analytics_df = analytics.process_analytics(trade_date)
summary_metrics = analytics.generate_summary_metrics(analytics_df)


# In[ ]:





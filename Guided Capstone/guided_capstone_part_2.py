from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

class DataIngestion:
    def __init__(self, spark, storage_account_name, storage_account_key):
        self.spark = spark
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self._configure_spark()

    def _configure_spark(self):
        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_account_name}.blob.core.windows.net",
            self.storage_account_key
        )

    def read_parquet(self, container_name, path):
        return self.spark.read.parquet(f"wasbs://{container_name}@{self.storage_account_name}.blob.core.windows.net/{path}")

class DataCleaning:
    @staticmethod
    def select_columns(df, columns):
        return df.select(*columns)

    @staticmethod
    def apply_latest(df):
        window_spec = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").orderBy(col("file_tm").desc())
        return df.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

class DataWriter:
    def __init__(self, storage_account_name):
        self.storage_account_name = storage_account_name

    def write_parquet(self, df, container_name, path):
        df.write.parquet(f"wasbs://{container_name}@{self.storage_account_name}.blob.core.windows.net/{path}")

class ETLProcess:
    def __init__(self, spark, storage_account_name, storage_account_key):
        self.ingestion = DataIngestion(spark, storage_account_name, storage_account_key)
        self.cleaning = DataCleaning()
        self.writer = DataWriter(storage_account_name)

    def process_trade_data(self, input_container, input_path, output_container, output_path):
        # Read Trade Partition Dataset
        trade_common = self.ingestion.read_parquet(input_container, f"{input_path}/partition=T")

        # Select necessary columns
        trade_columns = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "file_tm", "trade_pr"]
        trade = self.cleaning.select_columns(trade_common, trade_columns)

        # Apply data correction
        trade_corrected = self.cleaning.apply_latest(trade)

        # Write Trade Dataset
        self.writer.write_parquet(trade_corrected, output_container, f"{output_path}/trade")

    def process_quote_data(self, input_container, input_path, output_container, output_path):
        # Read Quote Partition Dataset
        quote_common = self.ingestion.read_parquet(input_container, f"{input_path}/partition=Q")

        # Select necessary columns
        quote_columns = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "file_tm", "bid_pr", "bid_size", "ask_pr", "ask_size"]
        quote = self.cleaning.select_columns(quote_common, quote_columns)

        # Apply data correction
        quote_corrected = self.cleaning.apply_latest(quote)

        # Write Quote Dataset
        self.writer.write_parquet(quote_corrected, output_container, f"{output_path}/quote")

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("EOD_Data_Load").getOrCreate()

    # Configuration
    storage_account_name = "<your-storage-account-name>"
    storage_account_key = "<your-storage-account-key>"
    input_container = "input-container"
    output_container = "output-container"
    input_path = "preprocessed_data"
    output_path = "eod_data"

    # Create ETL process
    etl_process = ETLProcess(spark, storage_account_name, storage_account_key)

    # Process Trade and Quote data
    etl_process.process_trade_data(input_container, input_path, output_container, output_path)
    etl_process.process_quote_data(input_container, input_path, output_container, output_path)

    # Stop Spark Session
    spark.stop()M
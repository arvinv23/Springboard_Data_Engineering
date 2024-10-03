from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.window import Window
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    def __init__(self, spark, storage_account, container):
        self.spark = spark
        self.storage_account = storage_account
        self.container = container

    @abstractmethod
    def read_data(self, partition):
        pass

    @abstractmethod
    def select_columns(self, df):
        pass

    def apply_latest(self, df):
        window = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb")
        return df.withColumn("latest", max("file_tm").over(window)) \
                 .where(col("file_tm") == col("latest")) \
                 .drop("latest")

    def process_data(self, trade_date, partition):
        df = self.read_data(partition)
        df = self.select_columns(df)
        df_corrected = self.apply_latest(df)
        self.write_data(df_corrected, trade_date)

    def write_data(self, df, trade_date):
        output_path = f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/{self.__class__.__name__.lower()}/trade_dt={trade_date}"
        df.write.mode("overwrite").parquet(output_path)

class TradeProcessor(DataProcessor):
    def read_data(self, partition):
        return self.spark.read.parquet(f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/output_dir/partition={partition}")

    def select_columns(self, df):
        return df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "file_tm", "trade_pr")

class QuoteProcessor(DataProcessor):
    def read_data(self, partition):
        return self.spark.read.parquet(f"wasbs://{self.container}@{self.storage_account}.blob.core.windows.net/output_dir/partition={partition}")

    def select_columns(self, df):
        return df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "file_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

class EODDataLoad:
    def __init__(self, storage_account, container, access_key):
        self.storage_account = storage_account
        self.container = container
        self.access_key = access_key
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        spark = SparkSession.builder \
            .appName("EOD Data Load") \
            .config(f"fs.azure.account.key.{self.storage_account}.blob.core.windows.net", self.access_key) \
            .getOrCreate()
        
        # Performance optimization: set broadcast threshold and shuffle partitions
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)  # 50MB
        spark.conf.set("spark.sql.shuffle.partitions", 200)
        
        return spark

    def run(self, trade_date):
        trade_processor = TradeProcessor(self.spark, self.storage_account, self.container)
        quote_processor = QuoteProcessor(self.spark, self.storage_account, self.container)

        trade_processor.process_data(trade_date, "T")
        quote_processor.process_data(trade_date, "Q")

    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    storage_account = "<storage-account-name>"
    container = "<container-name>"
    access_key = "<your-storage-account-access-key>"
    trade_date = "2020-08-06"

    eod_data_load = EODDataLoad(storage_account, container, access_key)
    eod_data_load.run(trade_date)
    eod_data_load.stop()
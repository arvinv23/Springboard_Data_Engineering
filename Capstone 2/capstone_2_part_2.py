from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, TimestampType, IntegerType, DecimalType
import json

class DataIngestionProcessor:
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.spark = self._create_spark_session()
        self._configure_azure_access(storage_account_name, storage_account_key)
        self.common_event_schema = self._define_schema()

    def _create_spark_session(self):
        return SparkSession.builder \
            .master('local') \
            .appName('DataIngestion') \
            .getOrCreate()

    def _configure_azure_access(self, storage_account_name, storage_account_key):
        self.spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
            storage_account_key
        )

    def _define_schema(self):
        return StructType([
            StructField("trade_dt", DateType(), True),
            StructField("rec_type", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("event_tm", TimestampType(), True),
            StructField("event_seq_nb", IntegerType(), True),
            StructField("arrival_tm", TimestampType(), True),
            StructField("trade_pr", DecimalType(10, 2), True),
            StructField("bid_pr", DecimalType(10, 2), True),
            StructField("bid_size", IntegerType(), True),
            StructField("ask_pr", DecimalType(10, 2), True),
            StructField("ask_size", IntegerType(), True),
            StructField("partition", StringType(), True)
        ])

    @staticmethod
    def _parse_csv(line):
        record = line.split(",")
        try:
            if record[2] == "T":
                return (record[0], "T", record[1], record[3], record[4], int(record[5]), record[6],
                        float(record[7]), None, None, None, None, "T")
            elif record[2] == "Q":
                return (record[0], "Q", record[1], record[3], record[4], int(record[5]), record[6],
                        None, float(record[7]), int(record[8]), float(record[9]), int(record[10]), "Q")
        except Exception:
            return (None,) * 12 + ("B",)

    @staticmethod
    def _parse_json(line):
        try:
            record = json.loads(line)
            if record['event_type'] == "T":
                return (record['trade_dt'], "T", record['symbol'], record['exchange'], record['event_tm'],
                        record['event_seq_nb'], record['file_tm'], record['price'], None, None, None, None, "T")
            elif record['event_type'] == "Q":
                return (record['trade_dt'], "Q", record['symbol'], record['exchange'], record['event_tm'],
                        record['event_seq_nb'], record['file_tm'], None, record['bid_pr'], record['bid_size'],
                        record['ask_pr'], record['ask_size'], "Q")
        except Exception:
            return (None,) * 12 + ("B",)

    def process_csv(self, csv_path):
        csv_rdd = self.spark.sparkContext.textFile(f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/{csv_path}")
        parsed_csv = csv_rdd.map(self._parse_csv)
        return self.spark.createDataFrame(parsed_csv, schema=self.common_event_schema)

    def process_json(self, json_path):
        json_rdd = self.spark.sparkContext.textFile(f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/{json_path}")
        parsed_json = json_rdd.map(self._parse_json)
        return self.spark.createDataFrame(parsed_json, schema=self.common_event_schema)

    def combine_and_write(self, csv_df, json_df, output_dir):
        combined_df = csv_df.union(json_df)
        combined_df.write.partitionBy("partition").mode("overwrite").parquet(
            f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/{output_dir}"
        )

    def verify_output(self, output_dir):
        for partition in ['T', 'Q', 'B']:
            print(f"Partition {partition}:")
            self.spark.read.parquet(f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/{output_dir}/partition={partition}").show()

    def run(self, csv_path, json_path, output_dir):
        csv_df = self.process_csv(csv_path)
        json_df = self.process_json(json_path)
        self.combine_and_write(csv_df, json_df, output_dir)
        self.verify_output(output_dir)

    def stop(self):
        self.spark.stop()


# Usage
if __name__ == "__main__":
    STORAGE_ACCOUNT_NAME = "<your-storage-account-name>"
    STORAGE_ACCOUNT_KEY = "<your-storage-account-access-key>"
    CONTAINER_NAME = "<your-container-name>"
    CSV_PATH = "<path_to_csv_files>"
    JSON_PATH = "<path_to_json_files>"
    OUTPUT_DIR = "output_dir"

    processor = DataIngestionProcessor(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME)
    processor.run(CSV_PATH, JSON_PATH, OUTPUT_DIR)
    processor.stop()
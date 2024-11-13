pip install azure-storage-blob
pip install pyarrow

storage_account = "sample_storageaccount"
container = "sample_container"
access_key = "sample_access_key"
trade_date = "2020-08-06"

etl = AnalyticalETL(None, storage_account, container, access_key)
final_df = etl.process_analytical_data(trade_date)

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, lag, first, last, when, lit, avg
from datetime import datetime, timedelta

class AnalyticalETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Trade Quote Analytics") \
            .getOrCreate()

    def _get_previous_business_date(self, current_date_str):
        current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
        prev_date = current_date - timedelta(days=1)
        while prev_date.weekday() > 4:  # 5 is Saturday, 6 is Sunday
            prev_date -= timedelta(days=1)
        return prev_date.strftime('%Y-%m-%d')

    def read_trade_data(self, trade_date):
        return self.spark.read.format("delta").load(f"/trade/trade_dt={trade_date}")

    def read_quote_data(self, trade_date):
        return self.spark.read.format("delta").load(f"/quote/trade_dt={trade_date}")

    def calculate_trade_moving_average(self, trade_df):
        """Calculate 30-minute moving average of trade prices"""
        window_spec = Window.partitionBy("symbol", "exchange") \
            .orderBy("event_tm") \
            .rangeBetween(-(30 * 60), 0)  # 30 minutes in seconds

        return trade_df.withColumn("mov_avg_pr", avg("trade_pr").over(window_spec))

    def get_prior_day_closing_prices(self, trade_date):
        """Get prior day's closing prices"""
        prev_date = self._get_previous_business_date(trade_date)
        prev_trade_df = self.read_trade_data(prev_date)
        
        window_spec = Window.partitionBy("symbol", "exchange") \
            .orderBy("event_tm")

        return prev_trade_df.withColumn(
            "close_pr",
            last("trade_pr").over(window_spec)
        ).select("symbol", "exchange", "close_pr").distinct()

    def create_quote_union(self, quote_df, trade_mov_avg_df):
        """Create unified view of quotes and trades"""
        quote_records = quote_df.select(
            col("trade_dt"),
            lit("Q").alias("rec_type"),
            "symbol", "event_tm", "event_seq_nb", "exchange",
            "bid_pr", "bid_size", "ask_pr", "ask_size",
            lit(None).cast("double").alias("trade_pr"),
            lit(None).cast("double").alias("mov_avg_pr")
        )

        trade_records = trade_mov_avg_df.select(
            col("trade_dt"),
            lit("T").alias("rec_type"),
            "symbol", "event_tm", "event_seq_nb", "exchange",
            lit(None).cast("double").alias("bid_pr"),
            lit(None).cast("int").alias("bid_size"),
            lit(None).cast("double").alias("ask_pr"),
            lit(None).cast("int").alias("ask_size"),
            "trade_pr", "mov_avg_pr"
        )

        return quote_records.unionByName(trade_records)

    def process_analytical_data(self, trade_date):
        """Main ETL processing function"""
        try:
            # Read input data
            quote_df = self.read_quote_data(trade_date)
            trade_df = self.read_trade_data(trade_date)

            # Calculate moving averages
            trade_mov_avg_df = self.calculate_trade_moving_average(trade_df)
            
            # Get prior day closing prices
            prior_day_closes = self.get_prior_day_closing_prices(trade_date)

            # Create union of quote and trade data
            quote_union = self.create_quote_union(quote_df, trade_mov_avg_df)

            # Register temporary views for SQL operations
            quote_union.createOrReplaceTempView("quote_union")
            prior_day_closes.createOrReplaceTempView("prior_day_closes")

            # Final analysis
            final_df = self.spark.sql("""
                SELECT 
                    q.trade_dt,
                    q.symbol,
                    q.event_tm,
                    q.event_seq_nb,
                    q.exchange,
                    q.bid_pr,
                    q.bid_size,
                    q.ask_pr,
                    q.ask_size,
                    q.trade_pr as last_trade_pr,
                    q.mov_avg_pr as last_mov_avg_pr,
                    q.bid_pr - COALESCE(p.close_pr, 0) as bid_pr_mv,
                    q.ask_pr - COALESCE(p.close_pr, 0) as ask_pr_mv
                FROM quote_union q
                LEFT JOIN prior_day_closes p
                ON q.symbol = p.symbol 
                AND q.exchange = p.exchange
                WHERE q.rec_type = 'Q'
            """)

            # Write results
            final_df.write.format("delta").mode("overwrite").partitionBy("trade_dt") \
                .save("/quote-trade-analytical")

            return final_df

        except Exception as e:
            print(f"Error in ETL process: {str(e)}")
            raise

if __name__ == "__main__":
    # Initialize ETL
    etl = AnalyticalETL()
    
    try:
        # Process data for a specific date
        trade_date = "2020-08-06"  # Modify this date as needed
        final_df = etl.process_analytical_data(trade_date)
        print("ETL process completed successfully")
        
    except Exception as e:
        print(f"ETL process failed: {str(e)}")

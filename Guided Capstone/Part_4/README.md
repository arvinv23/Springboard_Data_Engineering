# Trade and Quote Analytics ETL Pipeline

This project implements an analytical ETL pipeline for processing trade and quote data in a financial market context. The pipeline processes market data to generate analytics including moving averages, price movements, and quote/trade associations.

## Overview

The ETL pipeline:
- Processes trade and quote data stored in Delta format
- Calculates 30-minute moving averages for trade prices 
- Computes price movements relative to previous day closing prices
- Joins quote and trade data to create unified analytics
- Handles market schedules including holidays and weekends

## Architecture

The solution uses:
- Apache Spark for distributed processing
- Delta Lake for reliable data storage
- PySpark SQL for data transformations
- Databricks as the execution environment

## Key Components

### AnalyticalETL Class
Main class that orchestrates the ETL process with key methods:
- `process_analytical_data`: Main ETL workflow
- `calculate_trade_moving_average`: Computes price moving averages
- `get_prior_day_closing_prices`: Retrieves previous day closing prices
- `create_quote_union`: Unifies quote and trade records

### Data Processing
The pipeline handles:
- Trade data: `/trade/trade_dt={date}`
- Quote data: `/quote/trade_dt={date}`
- Output: `/quote-trade-analytical`

## Implementation Questions & Answers

## Q1: Regular SQL Join Usage
Can a regular SQL join be used for section 3.4.1?

**Answer:** Yes, a regular SQL join can be implemented as:
```sql
SELECT t.trade_dt,
       t.symbol,
       t.exchange,
       t.event_tm,
       t.event_seq_nb,
       t.trade_pr,
       q.bid_pr,
       q.bid_size,
       q.ask_pr,
       q.ask_size
FROM trade_common t
JOIN quote_common q
  ON t.symbol = q.symbol 
  AND t.exchange = q.exchange
  AND t.trade_dt = q.trade_dt
  AND q.event_tm <= t.event_tm
```

## Q2: Broadcast Join Implications
**Question:** What happens without the "BROADCAST" hint in join queries? How can you verify broadcast join usage?

### Answer
Without the BROADCAST hint, Spark's behavior is determined by several factors:

1. **Automatic Decision Making**
   - Spark automatically decides whether to broadcast based on table sizes
   - Default broadcast threshold is 10MB (configurable)
   - Larger tables default to sort-merge join

2. **Verification Methods**
```python
# Method 1: View the explain plan
df.explain(True)

# Method 2: Check Databricks/Spark Web UI
# Look for "BroadcastExchange" in the physical plan

# Method 3: Programmatic verification
def check_broadcast_join(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    return "BroadcastExchange" in plan
```

## Q3: Non-Business Day Data Handling
How to handle data submissions that don't occur on weekends and holidays?

## Solution Overview
The implementation provides a robust calendar-aware date handling system that accounts for market holidays and weekends.

## Calendar Management Implementation

```python
def get_previous_trading_date(self, current_date_str):
    current_date = pd.Timestamp(current_date_str)
    previous_date = current_date - pd.Timedelta(days=1)
    
    while (previous_date.strftime('%Y-%m-%d') in self.market_holidays or 
           previous_date.weekday() >= 5):
        previous_date -= pd.Timedelta(days=1)
        
    return previous_date.strftime('%Y-%m-%d')
```

## Key Features
- Maintains comprehensive market holiday schedule
- Validates trading dates
- Provides efficient previous trading date lookups
- Uses calendar table for optimized date operations

## Usage Example
```python
etl = AnalyticalETL()
trade_date = "2024-01-02"
final_df = etl.process_analytical_data(trade_date)
```

# Trade Analytics Implementation Guide

## Best Practices

### Data Quality & Performance
- Implement thorough input data validation
- Monitor pipeline performance metrics
- Handle weekend/holiday schedules appropriately

### Code Management
- Maintain comprehensive error handling
- Implement detailed logging
- Use version control for all outputs

## System Requirements
- PySpark
- Delta Lake
- Pandas
- Python 3.8+
- Databricks Runtime 7.0+

## Additional Notes
- Calendar table approach provides better performance for large-scale operations
- Holiday schedules should be updated annually
- Consider different market calendars for international trading

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

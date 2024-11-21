# Open Ended Capstone - Customer Orders Analysis Pipeline

## Project Overview
This project implements a comprehensive data analysis pipeline for pet food customer orders, focusing on customer behavior patterns and order analytics. The system processes multiple datasets to provide insights into purchasing patterns, customer preferences, and operational efficiency.

## Architecture
### Data Sources
- On-premise SQL Database (Pet Food Orders)
- Customer Behavior Logs

### Processing Components
- Azure Data Factory for ETL
- Azure Databricks for Analytics

### Storage Solutions
- Azure Data Lake Storage Gen2
- Azure SQL Database
- Cosmos DB for Web Application

## Datasets
The project analyzes the following datasets:

### Pet Food Customer Orders (pet_food_customer_orders.csv)
Key fields include:
- customer_id
- pet_id
- pet_order_number
- wet_food_order_number
- pet_has_active_subscription
- pet_food_tier
- pet_signup_datetime
- pet_allergen_list
- pet_health_issue_list
- order_payment_date

### Supporting Datasets
- aisles.csv: Product aisle information
- order_products__prior.csv: Historical order data
- order_products__train.csv: Training dataset
- departments.csv: Department classifications
- orders.csv: Order details
- products.csv: Product information

## Technical Dependencies
### Required Libraries
- plotly
- pandas
- numpy
- plotly.express
- plotly.graph_objects
- holoviews
- seaborn
- matplotlib

## Data Analysis Components
- Dataset loading and preprocessing
- Missing value analysis
- Feature distribution visualization
- Relationship analysis between variables
- Customer behavior pattern identification
- Order history analysis

## Implementation Steps

### Initial Setup and Analysis
1. Data import and validation
2. Exploratory data analysis
3. Feature engineering
4. Preliminary visualizations

### Architecture Setup
- Implemented scalable cloud infrastructure
- Integrated multiple data sources
- Established data processing workflows
- Created analytics pipelines

## Project Implementation Steps 8-10

## Step 8 - Deploy Code for Testing
I will deploy my customer orders analysis code to Azure compute resources for testing purposes. I need to create unit tests for all components of my data pipeline, with particular focus on edge cases in the pet food order data processing. I'll test my pipeline's ability to handle various scenarios like missing values in pet_allergen_list or invalid pet_signup_datetime formats. My code should demonstrate effective parallel processing of the large customer orders dataset. I'll ensure my test suite covers critical functionality like data transformation operations and calculations of metrics like total_order_kcal and wet_food_discount_percent.

### Key Testing Deliverables:
- Unit tests for all pipeline components
- Edge case testing
- Parallel processing validation
- Data transformation testing
- Metrics calculation verification

## Step 9 - Deploy Production Code & Process Dataset
After successful testing, I will deploy my finalized code to production Azure resources according to my architecture from Step 7. This means I'll set up Azure Data Factory for ETL processes, configure Azure Databricks for analytics, and establish proper connections to Azure Data Lake Storage Gen2 and Cosmos DB. I'll ensure my pipeline can efficiently process the entire pet food customer orders dataset, handling both historical data and new incoming orders. I'll pay critical attention to resource sizing to optimize costs while maintaining performance, particularly for compute-intensive operations like analyzing customer behavior patterns and order histories.

### Production Deployment Components:
- Azure Data Factory ETL setup
- Azure Databricks configuration
- Data Lake Storage Gen2 connection
- Cosmos DB integration
- Resource optimization

## Step 10 - Build Monitoring Dashboard
For my monitoring dashboard, I will use Azure Log Analytics to track key metrics of my deployed pipeline. My dashboard will monitor compute resource utilization during batch processing of order data, storage metrics for both the Data Lake and Cosmos DB, and network performance for real-time data ingestion. I'll set up visualizations for important operational metrics like pipeline execution times, data processing volumes, and error rates. I'll pay specific attention to monitoring the performance of customer behavior analysis components and wet food order processing, and I'll set up alerts for any anomalies in the data processing flow.

### Monitoring Components:
- Compute resource utilization tracking
- Storage metrics monitoring
- Network performance analytics
- Pipeline execution metrics
- Alert system setup

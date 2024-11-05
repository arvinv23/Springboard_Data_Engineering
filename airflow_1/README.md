# Stock Market Data Pipeline with Apache Airflow

## Overview
This project is a data pipeline built using Apache Airflow to automatically extract and analyze stock market data from Yahoo Finance. The pipeline is designed to download and process intraday stock market data for AAPL and TSLA stocks at one-minute intervals, providing a near real-time data processing workflow.

## Features
- **Daily data collection**: Automatically triggers at 6 PM (Monday-Friday)
- **Real-time stock data processing**
- **Parallel task execution**: Efficient task management for better performance
- **HDFS data storage integration**: Raw and processed data stored in HDFS
- **Automated market analysis reporting**
- **Built-in error handling and retries**: Ensures stability and reliability

## Table of Contents
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Directory Structure](#directory-structure)
- [Pipeline Components](#pipeline-components)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)
- [Support](#support)

## System Requirements
- **Python** 3.8+
- **Apache Airflow** 2.0+
- **HDFS**
- **Redis** (for Celery Executor)
- **PostgreSQL**

## Installation
# Install required packages
pip install apache-airflow
pip install yfinance
pip install pandas
pip install apache-airflow-providers-apache-hdfs

# Set up Airflow environment
export AIRFLOW_HOME=~/airflow
airflow db init
mkdir -p ~/airflow/dags

## Airflow Setup
executor = CeleryExecutor
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://airflow:airflow@localhost:5432/airflow

## Directory Structure
airflow_project/
├── dags/
│   └── market_data_dag.py
├── logs/
├── plugins/
└── config/

## Pipeline Components
# DAG Structure
Task 0: Initialize temporary storage
Task 1: Download AAPL stock data
Task 2: Download TSLA stock data
Task 3: Transfer AAPL data to HDFS
Task 4: Transfer TSLA data to HDFS
Task 5: Generate analysis report

#Task Dependencies
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5

## Usage
airflow webserver --port 8080
airflow scheduler
airflow celery worker

## Data Flow
# Data Collection
Source: Yahoo Finance API
Frequency: Daily at 6 PM
Interval: 1-minute intraday data

# Storage Locations
Raw data: /tmp/data/<date>/
Processed data: /user/airflow/stock_data/<date>/
Analysis report: /tmp/data/<date>/analysis_results.csv

# Monitoring
Web Interface: Accessible at http://localhost:8080
System Logs: Located in ~/airflow/logs/
Task Status: Available in the Airflow UI

# Troubleshooting
bash
Copy code
# Fix permissions
sudo chown -R $USER:$USER ~/airflow
chmod -R 775 ~/airflow

# Test HDFS connection
hadoop fs -ls /

# Verify dependencies
pip install -r requirements.txt
Development
Setting Up Development Environment
bash
Copy code

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install development dependencies
pip install -r requirements-dev.txt
Running Tests
bash
Copy code
pytest tests/
Contributing
Fork the repository
Create a feature branch
Commit your changes
Push to your branch
Submit a pull request

# License
This project is licensed under the MIT License.

# Acknowledgments
Yahoo Finance API
Apache Airflow Team
Hadoop Ecosystem Contributors

# Support
For support and questions, please open an issue in the repository.


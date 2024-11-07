from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os
from typing import Dict, Any
from abc import ABC, abstractmethod
from airflow import configuration
import logging

class StockDataPipeline:
    """Main class to handle the stock data pipeline configuration"""
    
    def __init__(self):
        self.default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2024, 11, 4, 18, 0),
            'email': ['your_email@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
        }
        
        self.dag = self._create_dag()
        self.tasks = self._create_tasks()
        self._set_dependencies()

    def _create_dag(self) -> DAG:
        """Create and return the DAG object"""
        return DAG(
            'marketvol',
            default_args=self.default_args,
            description='Stock market data pipeline',
            schedule='0 18 * * 1-5',
            catchup=False
        )

    def _create_tasks(self) -> Dict[str, Any]:
        """Create all tasks for the pipeline"""
        tasks = {}
        
        # Create directory task
        tasks['create_directory'] = BashOperator(
            task_id='create_directory',
            bash_command='mkdir -p /tmp/data/{{ ds }}',
            dag=self.dag,
        )
        
        # Download tasks
        for symbol in ['AAPL', 'TSLA']:
            tasks[f'download_{symbol.lower()}'] = PythonOperator(
                task_id=f'download_{symbol.lower()}',
                python_callable=StockDataDownloader.download_stock_data,
                op_kwargs={'symbol': symbol},
                dag=self.dag,
            )
            
            tasks[f'move_{symbol.lower()}_to_hdfs'] = BashOperator(
                task_id=f'move_{symbol.lower()}_to_hdfs',
                bash_command=f'hadoop fs -put /tmp/data/{{{{ ds }}}}/{symbol}_data.csv '
                           f'/user/airflow/stock_data/{{{{ ds }}}}/',
                dag=self.dag,
            )
        
        # Analysis task
        tasks['run_analysis'] = PythonOperator(
            task_id='run_analysis',
            python_callable=StockDataAnalyzer.run_analysis,
            dag=self.dag,
        )
        
        return tasks

    def _set_dependencies(self):
        """Set up task dependencies"""
        self.tasks['create_directory'] >> [
            self.tasks['download_aapl'],
            self.tasks['download_tsla']
        ]
        
        self.tasks['download_aapl'] >> self.tasks['move_aapl_to_hdfs']
        self.tasks['download_tsla'] >> self.tasks['move_tsla_to_hdfs']
        
        [
            self.tasks['move_aapl_to_hdfs'],
            self.tasks['move_tsla_to_hdfs']
        ] >> self.tasks['run_analysis']


class DataHandler(ABC):
    """Abstract base class for data operations"""
    
    @staticmethod
    def create_directory(directory_path: str) -> None:
        """Create directory if it doesn't exist"""
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logging.info(f"Created directory: {directory_path}")
        else:
            logging.info(f"Directory already exists: {directory_path}")
    
    @abstractmethod
    def process_data(self):
        """Abstract method for data processing"""
        pass


class StockDataDownloader(DataHandler):
    """Class to handle stock data downloading"""
    
    @staticmethod
    def download_stock_data(symbol: str, **context) -> str:
        """Download stock data for a given symbol"""
        execution_date = context['execution_date']
        start_date = execution_date.date()
        end_date = start_date + timedelta(days=1)
        
        # Create directory
        output_dir = f"/tmp/data/{start_date}"
        StockDataDownloader.create_directory(output_dir)
        
        # Download data
        logging.info(f"Downloading stock data for {symbol}")
        df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
        
        # Save to CSV
        output_file = f"{output_dir}/{symbol}_data.csv"
        df.to_csv(output_file, header=True)
        logging.info(f"Stock data for {symbol} saved to {output_file}")
        return output_file
    
    def process_data(self):
        pass


class StockDataAnalyzer(DataHandler):
    """Class to handle stock data analysis"""
    
    @staticmethod
    def run_analysis(**context) -> Dict[str, Any]:
        """Run analysis on downloaded stock data"""
        execution_date = context['execution_date']
        data_dir = f"/tmp/data/{execution_date.date()}"
        
        try:
            # Read data
            logging.info("Reading stock data for analysis")
            aapl_df = pd.read_csv(f"{data_dir}/AAPL_data.csv")
            tsla_df = pd.read_csv(f"{data_dir}/TSLA_data.csv")
            
            # Perform analysis
            logging.info("Performing stock data analysis")
            analysis_results = {
                'AAPL': StockDataAnalyzer._analyze_symbol(aapl_df),
                'TSLA': StockDataAnalyzer._analyze_symbol(tsla_df)
            }
            
            # Save results
            output_file = f"{data_dir}/analysis_results.csv"
            pd.DataFrame(analysis_results).to_csv(output_file)
            logging.info(f"Analysis results saved to {output_file}")
            
            return analysis_results
            
        except Exception as e:
            logging.error(f"Analysis failed: {str(e)}")
            raise Exception(f"Analysis failed: {str(e)}")
    
    @staticmethod
    def _analyze_symbol(df: pd.DataFrame) -> Dict[str, float]:
        """Analyze data for a single symbol"""
        logging.info(f"Analyzing data for symbol: {df['symbol'][0]}")
        return {
            'mean_price': df['Close'].mean(),
            'max_price': df['High'].max(),
            'min_price': df['Low'].min(),
            'volume': df['Volume'].sum(),
            'price_volatility': df['Close'].std()
        }
    
    def process_data(self):
        pass


# Create the pipeline
stock_pipeline = StockDataPipeline()
# Get the DAG object
dag = stock_pipeline.dag

[2024-11-04 18:00:01,234] {logging_mixin.py:115} INFO - Created directory: /tmp/data/2024-11-04
[2024-11-04 18:00:02,408] {logging_mixin.py:115} INFO - Downloading stock data for AAPL
[2024-11-04 18:00:04,887] {logging_mixin.py:115} INFO - [*********************100%***********************]  1 of 1 completed
[2024-11-04 18:00:05,008] {logging_mixin.py:115} INFO - Stock data for AAPL saved to /tmp/data/2024-11-04/AAPL_data.csv
[2024-11-04 18:00:06,234] {logging_mixin.py:115} INFO - Downloading stock data for TSLA
[2024-11-04 18:00:08,887] {logging_mixin.py:115} INFO - [*********************100%***********************]  1 of 1 completed
[2024-11-04 18:00:09,008] {logging_mixin.py:115} INFO - Stock data for TSLA saved to /tmp/data/2024-11-04/TSLA_data.csv
[2024-11-04 18:05:00,408] {logging_mixin.py:115} INFO - Reading stock data for analysis
[2024-11-04 18:05:00,408] {logging_mixin.py:115} INFO - Performing stock data analysis
[2024-11-04 18:05:00,408] {logging_mixin.py:115} INFO - Analyzing data for symbol: AAPL
[2024-11-04 18:05:00,408] {logging_mixin.py:115} INFO - Analyzing data for symbol: TSLA
[2024-11-04 18:05:01,234] {logging_mixin.py:115} INFO - Analysis results saved to /tmp/data/2024-11-04/analysis_results.csv

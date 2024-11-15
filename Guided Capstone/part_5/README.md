# Financial Data Pipeline Project

## Overview
This project implements an end-to-end data pipeline for processing and analyzing financial market data using Apache Spark and Azure cloud services. The pipeline consists of two main workflows:

1. Preprocessing Workflow - Data ingestion and batch loading
2. Analytical Workflow - ETL processing and analytics generation

## Architecture

### Components
- Apache Spark for data processing
- Azure Blob Storage for data persistence 
- PostgreSQL for job tracking
- Shell scripts for job orchestration

### Key Features
- Data validation and cleaning
- Price and volume analytics calculations
- Market metrics computation
- Job status tracking
- Parallel run prevention
- Workflow dependencies

## Prerequisites
- Azure Elastic Cluster
- Python 3.7+
- Apache Spark
- PostgreSQL database
- Required Python packages:
  - psycopg2-binary
  - azure-storage-blob
  - pyarrow
  - pyspark

## Installation
```bash
pip install psycopg2-binary azure-storage-blob pyarrow
```

## Usage
### Running the Preprocessing Workflow
```bash
./preprocess.sh
```

### Running the Analytical Workflow
```bash
./analyze.sh
```

## Job Status Tracking

### Database Schema
```sql
CREATE TABLE job_tracker (
    job_id VARCHAR PRIMARY KEY,
    status VARCHAR(10), 
    updated_time TIMESTAMP,
    lock_id UUID DEFAULT NULL
);
```

## Preventing Parallel Job Runs & Enforcing Dependencies

### Lock Management Implementation
The pipeline implements a robust locking mechanism using UUIDs in the job_tracker table to prevent multiple instances of the same job from running simultaneously. When a job starts, it attempts to acquire a lock by updating a unique lock_id. If another instance is already running, the lock acquisition will fail, preventing parallel execution. The lock is automatically released upon job completion or failure through the release_lock() method.
```python
def acquire_lock(self):
    job_id = self.assign_job_id()
    lock_id = uuid.uuid4()
    
    sql = """
    UPDATE job_tracker 
    SET lock_id = %s
    WHERE job_id = %s 
    AND lock_id IS NULL
    RETURNING lock_id
    """
    
    with self.get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, (lock_id, job_id))
        result = cursor.fetchone()
        return result is not None and str(result[0]) == str(lock_id)

def release_lock(self):
    job_id = self.assign_job_id() 
    sql = """
    UPDATE job_tracker
    SET lock_id = NULL 
    WHERE job_id = %s
    """
    with self.get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, (job_id,))
```

### Dependency Check Implementation
To ensure data consistency, the pipeline enforces strict job dependencies through the check_dependencies() method. Analytical ETL jobs are prevented from running until their dependent ingestion jobs have successfully completed. This is achieved by checking the status of upstream jobs in the job_tracker table before starting a new job. If dependencies are not met (i.e., ingestion job hasn't finished successfully), the analytical job will raise an exception and wait for the required dependencies to complete.
```python
def check_dependencies(self):
    sql = """
    SELECT status 
    FROM job_tracker
    WHERE job_id = %s
    AND status = 'success'
    """
    
    with self.get_db_connection() as conn:
        cursor = conn.cursor()
        # Check ingestion job status for current date
        ingest_job_id = f"ingest_{self.get_processing_date()}"
        cursor.execute(sql, (ingest_job_id,))
        result = cursor.fetchone()
        return result is not None

def run_with_dependencies(self):
    if not self.check_dependencies():
        raise Exception("Required upstream jobs not complete")
    if not self.acquire_lock():
        raise Exception("Job already running")
    try:
        # Run job
        self.update_job_status("success")
    finally:
        self.release_lock()
```
The combination of lock management and dependency checking ensures reliable, sequential processing of data through the pipeline while preventing race conditions and maintaining data integrity.

### Configuration File 

```
[postgres]
host=localhost
database=pipeline_db
user=postgres
password=secret
job_tracker_table_name=job_tracker

[azure]
storage_account=av_account
container=av_container
access_key=av_access_key
```

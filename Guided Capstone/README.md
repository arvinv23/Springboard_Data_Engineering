# My Guided Capstone Step 3: End-of-Day (EOD) Data Load

## Project Overview

I'm working on this project as part of the Guided Capstone for Spring Capital, an investment bank that relies on Big Data analytics for critical investment decisions. In this step, I'm focusing on creating the final data format for cloud storage, which will be used for both ETL processes and ad-hoc user queries.

## My Learning Objectives

By completing this step, I aim to:

1. Create Spark DataFrames using Parquet files
2. Perform data cleaning using Spark aggregation methods
3. Use cloud storage as output for my Spark jobs

## My Prerequisites

To complete this step, I need:

- PySpark knowledge: reading multiple Parquet files into a single DataFrame, performing transformations, and writing DataFrames
- Access to Azure Blob Storage or equivalent cloud storage
- Python and Spark development environment

## My Project Structure

I've structured my project with the following main components:

1. Data reading from temporary location
2. Column selection for trade and quote records
3. Data correction to handle updated records
4. Writing processed data back to cloud storage

## My Implementation Steps

### 1. Populating Trade Dataset

1.1 I'll read the Trade Partition Dataset
1.2 Then, I'll select the necessary columns
1.3 Next, I'll apply data correction
1.4 Finally, I'll write the Trade Dataset to Azure Blob Storage

### 2. Populating Quote Dataset

I'll follow the same method as for the Trade dataset

## Key Considerations

As I work on this project, I need to:

- Ensure I properly handle updated records by implementing the `applyLatest` method
- Optimize storage by selecting only necessary columns for each dataset
- Use an appropriate partitioning strategy when writing data back to cloud storage

## Running SQL Queries

To run SQL queries against my trade and quote data on Azure, I plan to:

1. Create external tables in Azure Synapse Analytics or Azure Databricks, pointing to the Parquet files in Azure Blob Storage
2. Use Spark SQL or standard SQL to query the data
3. Consider using Azure Data Lake Storage Gen2 for enhanced performance and security

## My Submission Plan

To complete this step, I will:

1. Implement the code for both Trade and Quote data processing
2. Ensure my `applyLatest` method is correctly implemented
3. Test my code with sample data
4. Commit and push my updated code to GitHub
5. Submit the GitHub link to my mentor for review

## Questions I'm Considering

As I work through this step, I'm thinking about:

- How can I optimize the data partitioning strategy for frequent queries?
- What additional data quality checks could I implement in this process?

## My Next Steps

After I complete this step, I'll move on to implementing analytical ETL jobs to calculate supplementary information for quote records.

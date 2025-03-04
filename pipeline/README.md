# AWS CDK Pipeline Stack

## Overview
This project defines an **AWS CDK Pipeline Stack** that automates data ingestion, transformation, and orchestration using **AWS Glue**. It includes Glue jobs, workflows, triggers, IAM roles, and S3 bucket deployments for efficient ETL processing.

## Infrastructure Components

### 1. **S3 Buckets**
- **Bucket Deployment Scripts**: Upload Glue ETL job scripts.

### 2. **IAM Roles**
- **Glue Job Role**: Grants Glue permissions to access S3 and other AWS services.
- **Bucket Deployment Role**: Manages S3 object uploads for job scripts.

### 3. **Glue Jobs**
- **Dimension Table Load Jobs**:
  - `load_venues.py`
  - `load_sales_channels.py`
  - `load_resellers.py`
  - `load_partnerships.py`
  - `load_organizers.py`
  - `load_events.py`
- **Initial Load Jobs**:
  - `initial_load_sales.py`: Loads sales data from PostgreSQL.
  - `initial_load_csv_sales.py`: Loads CSV-based sales data from S3.
- **Incremental Load Jobs**:
  - `load_sales.py`: Loads incremental sales data.
  - `load_csv_sales.py`: Processes daily sales CSV files.


### 4. **Glue Workflows & Triggers**
- **Main Workflow**
  - Runs daily at midnight UTC.
  - Executes all dimension and fact table jobs.
  - Triggers dependent jobs upon successful completion.
- **Initial Load Workflow**
  - Runs on demand for full data loads.

## Deployment

### Prerequisites
Ensure you have:
- **AWS CLI** configured with necessary permissions.
- **Node.js & AWS CDK** installed:
  ```sh
  npm install -g aws-cdk
  ```
- **Python Virtual Environment**:
  ```sh
  python -m venv .venv
  source .venv/bin/activate  # macOS/Linux
  .venv\Scripts\activate     # Windows
  pip install -r requirements.txt
  ```

### Environment Variables
Create a `.env` file in the **parent directory** (`../.env`) with the following:
```ini
ENV=dev
PROJECT=my-project
ACCOUNT_ID=123456789012
REGION=us-east-1
POSTGRES_SECRET_NAME=my-postgres-secret
VPC_NAME=my-vpc
SECURITY_GROUP=my-security-group
S3_BUCKET_DATA=my-data-bucket
S3_BUCKET_GLUE_JOBS=my-glue-jobs-bucket
GLUE_DATABASE_NAME=my-glue-db
```

### Deploy the Stack
Run:
```sh
cdk synth
cdk deploy
```

### Destroy the Stack
To remove all resources:
```sh
cdk destroy
```

## Summary
This AWS CDK Pipeline Stack automates **ETL processing** using AWS Glue workflows, making it easier to manage and scale data transformations for analytics and reporting. 🚀

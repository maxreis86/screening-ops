# AWS CDK Infrastructure Stack

## Overview

This project defines an AWS Cloud Development Kit (CDK) infrastructure stack that provisions and manages cloud resources for a **data-driven application**. It includes networking, storage, databases, and Glue resources to facilitate **ETL workflows and analytics**.

## Infrastructure Components

### 1. **Networking**
- **VPC**: A Virtual Private Cloud (VPC) is created to host resources securely.
- **Security Group**: Defines network access rules to control inbound and outbound traffic.

### 2. **Database**
- **Amazon RDS (PostgreSQL)**:
  - A **PostgreSQL RDS instance** is provisioned.
  - Credentials are securely stored in **AWS Secrets Manager**.
  - Configured with public accessibility and multi-AZ deployment.

### 3. **Storage**
- **Amazon S3 Buckets**:
  - **Data Storage**: Stores datasets, including external CSVs.
  - **Glue Jobs Storage**: Hosts AWS Glue job scripts.
  - **JDBC Driver Deployment**: The PostgreSQL JDBC driver is uploaded to S3.

### 4. **AWS Glue (ETL)**
- **AWS Glue Database**: Stores metadata for Parquet tables.

### 5. **Metabase (BI Tool)**
- **EC2 Instance**:
  - A **Metabase server** is deployed using an Amazon Linux EC2 instance.
  - **Docker** is installed and configured to run Metabase.
  - The security group allows **port 3000** for Metabase and **port 22** for SSH access.
  - The **public IP of the EC2 instance** is outputted for easy access.

## Deployment

### Prerequisites
Ensure you have:
- **AWS CLI** configured with the required credentials.
- **Node.js and AWS CDK** installed:
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
POSTGRES_USERNAME=admin
POSTGRES_SECRET_NAME=my-postgres-secret
VPC_NAME=my-vpc
SECURITY_GROUP=my-security-group
S3_BUCKET_DATA=my-data-bucket
S3_BUCKET_GLUE_JOBS=my-glue-jobs-bucket
GLUE_DATABASE_NAME=my-glue-db
POSTGRES_DATABASE_NAME=my-postgres-db
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
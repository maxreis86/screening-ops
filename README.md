Hi there!

If you are new here, I would recommend starting watching this vídeo [here](https://www.loom.com/share/09c4300660f348a1885915ae76772df7)

If you want to play arround with the final solution use the link and credentials below:

http://52.87.208.213:3000/dashboard/35-ticketing-overview

username: maxreis86@gmail.com

password: z_PiKN1Pl4CrD6

# AWS CDK Data Platform

## Overview
This project defines an **AWS CDK-based Data Platform** consisting of two main stacks:
1. **Infra Stack**: Sets up the foundational infrastructure, including networking, storage, databases, and AWS Glue resources.
2. **Pipeline Stack**: Automates data ingestion, transformation, and processing using AWS Glue workflows and Spark jobs.

## Project Structure
```
├── deliverables/                # Documentation and artifacts
├── infra/                       # Infra Stack - AWS resources provisioning
├── pipeline/                    # Pipeline Stack - Data processing and Glue workflows
├── .env                         # Environment variables file
├── .gitignore                   # Git ignore configuration
├── README.md                    # Project documentation
└── test_data_generator.ipynb     # Jupyter notebook for generating test data
```

## Infra Stack
The **Infra Stack** provisions the core infrastructure required for data processing:
- **Amazon RDS (PostgreSQL)**: Stores structured data securely.
- **Amazon S3 Buckets**: Used for data storage and Glue job scripts.
- **AWS Glue Catalog**: Manages metadata for structured data.
- **Amazon EC2 (Metabase)**: Hosts a business intelligence tool for data visualization.

## Pipeline Stack
The **Pipeline Stack** orchestrates data ingestion and transformation:
- **AWS Glue Jobs**: Spark-based ETL scripts process data.
- **AWS Glue Workflows**: Automate the execution of Glue jobs with triggers.
- **IAM Roles & Policies**: Manage secure access to AWS services.

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
Configure the `.env` file with required AWS parameters:
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

### Deploy the Stacks
```sh
cdk synth
cdk deploy --all
```

### Destroy the Stacks
To remove all resources:
```sh
cdk destroy --all
```

## Summary
This AWS CDK Data Platform automates the infrastructure setup and data processing pipeline, enabling seamless **ETL workflows, data storage, and analytics**. 🚀


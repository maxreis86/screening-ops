#!/usr/bin/env python3
import os
import sys
from aws_cdk import App
from pipeline.pipeline_stack import PipelineStack
from dotenv import load_dotenv

# Add the current directory to the Python path
sys.path.append(os.path.dirname(__file__))

# Load environment variables
load_dotenv("../.env")

env_name = os.getenv("ENV")
account_id = os.getenv("ACCOUNT_ID")
region = os.getenv("REGION")

app = App()

PipelineStack(
    app,
    f"ticketing-pipeline-stack-{env_name}",
    env={"account": account_id, "region": region},
)
app.synth()

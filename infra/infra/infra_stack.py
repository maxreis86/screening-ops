from aws_cdk import (
    aws_ec2 as ec2,
    aws_rds as rds,
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_deployment as s3deploy,
    aws_ec2 as ec2,
    CfnOutput,
)
from constructs import Construct
import os
import json
import datetime


class InfraStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Get env vars
        env = os.getenv("ENV")
        project = os.getenv("PROJECT")
        pg_db_username = os.getenv("POSTGRES_USERNAME")
        postgres_sm_name = os.getenv("POSTGRES_SECRET_NAME")
        vpc_name = os.getenv("VPC_NAME")
        security_group_name = os.getenv("SECURITY_GROUP")
        s3_bucket_name_data = os.getenv("S3_BUCKET_DATA")
        s3_bucket_name_glue_jobs = os.getenv("S3_BUCKET_GLUE_JOBS")
        glue_db_name = os.getenv("GLUE_DATABASE_NAME")
        postgres_db_name = os.getenv("POSTGRES_DATABASE_NAME")

        if not all(
            [
                env,
                pg_db_username,
                s3_bucket_name_data,
                s3_bucket_name_glue_jobs,
                postgres_sm_name,
                vpc_name,
                security_group_name,
                glue_db_name,
                postgres_db_name,
            ]
        ):
            raise ValueError(
                "One or more required environment variables are not defined."
            )

        # Creating VPC to control the access to the database
        vpc = ec2.Vpc(
            self,
            vpc_name,
            vpc_name=vpc_name,
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC, name="Public", cidr_mask=24
                )
            ],
        )
        # Explicitly tag the VPC with the correct Name
        Tags.of(vpc).add("Name", vpc_name)

        # Security group to allow all inbound and outbound traffic
        security_group = ec2.SecurityGroup(
            self,
            security_group_name,
            security_group_name=security_group_name,
            vpc=vpc,
            allow_all_outbound=True,
        )

        # Allow inbound traffic on port 3306 from any IP
        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(), connection=ec2.Port.tcp(5432)
        )

        security_group.add_ingress_rule(
            peer=security_group,  # Allow inbound traffic from itself
            connection=ec2.Port.all_traffic(),
        )

        # Create postgres credentials in Secrets Manager
        rds_secret = secretsmanager.Secret(
            self,
            postgres_sm_name,
            secret_name=postgres_sm_name,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps(
                    {"username": pg_db_username, "dbname": postgres_db_name}
                ),
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=32,
            ),
        )

        # Create the RDS instance with postgres
        rds_instance = rds.DatabaseInstance(
            self,
            f"{project}-postgres-db-instance-{env}",
            instance_identifier=f"{project}-postgres-db-instance-{env}",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16_3
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            credentials=rds.Credentials.from_secret(rds_secret),
            multi_az=True,
            allocated_storage=20,
            max_allocated_storage=100,
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            publicly_accessible=True,
            security_groups=[security_group],
        )

        # Create the S3 Bucket the store the CSV with 3rd party sales
        data_bucket = s3.Bucket(
            self,
            s3_bucket_name_data,
            bucket_name=s3_bucket_name_data,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create the S3 Bucket to store the spark glue jobs
        glue_job_bucket = s3.Bucket(
            self,
            s3_bucket_name_glue_jobs,
            bucket_name=s3_bucket_name_glue_jobs,  # Explicitly set a physical name
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,  # Security best practice
            encryption=s3.BucketEncryption.S3_MANAGED,  # Enable encryption for security
        )

        # Upload the PostgreSQL JDBC driver to the S3 bucket
        glue_drivers = s3deploy.BucketDeployment(
            self,
            "DeployPostgresJDBCDriver",
            sources=[
                s3deploy.Source.asset("./infra/glue-drivers")
            ],  # Path to local folder
            destination_bucket=glue_job_bucket,  # The bucket where Glue jobs are stored
            destination_key_prefix="glue-drivers",  # Folder in S3
        )

        # Create an AWS Glue Database for Parquet tables
        glue_database = glue.CfnDatabase(
            self,
            glue_db_name,
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=glue_db_name,
                description="Glue database for Parquet tables.",
                location_uri=f"s3://{s3_bucket_name_data}/glue_data/",
            ),
        )

        ## METABASE

        # Create IAM role for SSM
        ssm_role = iam.Role(
            self,
            "SSMRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                )
            ],
        )

        # Define an EC2 instance to host Metabase
        metabase_instance = ec2.Instance(
            self,
            f"{project}-metabase-instance-{env}",
            instance_name=f"{project}-metabase-instance-{env}",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2(),
            vpc=vpc,
            security_group=security_group,
            role=ssm_role,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            associate_public_ip_address=True,  # Ensure public IP is assigned
        )

        metabase_instance.add_user_data(
            "#!/bin/bash",
            "yum update -y",
            "amazon-linux-extras enable docker",
            "yum install -y docker",
            "service docker start",
            "usermod -a -G docker ec2-user",
            "chkconfig docker on",  # Ensures Docker starts on reboot
            "docker run -d -p 3000:3000 --restart always --name metabase metabase/metabase",
        )

        # Allow inbound traffic to Metabase port
        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(), connection=ec2.Port.tcp(3000)
        )

        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(), connection=ec2.Port.tcp(22)
        )

        # Output EC2 Public DNS
        CfnOutput(
            self,
            "Metabase Address",
            value="http://"+str(metabase_instance.instance_public_ip) + ":3000/",
        )

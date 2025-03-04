from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3_deployment as s3deploy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct
import os


class PipelineStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Get env vars
        env = os.getenv("ENV")
        project = os.getenv("PROJECT")
        postgres_sm_name = os.getenv("POSTGRES_SECRET_NAME")
        vpc_name = os.getenv("VPC_NAME")
        security_group_name = os.getenv("SECURITY_GROUP")
        s3_bucket_name_glue_jobs = os.getenv("S3_BUCKET_GLUE_JOBS")
        s3_bucket_name_data = os.getenv("S3_BUCKET_DATA")
        glue_db_name = os.getenv("GLUE_DATABASE_NAME")

        if not all(
            [
                env,
                project,
                s3_bucket_name_glue_jobs,
                postgres_sm_name,
                vpc_name,
                security_group_name,
                glue_db_name,
            ]
        ):
            raise ValueError(
                "One or more required environment variables are not defined."
            )

        glue_job_bucket = s3.Bucket.from_bucket_name(
            self,
            f"{project}-glue-job-bucket-{env}",
            bucket_name=s3_bucket_name_glue_jobs,
        )

        # Create a restricted IAM role for BucketDeployment
        bucket_deploy_role = iam.Role(
            self,
            f"{project}-bucket-deploy-role-{env}",
            role_name=f"{project}-bucket-deploy-role-{env}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Restrict permissions only to this S3 bucket
        bucket_deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
                resources=[
                    glue_job_bucket.bucket_arn,
                    f"{glue_job_bucket.bucket_arn}/*",
                ],
            )
        )

        # Deploy the Spark script and JDBC driver using an explicit role
        s3deploy.BucketDeployment(
            self,
            f"{project}-deploy-spark-script-{env}",
            sources=[s3deploy.Source.asset("./glue_jobs/")],
            destination_bucket=glue_job_bucket,
            destination_key_prefix="scripts/",
            role=bucket_deploy_role,  # Explicitly assign the IAM role
        )

        # Create an IAM role for Glue job
        glue_role = iam.Role(
            self,
            f"{project}-glue-job-role-{env}",
            role_name=f"{project}-glue-job-role-{env}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        glue_role.add_to_policy(iam.PolicyStatement(actions=["*"], resources=["*"]))

        # Grant full S3 read permissions + ListBucket to Glue
        glue_job_bucket.grant_read(glue_role)
        glue_job_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                principals=[glue_role],
                resources=[glue_job_bucket.bucket_arn],
            )
        )

        ## SPARK JOBS

        glue_load_organizers_job = glue.CfnJob(
            self,
            f"{project}-load-organizers-{env}",
            name=f"{project}-load-organizers-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_organizers.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-organizers-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_customers_job = glue.CfnJob(
            self,
            f"{project}-load-customers-{env}",
            name=f"{project}-load-customers-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_customers.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-customers-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_partnerships_job = glue.CfnJob(
            self,
            f"{project}-load-partnerships-{env}",
            name=f"{project}-load-partnerships-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_partnerships.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-partnerships-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_resellers_job = glue.CfnJob(
            self,
            f"{project}-load-resellers-{env}",
            name=f"{project}-load-resellers-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_resellers.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-resellers-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_venues_job = glue.CfnJob(
            self,
            f"{project}-load-venues-{env}",
            name=f"{project}-load-venues-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_venues.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-venues-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_events_job = glue.CfnJob(
            self,
            f"{project}-load-events-{env}",
            name=f"{project}-load-events-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_events.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-events-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_sales_channels_job = glue.CfnJob(
            self,
            f"{project}-load-sales-channels-{env}",
            name=f"{project}-load-sales-channels-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_sales_channels.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-sales-channels-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_sales_job = glue.CfnJob(
            self,
            f"{project}-load-sales-{env}",
            name=f"{project}-load-sales-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_sales.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-sales-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_initial_load_sales_job = glue.CfnJob(
            self,
            f"{project}-initial-load-sales-{env}",
            name=f"{project}-initial-load-sales-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/initial_load_sales.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-initial-load-sales-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_load_csv_sales_job = glue.CfnJob(
            self,
            f"{project}-load-csv-sales-{env}",
            name=f"{project}-load-csv-sales-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/load_csv_sales.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-load-csv-sales-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--S3_BUCKET_NAME_DATA": s3_bucket_name_data,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        glue_initial_load_csv_sales_job = glue.CfnJob(
            self,
            f"{project}-initial-load-csv-sales-{env}",
            name=f"{project}-initial-load-csv-sales-{env}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_job_bucket.bucket_name}/scripts/initial_load_csv_sales.py",
                python_version="3",
            ),
            role=glue_role.role_arn,
            glue_version="4.0",
            max_capacity=2,
            default_arguments={
                "--JOB_NAME": f"{project}-initial-load-csv-sales-{env}",
                "--GLUE_DATABASE_NAME": glue_db_name,
                "--PG_SECRECT_NAME": postgres_sm_name,
                "--S3_BUCKET_NAME_DATA": s3_bucket_name_data,
                "--REGION": self.region,
                "--ENV": env,
                "--enable-glue-datacatalog": "",  # Enable Glue Data Catalog as Hive Metastore
                "--extra-jars": f"s3://{glue_job_bucket.bucket_name}/glue-drivers/postgresql-42.7.5.jar",
            },
        )

        # Create Glue Workflow
        glue_workflow = glue.CfnWorkflow(
            self,
            f"{project}-glue-workflow-{env}",
            name=f"{project}-glue-workflow-{env}",
            description="Glue workflow to orchestrate ETL jobs."
        )

        # Start trigger: Runs "glue_load_resellers_job" and "glue_load_venues_job" in parallel at 00:00 UTC daily
        glue_trigger_parallel_jobs = glue.CfnTrigger(
            self,
            f"{project}-trigger-parallel-{env}",
            name=f"{project}-trigger-parallel-{env}",
            workflow_name=glue_workflow.name,
            type="SCHEDULED",  # Changed to scheduled trigger
            schedule="cron(0 0 * * ? *)",  # Runs at 00:00 UTC daily
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=glue_load_resellers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_venues_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_events_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_customers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_organizers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_partnerships_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_sales_channels_job.name),
            ]
        )

        # Dependent trigger: Runs "glue_load_csv_sales_job" after all jobs complete
        glue_trigger_csv_sales_job = glue.CfnTrigger(
            self,
            f"{project}-trigger-csv-sales-{env}",
            name=f"{project}-trigger-csv-sales-{env}",
            workflow_name=glue_workflow.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",  # Ensures all jobs must succeed
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_resellers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_venues_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_events_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_customers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_organizers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_partnerships_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_sales_channels_job.name,
                        state="SUCCEEDED",
                    ),
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=glue_load_csv_sales_job.name)],
        )

        glue_trigger_sales_job = glue.CfnTrigger(
            self,
            f"{project}-trigger-sales-{env}",
            name=f"{project}-trigger-sales-{env}",
            workflow_name=glue_workflow.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",  # Ensures both jobs must succeed
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_csv_sales_job.name,
                        state="SUCCEEDED",
                    ),
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=glue_load_sales_job.name)],
        )

        # Create Glue Workflow for the INITIAL LOAD
        glue_workflow_initial_load = glue.CfnWorkflow(
            self,
            f"{project}-glue-workflow-INITIAL-LOAD-{env}",
            name=f"{project}-glue-workflow-INITIAL-LOAD-{env}",
            description="Glue workflow to orchestrate ETL jobs"
        )

        # Start trigger: Runs "glue_load_resellers_job" and "glue_load_venues_job" in parallel at 00:00 UTC daily
        glue_trigger_parallel_jobs = glue.CfnTrigger(
            self,
            f"{project}-trigger-parallel-INITIAL-LOAD-{env}",
            name=f"{project}-trigger-parallel-INITIAL-LOAD-{env}",
            workflow_name=glue_workflow_initial_load.name,
            type="ON_DEMAND",  # Changed to scheduled trigger            
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=glue_load_resellers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_venues_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_events_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_customers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_organizers_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_partnerships_job.name),
                glue.CfnTrigger.ActionProperty(job_name=glue_load_sales_channels_job.name),
            ]
        )

        # Dependent trigger: Runs "glue_initial_load_csv_sales_job" after all jobs complete
        glue_trigger_csv_sales_initial_load_job = glue.CfnTrigger(
            self,
            f"{project}-trigger-csv-sales-INITIAL-LOAD-{env}",
            name=f"{project}-trigger-csv-sales-INITIAL-LOAD-{env}",
            workflow_name=glue_workflow_initial_load.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",  # Ensures both jobs must succeed
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_resellers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_venues_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_events_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_customers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_organizers_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_partnerships_job.name,
                        state="SUCCEEDED",
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_load_sales_channels_job.name,
                        state="SUCCEEDED",
                    ),
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=glue_initial_load_csv_sales_job.name)],
        )

        glue_trigger_sales_initial_load_job = glue.CfnTrigger(
            self,
            f"{project}-trigger-sales-INITIAL-LOAD-{env}",
            name=f"{project}-trigger-sales-INITIAL-LOAD-{env}",
            workflow_name=glue_workflow_initial_load.name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",  # Ensures both jobs must succeed
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=glue_initial_load_csv_sales_job.name,
                        state="SUCCEEDED",
                    ),
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=glue_initial_load_sales_job.name)],
        )
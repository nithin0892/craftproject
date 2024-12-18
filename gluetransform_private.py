import boto3
import os
import json
import requests
import zipfile
import logging

url = "https://www.stats.govt.nz/assets/Uploads/International-trade/International-trade-September-2024-quarter/Download-data/international-trade-september-2024-quarter-csv.zip"
local_download_path = "international_trade.zip"
extracted_data = "unzipped"
s3_bucket = "traderesults"
s3_script_key = "scripts/glue_etl_s3.py"
s3_path = "raw_data/"
transformed_path = "transformed-data/"
file_name = "output_csv_full.csv"
local_script_path = "glue_etl_s3.py" 
role_name = "DataPipelineRole"
glue_job_name = "ETLab"
glue_crawler_name = "glue-crawler"
glue_db = "Gluemeta"
policy_glue = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": ["glue.amazonaws.com"]
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
permission_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "glue:*",
                "athena:*",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_s3_bucket():
    s3 = boto3.client("s3")
    try:
        s3.create_bucket(Bucket=s3_bucket)
        logger.info(f"S3 bucket {s3_bucket} created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"S3 bucket {s3_bucket} already exists.")
    except Exception as e:
        logger.error(f"Failed to create S3 bucket: {e}")

def upload_etl_script_to_s3():
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_script_path, s3_bucket, s3_script_key)
        logger.info(f"ETL script uploaded to s3://{s3_bucket}/{s3_script_key}")
    except Exception as e:
        logger.error(f"Failed to upload ETL script: {e}")

def create_iam_role():
    iam = boto3.client("iam")
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(policy_glue)
        )
        logger.info(f"IAM role {role_name} created.")
    except iam.exceptions.EntityAlreadyExistsException:
        logger.info(f"IAM role {role_name} already exists.")
    except Exception as e:
        logger.error(f"Failed to create IAM role: {e}")

    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="Gluepolicy",
            PolicyDocument=json.dumps(permission_policy)
        )
        logger.info(f"Permissions policy attached to {role_name}.")
    except Exception as e:
        logger.error(f"Failed to attach policy: {e}")


def create_redshift_serverless_connection():
    glue = boto3.client("glue")

    # Redshift Serverless endpoint and database info
    redshift_endpoint = "redshiftwg.329599635672.us-east-1.redshift-serverless.amazonaws.com"  # Replace with your endpoint
    redshift_port = "5439"  # Default port for Redshift
    redshift_database = "red_schema"  # Replace with your Redshift database name
    redshift_jdbc_url = f"jdbc:redshift://{redshift_endpoint}:{redshift_port}/{redshift_database}"

    # VPC and security group details
    # Your existing VPC and Security Group IDs
    vpc_id = "vpc-06fbf42638eaf1f89"  # Replace with your VPC ID
    subnet_id = "subnet-0556ce1171a851901"  # Replace with your subnet ID (you can choose an appropriate subnet for Glue)
    security_group_ids = ["gluesg","redshiftsg","vpcendpointsg","ec2SG"]  # Replace with your Security Group ID(s)
    role_arn = "arn:aws:iam::329599635672:role/roleforredshift"
    try:
        glue.create_connection(
            ConnectionInput={
                'Name': 'RedshiftServerlessConnection',  # Name of the connection
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': redshift_jdbc_url,
                    'SECRET': 'arn:aws:secretsmanager:us-east-1:329599635672:secret:redshift!rsnamespace-admin-KmoXvJ'
                    'IAM_ROLE':  role_arn # Required for IAM-based authentication
                },
                'PhysicalConnectionRequirements': {
                    'AvailabilityZone': 'us-east-1a',  # Choose appropriate AZ for your VPC
                    'SecurityGroupIdList': security_group_ids,
                    'SubnetId': subnet_id
                    #'VpcId': vpc_id
                }
            }
        )
        logger.info("Redshift Serverless Glue connection created successfully.")
    except glue.exceptions.AlreadyExistsException:
        logger.info("Redshift Serverless Glue connection already exists.")
    except Exception as e:
        logger.error(f"Failed to create Redshift Serverless connection: {e}")

def create_glue_job():
    glue = boto3.client("glue")
    script_location = f"s3://{s3_bucket}/{s3_script_key}"

    # The connection you created for Redshift Serverless
    connection_name = "RedshiftServerlessConnection"  # This is the name of the connection created

    try:
        glue.create_job(
            Name=glue_job_name,
            Role=role_name,
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-bookmark-option": "job-bookmark-disable",
                "--enable-metrics": "true",
                "--enable-continuous-logging": "true"
            },
            MaxCapacity=2.0,  # Adjust capacity as needed
            Connections= {
                'Connections': ['RedshiftServerlessConnection']  # Add the Redshift connection here
            }
        )
        logger.info(f"Glue job {glue_job_name} created with Redshift Serverless connection.")
    except glue.exceptions.AlreadyExistsException:
        logger.info(f"Glue job {glue_job_name} already exists.")
    except Exception as e:
        logger.error(f"Failed to create Glue job: {e}")

def glue_crawler():
    glue = boto3.client("glue")
    s3_glue_path = f"s3://{s3_bucket}/{transformed_path}"

    try:
        glue.create_crawler(
            Name=glue_crawler_name,
            Role=role_name,
            DatabaseName=dev,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_glue_path
                    }
                ]
            },
            Schedule='cron(0/30 * * * ? *)',
            TablePrefix='s3_parquet_',  
        )
        logger.info(f"Glue Crawler {glue_crawler_name} created.")
    except glue.exceptions.AlreadyExistsException:
        logger.info(f"Glue Crawler {glue_crawler_name} already exists.")
    except Exception as e:
        logger.error(f"Failed to create Glue Crawler: {e}")

def main():
    #create_s3_bucket()
    #upload_etl_script_to_s3()
    #create_iam_role()
    create_redshift_serverless_connection()
    create_glue_job()
    #trigger_glue_job()
    #glue_crawler()

if __name__ == "__main__":
    main()

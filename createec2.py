import boto3
import time
import json
import base64


get_ec2 = boto3.client('ec2')
get_iam = boto3.client('iam')


sg_resp = get_ec2.create_security_group(
    GroupName='ec2SG',
    Description='ssh ec2'
)

sg_id = sg_resp['GroupId']
print(f"Security Group ID is: {sg_id}")


get_ec2.authorize_security_group_ingress(
    GroupId=sg_id,
    IpPermissions=[
        {
            'IpProtocol': 'tcp',
            'FromPort': 22,
            'ToPort': 22,
            'IpRanges': [{'CidrIp': '47.204.223.71/32'}]
        }
    ]
)

print(f"SSH access granted to IP for EC2 instance.")


s3_ec2_role = 's3AccessRole'

s3_access_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

create_role_resp = get_iam.create_role(
    RoleName=s3_ec2_role,
    AssumeRolePolicyDocument=json.dumps(s3_access_policy)
)

s3_arn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'

get_iam.attach_role_policy(
    RoleName=s3_ec2_role,
    PolicyArn=s3_arn
)

cloudwatch_arn = 'arn:aws:iam::aws:policy/CloudWatchFullAccess'

get_iam.attach_role_policy(
    RoleName=s3_ec2_role,
    PolicyArn=cloudwatch_arn
)

print(f"Role '{s3_ec2_role}' is created and AmazonS3ReadOnlyAccess, CloudWatchFullAccess policies are attached.")


instance_profile_name = 's3AccessInstanceProfile'


#instance_profile = get_iam.create_instance_profile(
#    InstanceProfileName=instance_profile_name
#)


get_iam.add_role_to_instance_profile(
    InstanceProfileName=instance_profile_name,
    RoleName=s3_ec2_role
)

print(f"Instance Profile '{instance_profile_name}' created and Role '{s3_ec2_role}' added.")


ud_script = """#!/bin/bash
yum install -y amazon-cloudwatch-agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-config-wizard
sudo systemctl enable amazon-cloudwatch-agent
sudo systemctl start amazon-cloudwatch-agent
"""


encoded_ud_script = base64.b64encode(ud_script.encode('utf-8')).decode('utf-8')


response = get_ec2.run_instances(
    ImageId='ami-0453ec754f44f9a4a',  
    InstanceType='t2.micro',
    MinCount=1,
    MaxCount=1,
    KeyName='nithinlabkey',  
    SecurityGroupIds=[sg_id],
    SubnetId='subnet-0f51ebf109bcabd87',
    IamInstanceProfile={'Name': instance_profile_name}, 
    UserData=encoded_ud_script  
)

ec2_instance_id = response['Instances'][0]['InstanceId']
print(f"EC2 Instance {ec2_instance_id} launched.")

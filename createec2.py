import boto3
import time
import json
import base64

ec2_client = boto3.client('ec2')
vpcid = 'vpc-06fbf42638eaf1f89'
instance_profile_name = 'Bastionprofile'
# Public SG 
public_sg_resp = ec2_client.create_security_group(
    GroupName='PublicSubnetSG',
    Description='Allow ingress from  47.204.223.71/32 for Public Subnet EC2',
    VpcId=vpcid
)
public_sg_id = public_sg_resp['GroupId']
print(f"Public Security Group ID: {public_sg_id}")

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

instance_profile = get_iam.create_instance_profile(
    InstanceProfileName=instance_profile_name



get_iam.add_role_to_instance_profile(
    InstanceProfileName=instance_profile_name,
    RoleName=s3_ec2_role
)

print(f"Instance Profile '{instance_profile_name}' created and Role '{s3_ec2_role}' added.")

ec2_client.authorize_security_group_ingress(
    GroupId=public_sg_id,
    IpPermissions=[
        {
            'IpProtocol': 'tcp',
            'FromPort': 22,
            'ToPort': 22,
            'IpRanges': [{'CidrIp': ' 47.204.223.71/32'}]
        }
    ]
)
print("Ingress rule added to Public Security Group.")

#Private SG
private_sg_resp = ec2_client.create_security_group(
    GroupName='PrivateSubnetSG',
    Description='Allow SSH from Public Security Group'
    VpcId=vpcid
)
private_sg_id = private_sg_resp['GroupId']
print(f"Private Security Group ID: {private_sg_id}")
ec2_client.authorize_security_group_ingress(
    GroupId=private_sg_id,
    IpPermissions=[
        {
            'IpProtocol': 'tcp',
            'FromPort': 22,
            'ToPort': 22,
            'UserIdGroupPairs': [{'GroupId': public_sg_id}]
        }
    ]
)
print("Ingress rule added to Private Security Group.")

# UD script for cloudwatch installation 
ud_script = """#!/bin/bash
yum install -y amazon-cloudwatch-agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-config-wizard
sudo systemctl enable amazon-cloudwatch-agent
sudo systemctl start amazon-cloudwatch-agent
"""
encoded_ud_script = base64.b64encode(ud_script.encode('utf-8')).decode('utf-8')
#Bastion host 
try:

    public_instance_resp = ec2_client.run_instances(
        ImageId='ami-0453ec754f44f9a4a',
        InstanceType='t2.micro',
        MinCount=1,
        MaxCount=1,
        KeyName='nithinlabkey',
        SecurityGroupIds=[public_sg_id],
        SubnetId='subnet-0ddfb90a4d814de63',
        UserData=encoded_ud_script
    )
except Exception as e :
    print(f"Failed to launch Public EC2 instance: {str(e)}")

public_instance_id = public_instance_resp['Instances'][0]['InstanceId']
print(f"Public EC2 Instance {public_instance_id} launched.")

# Private EC2 instance 
try:
    private_instance_resp = ec2_client.run_instances(
        ImageId='ami-0453ec754f44f9a4a',
        InstanceType='t2.micro',
        MinCount=1,
        MaxCount=1,
        KeyName='nithinlabkey',
        SecurityGroupIds=[private_sg_id],
        SubnetId='subnet-07d5c2276e8c7066d',
        UserData=encoded_ud_script,
        IamInstanceProfile={'Name': instance_profile_name}
    )
    private_instance_id = private_instance_resp['Instances'][0]['InstanceId']
    print(f"Private EC2 Instance {private_instance_id} launched.")
except Exception as e :
    print(f"Failed to launch Private EC2 instance: {str(e)}")


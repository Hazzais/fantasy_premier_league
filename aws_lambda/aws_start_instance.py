import os

import boto3
import paramiko


# Assign using environment variables for now - TODO: want to think about what I'm doing with this
# as lambda won't have pem file by default
INSTANCE_ID = os.environ.get('EC2_ID')
PEM_LOC = os.environ.get('PATH_TO_EC2_PEM')

ec2 = boto3.client('ec2')

ec2.start_instances(
    InstanceIds=[INSTANCE_ID]
)

info = ec2.describe_instances\
        (
    InstanceIds=[INSTANCE_ID]
)

instance_state = ec2.describe_instances(
    InstanceIds=[INSTANCE_ID]
)['Reservations'][0]['Instances'][0]['State']['Name']

instance_ip_public = ec2.describe_instances(
    InstanceIds=[INSTANCE_ID]
)['Reservations'][0]['Instances'][0]['PublicIpAddress']

ec2.stop_instances(
    InstanceIds=[INSTANCE_ID]
)

ssm = boto3.client('ssm')

ret = ssm.send_command(
    InstanceIds=[INSTANCE_ID],
    DocumentName='AWS-RunShellScript',
    Parameters={'commands': ['touch simple-file']},
    Comment='Test simple file creation'
)


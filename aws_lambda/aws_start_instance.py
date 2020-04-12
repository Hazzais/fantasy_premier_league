import boto3
import paramiko


INSTANCE_ID = 'i-0ed884846e3bb1ca6'
PEM_LOC = '~/.aws/aws-key-pairs/first-fpl-ec2-key-pair.pem'

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


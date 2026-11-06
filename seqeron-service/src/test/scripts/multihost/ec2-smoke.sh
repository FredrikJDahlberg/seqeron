#!/bin/bash
# One c6in.8xlarge in the default VPC; all three members, host and bench on it (MULTIHOST_MEMBERS=localhost x3).
set -euo pipefail

export AWS_REGION=us-east-1
KEY=seqeron-smoke
SG=seqeron-smoke
MYIP=$(curl -s https://checkip.amazonaws.com)

# Key pair (once)
[[ ! -e ~/.ssh/$KEY.pem ]] || { echo "ERROR: ~/.ssh/$KEY.pem exists" >&2; exit 1; }
(umask 077; aws ec2 create-key-pair --key-name "$KEY" --query KeyMaterial --output text > ~/.ssh/$KEY.pem)

# Security group: ssh from this machine only
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
SGID=$(aws ec2 create-security-group --group-name "$SG" --description "seqeron smoke" --vpc-id "$VPC" \
    --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SGID" --protocol tcp --port 22 --cidr "$MYIP/32"

# Stock Ubuntu 24.04 x86_64 (the AMI of doc/ec2-tests.md section 4 is not built yet)
AMI=$(aws ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)

# Self-terminating after 2 h; hyper-threading off (16 physical cores)
USERDATA=$(mktemp)
printf '#!/bin/bash\nshutdown -h +120\n' > "$USERDATA"
ID=$(aws ec2 run-instances \
    --image-id "$AMI" --instance-type c6in.8xlarge \
    --cpu-options CoreCount=16,ThreadsPerCore=1 \
    --key-name "$KEY" --security-group-ids "$SGID" \
    --instance-initiated-shutdown-behavior terminate \
    --user-data "file://$USERDATA" \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=seqeron-smoke}]' \
    --query 'Instances[0].InstanceId' --output text)
echo "instance $ID"

aws ec2 wait instance-running --instance-ids "$ID"
IP=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "ssh -i ~/.ssh/$KEY.pem ubuntu@$IP"

# Teardown (run when done):
#   aws ec2 terminate-instances --instance-ids $ID
#   aws ec2 wait instance-terminated --instance-ids $ID
#   aws ec2 delete-security-group --group-id $SGID
#   aws ec2 delete-key-pair --key-name $KEY && rm ~/.ssh/$KEY.pem

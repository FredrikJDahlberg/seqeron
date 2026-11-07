# The AWS account for the EC2 tests

What [`ec2-tests.md`](ec2-tests.md) needs from an AWS account, in the order to set it up: the account and its users, the
cost limits, the permissions the tests run under, the region and quotas, the network, the image, how a host is launched,
and how to check that nothing is left running. The commands use `us-east-1` and a 10.0.0.0/24 subnet, the values
`ec2-tests.md` §5 assumes; change them together.

Nothing here is part of the build. No script in this tree creates an account, a user, a budget or a network; the
instances themselves are launched by [`ec2-smoke.sh`](../seqeron-service/src/test/scripts/multihost/ec2-smoke.sh)
for the smoke test and by the commands in §8 for the full plan.

Variables used throughout:

```bash
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
export AZ=us-east-1a        # one Availability Zone for every host; §4 says how to choose it
export PROJECT=seqeron-bench
```

## 1. The account and the root user

1. Register the account with a payment method.
2. Sign in as the root user and enable MFA on it (IAM, "Security credentials"). Create no root access keys, and use the
   root user afterwards only for what only it can do.
3. Set the alternate billing contact (Account) and turn on IAM access to the Billing console (Account, "IAM user and
   role access to Billing information"). The administrator in §2 then reads costs without the root user.

## 2. An administrator without long-lived keys

IAM Identity Center gives short-lived credentials from `aws sso login`; an IAM user with an access key is the fallback
when it is unavailable.

1. Console: IAM Identity Center, Enable. Its region cannot be changed afterwards without deleting the instance, so
   choose one to keep.
2. Create a user for yourself, with MFA, and a permission set. Give the administrator `AdministratorAccess` for the
   account setup; §5 narrows what the benchmark runs under. Assign the user and the permission set to the account.
3. Configure the CLI and sign in:

```bash
aws configure sso            # session name, start URL, region, then the account and permission set
aws sso login --profile <profile>
export AWS_PROFILE=<profile>
aws sts get-caller-identity
```

With an IAM user instead: create the user with a programmatic key, run `aws configure`, and rotate the key after the
session. Keep the key out of this repository and out of scripts' environments that are copied to the instances.

## 3. Limits on cost

Every hour a forgotten Test 1 instance set runs costs $6.35 on c6in.8xlarge and $20.26 on c6id.metal (`ec2-tests.md` §3).

**A budget with alerts**, sized to the session: about $50 for tests 1a, 2 and 3, and the budget for 1b on top.

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
cat > /tmp/budget.json <<EOF
{ "BudgetName": "seqeron-bench", "BudgetType": "COST", "TimeUnit": "MONTHLY",
  "BudgetLimit": { "Amount": "50", "Unit": "USD" } }
EOF
cat > /tmp/notifications.json <<EOF
[ { "Notification": { "NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN",
                      "Threshold": 50, "ThresholdType": "PERCENTAGE" },
    "Subscribers": [ { "SubscriptionType": "EMAIL", "Address": "you@example.com" } ] },
  { "Notification": { "NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN",
                      "Threshold": 80, "ThresholdType": "PERCENTAGE" },
    "Subscribers": [ { "SubscriptionType": "EMAIL", "Address": "you@example.com" } ] },
  { "Notification": { "NotificationType": "FORECASTED", "ComparisonOperator": "GREATER_THAN",
                      "Threshold": 100, "ThresholdType": "PERCENTAGE" },
    "Subscribers": [ { "SubscriptionType": "EMAIL", "Address": "you@example.com" } ] } ]
EOF
aws budgets create-budget --account-id "$ACCOUNT" --budget file:///tmp/budget.json \
    --notifications-with-subscribers file:///tmp/notifications.json
```

A budget as set up here alerts after the spend and stops nothing. Billing data lags by hours, so it is a second line behind the
shutdown timer below.

**Cost allocation tag.** Activate `project` as a cost allocation tag (Billing, "Cost allocation tags"), and tag every
resource `project=seqeron-bench`, as §8's commands do. Cost Explorer then shows the benchmark's cost on its own.

**Cost Anomaly Detection** (Billing, free): one monitor on the account's services, alerting by email.

**The shutdown timer.** Every instance is launched with `--instance-initiated-shutdown-behavior terminate` and a user-data
`shutdown -h +<minutes>`, so a forgotten session ends itself. 180 minutes covers tests 1a, 2 and 3; test 1b is about
7.5 hours (`ec2-tests.md` §3) and needs a longer timer.

## 4. Region, Availability Zone and quotas

**An Availability Zone that offers every type used.** All hosts of a session go in one zone and one cluster placement
group, so the zone must offer the member type, the driver type and, for metal runs, c6id.metal:

```bash
for type in c6in.8xlarge c6in.4xlarge c6id.metal c7i.4xlarge; do
    echo "$type: $(aws ec2 describe-instance-type-offerings --location-type availability-zone \
        --filters Name=instance-type,Values=$type --query 'InstanceTypeOfferings[].Location' --output text)"
done
```

Pick a zone in every list and set `AZ`. A cluster placement group can fail with `InsufficientInstanceCapacity`; retry
later, or try the next zone and start the session again from §6.

**The vCPU quota.** The quota is "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances", code `L-1216C47A`,
counted in vCPUs, which are hardware threads: a c6in.8xlarge is 32 vCPUs on 16 cores. Whether it counts the vCPUs the
instance type lists or the threads left after `--cpu-options` turns hyper-threading off is not stated here; plan for
the type's full figure.

| session | vCPUs |
| --- | --- |
| smoke test: 1 × c6in.8xlarge | 32 |
| tests 1 and 3: 3 × c6in.8xlarge, 1 × c6in.4xlarge | 112 |
| plus test 2 alongside: 3 × c7i.4xlarge | 160 |
| tests 1 and 3 on metal: 3 × c6id.metal (128 each), 1 × c6in.4xlarge | 400 |

```bash
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A \
    --query Quota.Value
aws service-quotas request-service-quota-increase --service-code ec2 --quota-code L-1216C47A \
    --desired-value 160
aws service-quotas list-requested-service-quota-change-history-by-quota --service-code ec2 \
    --quota-code L-1216C47A --query 'RequestedQuotas[].[Status,DesiredValue]'
```

A new account's quota is often 5 to 32 vCPUs, and a request can take a day: make it first. The metal figure needs
its own, larger request.

Test 2's failover soak runs on a spot instance, which counts against a quota of its own, "All Standard (A, C, D, H, I, M,
R, T, Z) Spot Instance Requests", code `L-34B43A08`: 16 vCPUs for one c7i.4xlarge. Request it the same way
(`--quota-code L-34B43A08`) if that run is wanted.

## 5. Permissions the tests run under

Run the tests under a permission set (or IAM user) narrower than the administrator's. The EC2 actions are limited to
the region; the parameter, quota and budget statements carry no region condition.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Ec2InTheRegion",
      "Effect": "Allow",
      "Action": [
        "ec2:Describe*", "ec2:RunInstances", "ec2:TerminateInstances", "ec2:StartInstances", "ec2:StopInstances",
        "ec2:GetConsoleOutput", "ec2:CreateTags",
        "ec2:CreateKeyPair", "ec2:DeleteKeyPair",
        "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup", "ec2:AuthorizeSecurityGroupIngress",
        "ec2:RevokeSecurityGroupIngress", "ec2:CreatePlacementGroup", "ec2:DeletePlacementGroup",
        "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:CreateSubnet", "ec2:DeleteSubnet",
        "ec2:CreateInternetGateway", "ec2:DeleteInternetGateway", "ec2:AttachInternetGateway",
        "ec2:DetachInternetGateway", "ec2:CreateRouteTable", "ec2:DeleteRouteTable", "ec2:CreateRoute",
        "ec2:AssociateRouteTable", "ec2:DisassociateRouteTable",
        "ec2:CreateImage", "ec2:DeregisterImage", "ec2:CreateSnapshot", "ec2:DeleteSnapshot",
        "ec2:DeleteVolume"
      ],
      "Resource": "*",
      "Condition": { "StringEquals": { "aws:RequestedRegion": "us-east-1" } }
    },
    {
      "Sid": "TheImageIdOfUbuntu",
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:us-east-1::parameter/aws/service/canonical/*"
    },
    {
      "Sid": "QuotasAndBudgets",
      "Effect": "Allow",
      "Action": [
        "servicequotas:GetServiceQuota", "servicequotas:ListRequestedServiceQuotaChangeHistoryByQuota",
        "servicequotas:RequestServiceQuotaIncrease",
        "budgets:ViewBudget", "ce:GetCostAndUsage"
      ],
      "Resource": "*"
    }
  ]
}
```

In Identity Center this is a custom inline policy on a second permission set; assign it to the same user and sign in
with it for the sessions (`aws configure sso` again, as a second profile). The set has no IAM permissions and nothing
that passes a role, since no instance of the plan takes an instance profile.

If a call is refused, the error names the action; add that action rather than widening to `ec2:*`.

## 6. The network

The default VPC is enough for the smoke test, which `ec2-smoke.sh` uses. A session of the full plan uses a VPC of its
own, so that its addresses are the ones `ec2-tests.md` §5 names and deleting it ends the session's network.

```bash
VPC=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query Vpc.VpcId --output text \
      --tag-specifications "ResourceType=vpc,Tags=[{Key=project,Value=$PROJECT}]")
SUBNET=$(aws ec2 create-subnet --vpc-id "$VPC" --cidr-block 10.0.0.0/24 --availability-zone "$AZ" \
         --query Subnet.SubnetId --output text \
         --tag-specifications "ResourceType=subnet,Tags=[{Key=project,Value=$PROJECT}]")
IGW=$(aws ec2 create-internet-gateway --query InternetGateway.InternetGatewayId --output text \
      --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=project,Value=$PROJECT}]")
aws ec2 attach-internet-gateway --internet-gateway-id "$IGW" --vpc-id "$VPC"
RT=$(aws ec2 create-route-table --vpc-id "$VPC" --query RouteTable.RouteTableId --output text)
aws ec2 create-route --route-table-id "$RT" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW"
aws ec2 associate-route-table --route-table-id "$RT" --subnet-id "$SUBNET"
```

A public address costs $0.005/hr, a NAT gateway $0.045/hr and $0.045/GB, and Availability Zones apart cost $0.02/GB
(`ec2-tests.md` §2). So only the driver host takes a public address, in the one subnet of one zone, and the members
are reached over their private ones. A member then has no route to the internet: whatever it runs, the image
carries, and its clock comes from the Amazon Time Sync address, 169.254.169.123, which chrony must use (`ec2-tests.md`
§4: slewed, never stepped).

**The security group**: ssh from this machine, and everything between the group's own members, because the members,
the host and the bench talk over Aeron's UDP and TCP port block and a rule per port would only be a way to forget one.

```bash
SG=$(aws ec2 create-security-group --group-name "$PROJECT" --description "$PROJECT" --vpc-id "$VPC" \
     --query GroupId --output text \
     --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=$PROJECT}]")
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 \
    --cidr "$(curl -s https://checkip.amazonaws.com)/32"
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol=-1 --source-group "$SG"
```

Nothing else is opened to the internet. When this machine's address changes, add the new one and remove the old.

**The placement group** puts the hosts on one network spine, for the lowest latency between them:

```bash
aws ec2 create-placement-group --group-name "$PROJECT" --strategy cluster \
    --tag-specifications "ResourceType=placement-group,Tags=[{Key=project,Value=$PROJECT}]"
```

**ssh between the hosts.** The drivers run on the driver host and ssh to every member without a prompt
(`MULTIHOST_SSH` defaults to `ssh -o BatchMode=yes`). Do not copy your own private key onto the instances. Forward
the agent instead:

```bash
ssh-add ~/.ssh/$PROJECT.pem
ssh -A ubuntu@<driver public address>
```

The driver host then reaches the members, and itself, with the key your agent holds. `BatchMode` refuses a host key it
has not seen, so the first contact with each host would fail; on the driver host, accept the keys as they appear:

```bash
export MULTIHOST_SSH="ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
```

## 7. The key pair and the image

```bash
[[ ! -e ~/.ssh/$PROJECT.pem ]] || { echo "~/.ssh/$PROJECT.pem exists"; exit 1; }
(umask 077; aws ec2 create-key-pair --key-name "$PROJECT" --key-type ed25519 \
    --query KeyMaterial --output text > ~/.ssh/$PROJECT.pem)
```

The check comes first because a redirect truncates an existing file before the command can fail.

The image is built once, on a c6in.large, from the stock Ubuntu 24.04 AMI, by the steps of `ec2-tests.md` §4. When it
is built and the tuning is applied, save it:

```bash
IMAGE=$(aws ec2 create-image --instance-id <the builder> --name "$PROJECT-$(date -u +%Y%m%d)" \
        --query ImageId --output text \
        --tag-specifications "ResourceType=image,Tags=[{Key=project,Value=$PROJECT}]")
aws ec2 wait image-available --image-ids "$IMAGE"
aws ec2 terminate-instances --instance-ids <the builder>
```

An AMI keeps its snapshot, which is billed by the GB-month on the blocks in use (about $0.40–0.75 a month for this
image, at $0.05 per GB); `deregister-image` and
`delete-snapshot` remove both when the tests are finished.

## 8. Launching a session

The members take fixed private addresses, so that `MULTIHOST_MEMBERS` is known before they start (`ec2-tests.md` §5).
The members and the driver are launched into the placement group, the subnet and the security group above.

```bash
AMI=$IMAGE                 # §7
USERDATA=$(mktemp)         # ends the session on its own; 180 minutes covers tests 1a, 2 and 3
printf '#!/bin/bash\nshutdown -h +180\n' > "$USERDATA"

launch() {   # launch <name> <instance type> <private ip> [extra run-instances arguments…]
    local name="$1" type="$2" ip="$3"
    shift 3
    aws ec2 run-instances --image-id "$AMI" --instance-type "$type" --key-name "$PROJECT" \
        --subnet-id "$SUBNET" --security-group-ids "$SG" --private-ip-address "$ip" \
        --placement "GroupName=$PROJECT,AvailabilityZone=$AZ" \
        --instance-initiated-shutdown-behavior terminate --user-data "file://$USERDATA" \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=project,Value=$PROJECT}]" \
        --query 'Instances[0].InstanceId' --output text "$@"
}

# 16 physical cores a member: hyper-threading off at launch
for id in 0 1 2; do
    launch "$PROJECT-member-$id" c6in.8xlarge "10.0.0.1$((id + 1))" --cpu-options CoreCount=16,ThreadsPerCore=1
done
# the driver host, the only one with a public address
launch "$PROJECT-driver" c6in.4xlarge 10.0.0.14 --associate-public-ip-address
```

`--associate-public-ip-address` is the driver's alone; the subnet does not assign one by default. On metal, leave out
`--cpu-options` and turn hyper-threading off in the user data, as `ec2-tests.md` §4 says. Test 2's hosts are
c7i.4xlarge in the same way, and test 1b needs the timer set to the session's length.

When the instances are running, ssh to the driver as in §6 and follow `ec2-tests.md` §5.

## 9. Before the first session

Each line should succeed, or name what is missing:

```bash
aws sts get-caller-identity                                                      # the permission set of §5
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A \
    --query Quota.Value                                                          # at or above the session's vCPUs
aws budgets describe-budgets --account-id "$ACCOUNT" --query 'Budgets[].BudgetName'
aws ec2 describe-placement-groups --group-names "$PROJECT"
aws ec2 describe-security-groups --group-ids "$SG" --query 'SecurityGroups[].IpPermissions'
```

Then run `ec2-smoke.sh`: it needs only the quota for one c6in.8xlarge, and shows that the account, the permissions and
the image lookup work before the plan's instances are launched.

## 10. Ending a session, and what to check

Terminate the instances (their volumes go with them, by `DeleteOnTermination`), then delete what the session created.
The key pair, the security group, the placement group and the network are free while they exist; the image's snapshot
is not.

```bash
aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances \
    --filters Name=tag:project,Values=$PROJECT Name=instance-state-name,Values=pending,running,stopped \
    --query 'Reservations[].Instances[].InstanceId' --output text)
aws ec2 wait instance-terminated --instance-ids <the same>
```

Then, to remove the network: `delete-placement-group`, `delete-security-group`, `disassociate-route-table`,
`delete-route-table`, `detach-internet-gateway`, `delete-internet-gateway`, `delete-subnet`, `delete-vpc`, in that
order, then `delete-key-pair` and `rm ~/.ssh/$PROJECT.pem`.

Check that nothing that costs is left, after every session:

```bash
aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]' --output text
aws ec2 describe-volumes --query 'Volumes[].[VolumeId,Size,State]' --output text
aws ec2 describe-addresses --query 'Addresses[].PublicIp' --output text
aws ec2 describe-snapshots --owner-ids self --query 'Snapshots[].[SnapshotId,VolumeSize]' --output text
aws ec2 describe-images --owners self --query 'Images[].[ImageId,Name]' --output text
```

An empty result from each, apart from an image kept on purpose, means no resource is left that continues to cost. The
next day Cost Explorer, filtered on the `project` tag, shows what it cost; compare it with `ec2-tests.md` §3.

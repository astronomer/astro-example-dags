#!/bin/bash

CMD=$1
if [ "$CMD" == "" ]; then
    CMD="update-stack"
else
    CMD="create-stack"
fi

STAGE="production"
BastionSecurityGroupId=$(aws cloudformation describe-stacks --stack-name production-harper-bastion-stack --query "Stacks[0].Outputs[?OutputKey=='BastionSecurityGroup'].OutputValue" --output text)

DatalakeAdminUser=$(op item get "$STAGE-datalake" --format json --vault Environments --fields DATALAKE_ADMINUSER | jq -r '.value' || exit 1)
DatalakeAdminPass=$(op item get "$STAGE-datalake" --format json --vault Environments --fields DATALAKE_ADMINPASS | jq -r '.value' || exit 1)

# in case of previous failures run this
# aws cloudformation delete-stack --stack-name "$stage-harper-vpc-stack"

# Create the CloudFormation stack and pass the parameters
aws cloudformation $CMD \
    --stack-name "$STAGE-harper-datalake-stack" \
    --template-body file://harper-datalake-stack.yml \
    --parameters \
      ParameterKey=Stage,ParameterValue="${STAGE}" \
      ParameterKey=DatalakeAdminUser,ParameterValue="${DatalakeAdminUser}" \
      ParameterKey=DatalakeAdminPass,ParameterValue="${DatalakeAdminPass}" \
      ParameterKey=BastionSecurityGroupId,ParameterValue="${BastionSecurityGroupId}" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
    --region eu-west-1 \

#!/bin/bash

STAGE="production"

if [ "$STAGE" == "production" ]; then
  # 6 years
  LogRetentionInDays=2192
else
  LogRetentionInDays=90
fi

aws_slack_webhook=$(op item get "$STAGE-datalake" --format json --vault Environments --fields ALERTS_SLACK_WEBHOOK | jq -r '.value' || exit 1)
DatalakeAdminUser=$(op item get "$STAGE-datalake" --format json --vault Environments --fields DATALAKE_ADMINUSER | jq -r '.value' || exit 1)
DatalakeAdminPass=$(op item get "$STAGE-datalake" --format json --vault Environments --fields DATALAKE_ADMINPASS | jq -r '.value' || exit 1)

# in case of previous failures run this
# aws cloudformation delete-stack --stack-name "$stage-harper-vpc-stack"

# Create the CloudFormation stack and pass the parameters
aws cloudformation update-stack \
    --stack-name "$STAGE-harper-datalake-stack" \
    --template-body file://harper-datalake-stack.yml \
    --parameters \
      ParameterKey=Stage,ParameterValue="${STAGE}" \
      ParameterKey=LogRetentionInDays,ParameterValue="${LogRetentionInDays}" \
      ParameterKey=SlackWebhookUrl,ParameterValue="${aws_slack_webhook}" \
      ParameterKey=DatalakeAdminUser,ParameterValue="${DatalakeAdminUser}" \
      ParameterKey=DatalakeAdminPass,ParameterValue="${DatalakeAdminPass}" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
    --region eu-west-1 \

import boto3

# ------------------------------------------------------------------------
# DynamoDB

TableName="ReportGen"
client = boto3.client('dynamodb')
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TableName)

from langchain_community.chat_message_histories import DynamoDBChatMessageHistory

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName="ReportGen",
    KeySchema=[{"AttributeName": "SessionId", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "SessionId", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
)

# Wait until the table exists.
table.meta.client.get_waiter("table_exists").wait(TableName="ReportGen")


### Clean-up: delete table
#response = client.delete_table(
#    TableName='ReportGen'
#)
#print(response)
import boto3

# client instance
client = boto3.client('s3')

##retrieve all bucket Metadata
response = client.list_buckets()

for b in response['Buckets']:
    print(b['Name'])


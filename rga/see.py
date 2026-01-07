import boto3
from botocore.exceptions import ClientError
import json
import os
 
def get_secret():
    print(os.getenv("Env"))
    print(os.getenv("secret_name"))
    secret_name = os.getenv("secret_name")
    #secret_name = "aig-azcdi-us-ops-report-ds-secret-dev"
    region_name = boto3.session.Session().region_name
 
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
 
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
 
    secret = json.loads(get_secret_value_response['SecretString'])
    print(secret_name)
    print (secret['INDEX_NAME'])
    
def get_secret2(Varname):
    print(os.getenv("Env"))
    print(os.getenv("secret_name"))
    secret_name = os.getenv("secret_name")
    #secret_name = "aig-azcdi-us-ops-report-ds-secret-dev"
    region_name = boto3.session.Session().region_name
 
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
 
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
 
    secret = json.loads(get_secret_value_response['SecretString'])
   # print(secret_name)
    return secret[Varname]

if __name__ == '__main__':
	print(get_secret2('INDEX_NAME'))

import json
import boto3
import re
from datetime import datetime
import pdfplumber
#import fitz
from utils_aws import *
from const_aws import *
from dotenv import load_dotenv
load_dotenv()

from langchain_community.vectorstores import OpenSearchVectorSearch

opensearch_domain_endpoint = os.environ['OPENSEARCH_ENDPOINT']
opensearch_index = os.environ['OPENSEARCH_INDEX']

def Ingestion_Json(event, context):
    # TODO implement
    #s3 = boto3.client('s3')
    Ingest_PQR(bucket_name, upload_folder)
    return 'Hello from Lambda'

event = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }

Ingestion_Json(event, "hhhh")
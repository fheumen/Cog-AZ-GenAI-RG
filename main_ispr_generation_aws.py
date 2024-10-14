from datetime import datetime
import pdfplumber

# import fitz
from utils_aws import *
from const_aws import *
from dotenv import load_dotenv

load_dotenv()

from langchain_community.vectorstores import OpenSearchVectorSearch

opensearch_domain_endpoint = os.environ["OPENSEARCH_ENDPOINT"]
opensearch_index = os.environ["OPENSEARCH_INDEX"]

def ISPR_Generation(event, context):
    # s3 = boto3.client('s3')
    ispr_generation(bucket_name)
    return "Hello from Lambda"

event = {"key1": "value1", "key2": "value2", "key3": "value3"}

ISPR_Generation(event, "hhhh")
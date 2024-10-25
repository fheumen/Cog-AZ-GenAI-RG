from typing import Set, List, Dict, Any
import os

from utils_aws import *
from const_aws import *

# from backend.core import *
import streamlit as st
from streamlit_chat import message


# from streamlit_demo.tracing.langfuse import langfuse

###############################

from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chat_models import ChatOpenAI

# from langchain.chains import RetrievalQA
from app_streamlit.chains.retrieval import StreamingConversationalRetrievalChain
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory


from langchain.vectorstores import Pinecone
from langchain.memory import ConversationBufferMemory

from dotenv import load_dotenv

load_dotenv()

from chat_app_aws import ReportGeneration
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch
from requests_aws4auth import AWS4Auth


# Get Langchain handler for a trace
from app_streamlit.tracing.langfuse import *

print("xxxxxxxxxxxxxxxxxxxxx")
print(trace_id)

import os
import boto3

### instantiate client
client = boto3.client("s3")

# set variables
bucket = "s3-az-reportgen-bucket"

# @observe()
service = "aoss"
credentials = boto3.Session().get_credentials()
region = boto3.Session().region_name
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token,
)

opensearch_domain_endpoint = os.environ["OPENSEARCH_ENDPOINT"]
opensearch_index = os.environ["OPENSEARCH_INDEX"]
embedding = BedrockEmbeddings(model_id=os.environ["aws_embedding_model"])

chat = ReportGeneration(embedding, opensearch_domain_endpoint, opensearch_index)
product_name = "Fasenra"
reporting_period = "14Nov2022_13Nov2023"
user_question = "What where the batch numbers manufactured at the site fmc?"
question_with_context = (
    user_question
    + f". Given that the product name is {product_name} and the reporting period is {reporting_period}"
)
print(chat.retriever.invoke(question_with_context))

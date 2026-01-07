# Standard Library Imports
import base64
import csv
import html
import io
import json
import os
import random
import re
import string
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional, List, Dict


# Third-Party Library Imports
import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from dateutil import parser
import pandas as pd
import pdfplumber
import PyPDF2
from PyPDF2 import PdfReader
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response, UploadFile, status, File, Form
from fastapi.responses import JSONResponse
from loguru import logger
from PIL import Image
from bs4 import BeautifulSoup
import mammoth
import html2docx

# Document Handling - docx
from docx import Document as Document_docx
from docx.image.exceptions import UnrecognizedImageError
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.run import Run
from docx.shared import Inches
from docx.enum.text import WD_COLOR_INDEX

# LangChain & Related
from langchain_aws import ChatBedrock
from langchain.chains.summarize import load_summarize_chain
from langchain.prompts import PromptTemplate
from langchain.docstore.document import Document
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage


# OpenSearch & AWS Auth
from opensearchpy import (
    RequestsHttpConnection,
    OpenSearch,
    OpenSearchException,
    NotFoundError,
    RequestError,
)

from requests_aws4auth import AWS4Auth

# Application-specific Imports
from data import (
    IngestResult, IsprTrackingSection, IsprTrackingSubSection, IsprTrackingCompletion, QueryRequest,
    QnaAnswer, AnswerRequest, User, Query, RequestQuery, Citation, QuickReply, Result, QueryResponse,
    FeedbackDisplayOptions, Feedback, ChatInteraction, ChatMetadata, ChatHistorySearchRequest, 
    FeedbackRequest, PqrTrackingStatus, ReportTrackingSection, ReportTrackingCompletion, 
    ReportTrackingWelcomePageAllProduct, WelcomeFilter, ReportSelectForEditionOutput, ReportQueue, 
    ReportRequestCreation, ReportFile
)

from knowbase_utils import (
    get_knowledge_base_id, create_data_source, sync_data_source, 
    check_sync_status, retrieve_and_generate, get_data_source_id,  delete_data_source
)

from imgproc_utils import (
    extract_images_and_upload, normalize_text, _insert_image, 
    insert_image_after_paragraph, sitev_insert_figure_in_report
)

from llm_imgproc_utils import (
    llm_extract_images_and_upload
)

from tabproc_utils import (
    normalize_text, _insert_table, 
    insert_table_after_paragraph, sitev_insert_table_in_report
)

from report_table_mapper import populate_tables
from report_edition.utils import store_reporttrackingcompletion
from config import *

load_dotenv()

# -------------------------
# Constants
# -------------------------
HIGHLIGHT_COLOR = "green"
PLACEHOLDER_TYPE_TEXT = "text"
PLACEHOLDER_TYPE_FIG = "figure"
PLACEHOLDER_TYPE_TAB = "table"

COLUMN_PLACEHOLDER_TYPE = "Place_Holder_Typ"
COLUMN_SOURCE_TYPE = "Source_pdf_typ"
COLUMN_PLACEHOLDER = "Place_holder"
COLUMN_RULES = "Rules"
ENCODING_UTF8 = 'utf-8'
S3_LIST_KEY = "Contents"
S3_FOLDER_SUFFIX = "/"
SOURCE_TYPE = "source_type"

COLUMN_SECTION_NAMES_ISPR = "ISPR Structur Section names"
COLUMN_SECTION_NAMES_KEYSEARCH = "Section names key search"
COLUMN_ISPR_SUMMARY_FLAG = "Integration type in ISPR Flag"
COLUMN_MAP_PROMPT_TEMPLATE = "Map Prompt Template"
COLUMN_COMBINE_PROMPT_TEMPLATE = "Combine Prompt Template"
KEYSEARCH_DELIMITER = ";"  # Delimiter used in keysearch column

CHAIN_TYPE = "map_reduce"
INPUT_VARS = ["text"]
DEFAULT_VERSION = "v0.0"
DATE_FORMAT = "%d%b%Y"
HEADER_FLAG_INIT = 0
FOOTER_FLAG_INIT = 0

FIRST_PAGE_INDEX = 0
TMP_TO_FINAL_LOG_TEMPLATE = (
    "ingest_phase_1 --- 5.{index} {label}: {value}"
)

# Default feedback options: all disabled
DEFAULT_FEEDBACK_OPTIONS = FeedbackDisplayOptions(thumbsUp="N", thumbsDown="N", feedbackText="N")

# Constants
INDEX_PRODUCT_USER_TIMESTAMP = "ProductName_User-Timestamp-index"
SUCCESS_MESSAGE = "ISPR-Edition interaction stored successfully"

# OpenSearch Metadata Keys
METADATA_PRODUCT_NAME_KEY = "metadata.product_name.keyword"
METADATA_REPORTING_PERIOD_KEY = "metadata.reporting_period.keyword"
METADATA_SITE_NAME_KEY = "metadata.site_name.keyword"

# Logging
LOG_INSERT_SUCCESS = "opensearch_insert_docs: Successfully added documents to OpenSearch vectorstore."
LOG_INSERT_COUNT = "opensearch_insert_docs: Preparing to insert {} documents into OpenSearch vectorstore."
LOG_OPENSEARCH_ERROR = "opensearch_insert_docs: OpenSearch error occurred: {}"
LOG_UNEXPECTED_ERROR = "opensearch_insert_docs: An unexpected error occurred: {}"

# File Handling
SUPPORTED_FILE_EXTENSION = ".pdf"
SUPPORTED_FILE_TYPES = ['.pdf', '.docx']
IMAGE_TABLE_WIDTH = 1024
IMAGE_TABLE_HEIGHT = 1500

SOURCE_TYPE_TABLE_NAME = "Table in Report"

# Encoding & JSON
ENCODING_FORMAT = "utf-8"
JSON_INDENTATION = 4

# Date Patterns and Formats
DATE_PATTERNS = [
    r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b',
    r'\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',
    r'\b(?:\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{4})\b',
    r'\b(?:\d{1,2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\d{4})\b',
    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s(?:\d{1,2},\s\d{4})\b',
    r'\b(?:\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})\b',
    r'\b(?:\w+\s\d{1,2},\s\d{4})\b',
    r'\b(?:\d{2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{2})\b',
    r'\b(?:\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2})\b'
]

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m/%d/%Y",
    "%d %B %Y",
    "%d %b %Y",
    "%d%b%Y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%d%b%y",
    "%Y-%m-%dT%H:%M:%S"
]

# Versioning and Identifiers
DEFAULT_FOLDER_NAME = "temp/"
# DEFAULT_VERSION = "v0.0"
VERSION_PATTERNS = [r"\sv\d+\.\d+", r"\d+\.\d+"]
VERSION_REGEX_PATTERN = r"v(\d+)\.(\d+)"

# DynamoDB-related
DEFAULT_RETURN_VALUES = "UPDATED_NEW"
INDEX_NAME = "ProductName-index"
ISPR_EDITOR_STATUS_FIELD = "Ispr_Editor_Status"
UPDATED_AT_FIELD = "updated_at"
DATE_OF_EDITION_FIELD = "DateofEdition"
LOCKED_FIELD = "locked"
ISPR_KEYWORD = "ISPR"

# Miscellaneous
TIMESTAMP_MULTIPLIER = 1000  # To convert seconds to milliseconds

SUMMARY_FLAG = 1
CHUNK_REPORT_FOLDER = "chunk_report_gen"

# Constants
DEFAULT_DPI = 96  # Default DPI for Word documents
BORDER_SIZE = "6"
BORDER_TYPE = "single"
BORDER_SPACE = "0"
BORDER_COLOR = "000000"
BORDER_NAMES = ["top", "left", "bottom", "right", "insideH", "insideV"]

s3 = boto3.client("s3")
bucket_name = f"{BUCKET_NAME}"

ERROR_MESSAGE = "Oops! It seems there’s a network issue. Please check your connection and try again in a moment."

bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")
    

def convert_date_ranges(date_ranges):
    """
    Converts a list of string date ranges to a list of string year ranges.

    Args:
        date_ranges (list): A list of strings representing date ranges in the format 'DDMonYYYY_DDMonYYYY'.

    Returns:
        list: A list of strings representing the year ranges extracted from the input date ranges.

    Example:
        >>> input_list = ['01Oct2022_30Jun2023', '30Jul2022_29Jul2023', '30Jul2022_29Jul2023']
        >>> convert_date_ranges(input_list)
        ['2022_2023', '2022_2023', '2022_2023']
    """
    logger.info(f"{convert_date_ranges.__name__}: Starting date range conversion.")

    output = []
    pattern = r"(\d{2}\w{3})(\d{4})_(\d{2}\w{3})(\d{4})"  # Regex pattern to extract years from date range

    for date_range in date_ranges:
        date_range = date_range.strip()  # Remove leading/trailing whitespace
        logger.info(f"{convert_date_ranges.__name__}: Processing '{date_range}'.")

        match = re.match(pattern, date_range)
        if match:
            start_year = match.group(2)
            end_year = match.group(4)
            year_range = f"{start_year}_{end_year}"
            output.append(year_range)
            logger.info(f"{convert_date_ranges.__name__}: Extracted year range '{year_range}'.")
        else:
            logger.info(f"{convert_date_ranges.__name__}: No match found for '{date_range}'.")

    logger.info(f"{convert_date_ranges.__name__}: Completed processing. Output: {output}")
    return output

# def generate_technical_error_message(msg_id, transaction_count , user_query, sessionId):  
        
#         feedbackoptions = FeedbackDisplayOptions(thumbsUp="N", thumbsDown="N", feedbackText="N")
#         feedback = Feedback(feedbackDisplayOptions=feedbackoptions)
        
#         result = Result(
#             messageId=str(msg_id),
#             answer=ERROR_MESSAGE,
#             transactionCount=transaction_count,
#             #citations=citations,
#             feedback=feedback
#         )        
#         queryResponse = QueryResponse(
#             status="error",
#             sessionId=sessionId,
#             userQuery=user_query,
#             result=result
#         )        
#         return queryResponse


def generate_technical_error_message(msg_id, transaction_count, user_query, session_id):
    """
    Generates a standardized error response message when a technical error occurs.

    Args:
        msg_id (str or int): Unique identifier for the message.
        transaction_count (int): Number of transactions processed in the session.
        user_query (str): The original user query.
        session_id (str): Unique session identifier.

    Returns:
        QueryResponse: An object containing the structured error response including feedback and status.
    """
    logger.info(f"{generate_technical_error_message.__name__}: Generating technical error message.")

    # Use default feedback display options
    feedback = Feedback(feedbackDisplayOptions=DEFAULT_FEEDBACK_OPTIONS)
    logger.info(f"{generate_technical_error_message.__name__}: Feedback object created.")

    result = Result(
        messageId=str(msg_id),
        answer=ERROR_MESSAGE,
        transactionCount=transaction_count,
        feedback=feedback
    )
    logger.info(f"{generate_technical_error_message.__name__}: Result object created.")

    query_response = QueryResponse(
        status="error",
        sessionId=session_id,
        userQuery=user_query,
        result=result
    )
    logger.info(f"{generate_technical_error_message.__name__}: QueryResponse created and returned.")

    return query_response


# def store_isprtrackingcompletion(interaction: IsprTrackingCompletion, tablename):
#     """
#     Store the IsprTrackingCompletion interaction in the ISPR_DYNAMOTABLE DynamoDB table.

#     This function performs the following steps:
#     1. Delete any existing records with the same ProductName_User combination.
#     2. Insert the new IsprTrackingCompletion interaction into the table.
#     3. Invoked from update ispr and Generate ispr APIs.
    
#     **Steps Carried Out by the Function:**

#     1. Delete any existing records with the same `ProductName_User` combination from the DynamoDB table.
#        - Query the table using the `ProductName_User-Timestamp-index` to retrieve existing records with the same `ProductName_User`.
#        - Delete each retrieved record using the correct partition key (`SessionID`) and sort key (`Timestamp`).
#     2. Insert the new `IsprTrackingCompletion` interaction into the DynamoDB table.
#        - Convert the `IsprTrackingCompletion` object to a dictionary using the `dict()` method.
#        - Insert the dictionary as a new item in the DynamoDB table using the `put_item()` method.

#     **External Function Calls:**

#     - `boto3.resource('dynamodb')`: Used to create a DynamoDB resource.
#     - `table.query()`: Used to query the DynamoDB table with a specific index and condition expression.
#     - `table.delete_item()`: Used to delete an item from the DynamoDB table based on the provided primary key.
#     - `table.put_item()`: Used to insert a new item into the DynamoDB table.

#     Args:
#         interaction (IsprTrackingCompletion): The IsprTrackingCompletion interaction to be stored.
#         tablename (str): The name of the DynamoDB table.

#     Returns:
#         dict: A success message if the interaction is stored successfully.

#     Raises:
#         HTTPException: If an exception occurs during the storage process.

#     External Function Calls:
#         - boto3.resource('dynamodb')
#         - table.query()
#         - table.delete_item()
#         - table.put_item()
#     """
#     logger.info("\nstore_isprtrackingcompletion ---------- START --------")
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(tablename)
#     ############## First delete all previous record from the same ProductName&UserID
#     response = table.query(
#                 IndexName = "ProductName_User-Timestamp-index",
#                 KeyConditionExpression=Key('ProductName_User').eq(interaction.ProductName_User),
#               )
#     # for item in response.get('Items', []):
#     #     table.delete_item(Key={"ProductName_User": item["ProductName_User"]})
#     # Step 2: Delete existing records using correct keys
#     for item in response.get('Items', []):
#         table.delete_item(Key={
#             "SessionID": item["SessionID"],  # Correct partition key
#             "Timestamp": item["Timestamp"]   # Correct sort key
#         })
#     print("store_isprtrackingcompletion ---------- Deleted --------")
#     #################### Insert New Records
#     try:
#         item = interaction.dict()
#         ############ delete all previous records about the interaction.ProductName_User
#         table.put_item(Item=item)
#         print("store_isprtrackingcompletion ---------- END --------ISPR-Edition interaction stored successfully")
#         return {"message": "ISPR-Edition interaction stored successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

def store_isprtrackingcompletion(interaction: IsprTrackingCompletion, table_name: str):
    """
    Store the IsprTrackingCompletion interaction in the specified DynamoDB table.

    Steps:
    1. Delete existing records with the same ProductName_User value using the index.
    2. Insert the new interaction record into the table.

    Args:
        interaction (IsprTrackingCompletion): The interaction object to be stored.
        table_name (str): Name of the DynamoDB table.

    Returns:
        dict: Success message if operation is successful.

    Raises:
        HTTPException: If any exception occurs during the process.
    """
    logger.info("store_isprtrackingcompletion: START")

    # Initialize DynamoDB table resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        logger.info("store_isprtrackingcompletion: Querying for existing records")
        # Step 1: Query for existing records with the same ProductName_User
        response = table.query(
            IndexName=INDEX_PRODUCT_USER_TIMESTAMP,
            KeyConditionExpression=Key('ProductName_User').eq(interaction.ProductName_User),
        )

        logger.info(f"store_isprtrackingcompletion: Found {len(response.get('Items', []))} existing records to delete")

        # Step 2: Delete all matched records using correct primary and sort keys
        for item in response.get('Items', []):
            table.delete_item(Key={
                "SessionID": item["SessionID"],
                "Timestamp": item["Timestamp"]
            })

        logger.info("store_isprtrackingcompletion: Existing records deleted")

        # Step 3: Insert the new interaction record
        item = interaction.dict()
        table.put_item(Item=item)

        logger.info("store_isprtrackingcompletion: New interaction stored successfully")
        return {"message": SUCCESS_MESSAGE}

    except Exception as e:
        logger.info(f"store_isprtrackingcompletion: Exception occurred - {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


########### Retrieve the Context
# def retrievecontext(bucket_name, output_folder, pqr_param_json_filename):
#     # Download the file
#     response = s3.get_object(Bucket=bucket_name, Key=f"{output_folder}{pqr_param_json_filename}")
#     content = response['Body'].read().decode('utf-8')
#     # Parse the JSON content
#     data = json.loads(content)
#     # Extract specific fields into variables
#     product_name = data.get('product_name')
#     reporting_period = data.get('reporting_period')
#     site_names = data.get('site_names')
#     pqr_file_names = data.get('pqr_file_names')
#     pqr_versions = data.get('pqr_versions')
#     return product_name, reporting_period, site_names, pqr_file_names, pqr_versions


def retrieve_context(bucket_name: str, output_folder: str, pqr_param_json_filename: str):
    """
    Retrieves and parses context information from a JSON file stored in an S3 bucket.

    This function performs the following steps:
    1. Downloads the JSON file from the specified S3 bucket and folder.
    2. Decodes the content using UTF-8 encoding.
    3. Parses the JSON data and extracts relevant fields.

    Args:
        bucket_name (str): Name of the S3 bucket.
        output_folder (str): Folder path inside the S3 bucket where the file is located.
        pqr_param_json_filename (str): Name of the JSON file to retrieve.

    Returns:
        tuple: A tuple containing the following extracted fields:
            - product_name (str)
            - reporting_period (str)
            - site_names (list)
            - pqr_file_names (list)
            - pqr_versions (list)
    """
    logger.info("retrieve_context: Downloading file from S3")

    # Step 1: Download the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=f"{output_folder}{pqr_param_json_filename}")
    content = response['Body'].read().decode(ENCODING_UTF8)

    logger.info("retrieve_context: File downloaded and decoded")

    # Step 2: Parse the JSON content
    data = json.loads(content)

    logger.info("retrieve_context: JSON content parsed")

    # Step 3: Extract relevant fields
    product_name = data.get('product_name')
    reporting_period = data.get('reporting_period')
    site_names = data.get('site_names')
    pqr_file_names = data.get('pqr_file_names')
    pqr_versions = data.get('pqr_versions')

    logger.info("retrieve_context: Fields extracted successfully")

    return product_name, reporting_period, site_names, pqr_file_names, pqr_versions


#### Control the size of token
# def num_tokens_from_string(string: str, encoding_name: str) -> int:
#     #encoding = tiktoken.encoding_for_model(encoding_name)
#     encoding = tiktoken.get_encoding(encoding_name)
#     num_tokens = len(encoding.encode(string))
#     return num_tokens

### Get the list of files into a S3 directory
# def get_list_of_files(bucket_name, folder_prefix):
#     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)["Contents"]
#     return response

def get_list_of_files(bucket_name: str, folder_prefix: str):
    """
    Retrieves a list of files from an S3 bucket under a specified folder prefix.

    This function uses the AWS S3 client to list objects in a given bucket and folder path.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_prefix (str): Folder path (prefix) to list the files under.

    Returns:
        list: A list of objects (dictionaries) representing the files found under the prefix.
              If no files are found, it returns an empty list.

    Raises:
        KeyError: If the 'Contents' key is not present in the response.
    """
    logger.info("get_list_of_files: Listing objects from S3 bucket")

    # Fetch objects from S3 using list_objects_v2 with the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    logger.info("get_list_of_files: Objects listed successfully")

    # Extract the 'Contents' key from the response
    file_list = response.get(S3_LIST_KEY, [])

    logger.info(f"get_list_of_files: Retrieved {len(file_list)} files")

    return file_list


##### Create a Folder into a Bucket
# def check_create_dir(bucket_name, path):
#     # Check if the subfolder exists by listing objects with the given prefix
#     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
#     if "Contents" not in response:
#         s3.put_object(Bucket=bucket_name, Key=(path + "/"))

def check_create_dir(bucket_name: str, path: str):
    """
    Checks if a directory (prefix) exists in an S3 bucket, and creates it if it does not exist.

    This function lists objects with the specified prefix in the given bucket. 
    If the prefix does not exist (i.e., no contents), it creates an empty object with that prefix,
    effectively simulating a folder.

    Args:
        bucket_name (str): The name of the S3 bucket.
        path (str): The folder path (prefix) to check or create.

    Returns:
        None
    """
    logger.info("check_create_dir: Checking if directory exists in S3")

    # List objects using the given prefix to check for folder existence
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)

    # If the folder doesn't exist, create an empty object with that prefix
    if S3_LIST_KEY not in response:
        logger.info("check_create_dir: Directory not found, creating folder")
        s3.put_object(Bucket=bucket_name, Key=(path + S3_FOLDER_SUFFIX))
    else:
        logger.info("check_create_dir: Directory already exists")


# def extract_prefixes(strings):
#     """
#     Extracts the prefix before the first underscore in each string from the input list.

#     Args:
#         strings (list of str): A list of strings containing a prefix followed by an underscore.

#     Returns:
#         list of str: A list containing the extracted prefixes before the first underscore.
        
#     Example:
#         >>> extract_prefixes([
#         ...     'CSD_(PRO-0184377) - Document.pdf',
#         ...     'MAIN_(PRO-0187549) - Another Document.pdf'
#         ... ])
#         ['CSD', 'MAIN']
#     """
#     prefixes = []
#     for s in strings:
#         if "_" in s:
#             prefix = s.split("_", 1)[0]
#             prefixes.append(prefix)
#         else:
#             prefixes.append("")
#     return prefixes


###################### Site Validation ###################### 

# -------------------------
# Helper Functions
# -------------------------

# def extract_prefixes(source_file_names):
#     """
#     Extracts prefixes from a list of source file names.
    
#     This function assumes each file name uses an underscore to separate the prefix
#     from the rest of the name (e.g., "invoice_001.pdf" → "invoice").

#     Args:
#         source_file_names (list): List of source file names as strings.

#     Returns:
#         list: List of extracted prefixes.
#     """
#     if not isinstance(source_file_names, list):
#         raise ValueError("source_file_names must be a list.")
#     return [str(name).split('_')[0] for name in source_file_names]

# Constants (none reused in this small function)

def extract_prefixes(source_file_names: list) -> list:
    """
    Extract prefixes from a list of source file names.

    Each file name is expected to contain an underscore '_' separating the prefix
    from the rest of the name. For example, "invoice_001.pdf" yields the prefix "invoice".

    Args:
        source_file_names (list): List of source file names as strings.

    Returns:
        list: List of extracted prefixes as strings.

    Raises:
        ValueError: If input is not a list.
    """
    logger.info("extract_prefixes: Starting prefix extraction")

    if not isinstance(source_file_names, list):
        logger.info("extract_prefixes: Invalid input type detected")
        raise ValueError("source_file_names must be a list.")

    prefixes = [str(name).split('_')[0] for name in source_file_names]

    logger.info(f"extract_prefixes: Extracted {len(prefixes)} prefixes")

    return prefixes


def load_excel_from_s3(bucket_name, file_path, sheet_name):
    """
    Loads an Excel sheet from an S3 bucket into a pandas DataFrame.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_path (str): Key/path of the Excel file in the bucket.
        sheet_name (str): Name of the sheet to load.

    Returns:
        pd.DataFrame: Loaded data as a DataFrame.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        excel_data = response['Body'].read()
        return pd.read_excel(io.BytesIO(excel_data), sheet_name=sheet_name)
    except Exception as e:
        logger.error(f"Failed to load Excel from S3: {e}")
        raise


def extract_unique_source_types(df):
    """
    Extracts unique source types for all placeholders from the mapping DataFrame.

    Args:
        df (pd.DataFrame): The mapping DataFrame.

    Returns:
        list: Unique list of cleaned source types.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame.")
    if COLUMN_PLACEHOLDER_TYPE not in df.columns or COLUMN_SOURCE_TYPE not in df.columns:
        raise KeyError("Required columns missing in the DataFrame.")
    return list({
        item.strip()
        # for item in df[df[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TEXT][COLUMN_SOURCE_TYPE]
        for item in df[COLUMN_SOURCE_TYPE]
        if isinstance(item, str)
    })


def filter_mapping_data(df, source_type_list):
    """
    Filters the mapping DataFrame based on provided source types.

    Args:
        df (pd.DataFrame): The full mapping DataFrame.
        source_type_list (list): List of allowed source types to include.

    Returns:
        pd.DataFrame: Filtered DataFrame based on matching source types.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame.")
    if COLUMN_SOURCE_TYPE not in df.columns:
        raise KeyError(f"{COLUMN_SOURCE_TYPE} column not found in DataFrame.")
    if not isinstance(source_type_list, list):
        raise ValueError("source_type_list must be a list.")
    return df[df[COLUMN_SOURCE_TYPE].str.strip().isin(source_type_list)]

# def filter_mapping_data(df: pd.DataFrame, source_type_list: list) -> pd.DataFrame:
#     """
#     Filters the mapping DataFrame based on provided source types.

#     This function validates the inputs and filters rows where the 'source_type' column
#     matches any of the types provided in the source_type_list. Leading/trailing spaces
#     in the 'source_type' column are stripped before filtering.

#     Args:
#         df (pd.DataFrame): The full mapping DataFrame.
#         source_type_list (list): List of allowed source types to include.

#     Returns:
#         pd.DataFrame: Filtered DataFrame containing only rows with matching source types.

#     Raises:
#         ValueError: If df is not a DataFrame or source_type_list is not a list.
#         KeyError: If the expected source_type column is not in the DataFrame.
#     """
#     logger.info("filter_mapping_data: Starting to filter DataFrame by source types")

#     if not isinstance(df, pd.DataFrame):
#         logger.info("filter_mapping_data: Input df is not a pandas DataFrame")
#         raise ValueError("df must be a pandas DataFrame.")

#     if SOURCE_TYPE not in df.columns:
#         logger.info(f"filter_mapping_data: Column '{SOURCE_TYPE}' not found in DataFrame")
#         raise KeyError(f"{SOURCE_TYPE} column not found in DataFrame.")

#     if not isinstance(source_type_list, list):
#         logger.info("filter_mapping_data: source_type_list is not a list")
#         raise ValueError("source_type_list must be a list.")

#     filtered_df = df[df[SOURCE_TYPE].str.strip().isin(source_type_list)]

#     logger.info(f"filter_mapping_data: Filtering complete, {len(filtered_df)} rows retained")

#     return filtered_df


def generate_outputs(rules, kb_name, model_id, kbname_max_size, report_id):
    """
    Applies a generation function to each rule using context from the report ID and knowledge base.

    Args:
        rules (list): List of text generation rules.
        kb_name (str): Knowledge base name.
        model_id (str): Model ID to use.
        kbname_max_size (int): Max size for KB name.
        report_id (str): Contextual report identifier.

    Returns:
        list: List of generated text outputs for each rule.
    """
    if not isinstance(rules, list):
        raise ValueError("rules must be a list.")
    outputs = []
    for rule in rules:
        try:
            result = retrieve_and_generate(
                f"based on the report id '{report_id}'," + str(rule),
                kb_name,
                model_id,
                kbname_max_size
            )
            outputs.append(result)
        except Exception as e:
            logger.error(f"Error generating output for rule '{rule}': {e}")
            outputs.append(None)
    return outputs


# -------------------------
# Main Function
# -------------------------

def get_document_fulltyp(short_typ_name, table_template_master, template_fullname):
    """
    Determines the document type from source file types using the template mapping.

    Args:
        document_name (str): The name of the document file.
        source_file_names (list): List of source file names.
        source_file_types (list): Corresponding list of file types.
        table_template_master (str): DynamoDB table name for templates.
        template_fullname (str): Full template name.

    Returns:
        str: Document type name (mapped or inferred).
    """
    logger.info("get_document_fulltyp: Extracting template and resolving document type")
    try:
        template_id, docx_s3_key, excel_s3_key, section_names, template_name, doc_types, doc_types_full = sitev_extract_template(
            table_template_master, template_fullname
        )
    
        # Map the document type from source file type
        full_file_type = doc_types_full[doc_types.index(short_typ_name)]
        return full_file_type

    except Exception as e:
        logger.error(f"get_document_fulltypp: Fallback to inferred document type due to error: {str(e)}")


def safe_float(val):
    """
    Safely converts a value to a float.

    Attempts to convert the input `val` to a float. If the value is None, an empty string,
    or cannot be converted due to a type or value error, returns None instead of raising an exception.

    Args:
        val: The value to be converted to float. Can be of any type.

    Returns:
        float or None: The converted float value if successful, otherwise None.
    """
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def get_mapping_list_sv(bucket_name, excel_file_path, az_mapping_sheet_name, kb_name, model_id, kbname_max_size, report_id, source_file_names, source_file_types, upload_folder, table_template_master, template_fullname):
    """
    Extracts placeholder-to-value mappings from an Excel file stored in an S3 bucket and generates
    textual outputs based on rules defined in the Excel sheet.

    Args:
        bucket_name (str): The name of the S3 bucket where the Excel file is stored.
        excel_file_path (str): The key/path of the Excel file in the S3 bucket.
        az_mapping_sheet_name (str): The name of the sheet in the Excel file containing the mappings.
        kb_name (str): The name of the knowledge base used for generating outputs.
        model_id (str): The ID of the model used for generating responses.
        kbname_max_size (int): Maximum allowed size of the knowledge base name for the model input.
        report_id (str): The identifier of the report, used to provide context for generation.
        source_file_names (list): List of source file names uploaded to the system.

    Returns:
        dict: A dictionary mapping placeholder names to their corresponding generated text outputs.
    """
    try:
        # Step 1: Extract source file prefixes
        source_file_names_pref_list = extract_prefixes(source_file_names)
        logger.info(f"Source File Names: {source_file_names}")
        logger.info(f"Source File Prefixes: {source_file_names_pref_list}")

        # Step 2: Load the Excel sheet from S3
        df_mapping = load_excel_from_s3(bucket_name, excel_file_path, az_mapping_sheet_name)
        
        #Additions
        # Ensure coordinate columns are present; if missing, add with NaN
        for col in ["x0", "y0", "x1", "y1"]:
            if col not in df_mapping.columns:
                df_mapping[col] = None


        # Step 3: Identify source types that have text placeholders
        source_pdf_type_list = extract_unique_source_types(df_mapping)
        logger.info(f"Source PDF Types: {source_pdf_type_list}")

        # Step 4: Filter mappings to only those relevant source types
        df_filtered = filter_mapping_data(df_mapping, source_pdf_type_list)
        logger.info(f"Original Mapping Shape: {df_mapping.shape}")
        logger.info(f"Filtered Mapping Shape: {df_filtered.shape}")

        # Step 5: Extract placeholders and corresponding rules
        if COLUMN_PLACEHOLDER_TYPE not in df_filtered.columns or COLUMN_PLACEHOLDER not in df_filtered.columns or COLUMN_RULES not in df_filtered.columns:
            raise KeyError("One or more required columns missing in filtered DataFrame.")
        placeholders_text = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TEXT][COLUMN_PLACEHOLDER].tolist()
        rules_text = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TEXT][COLUMN_RULES].tolist()
        
        placeholders_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_FIG][COLUMN_PLACEHOLDER].tolist()
        sourcepdftyp_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE]== PLACEHOLDER_TYPE_FIG][COLUMN_SOURCE_TYPE].tolist()
        rules_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_FIG][COLUMN_RULES].tolist()
        
        placeholders_tab = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TAB][COLUMN_PLACEHOLDER].tolist()
        rules_tab = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TAB][COLUMN_RULES].tolist()
        
        # Step 6: Generate outputs for each rule
        
        # Step 6.1: Generate outputs for text rule
        generated_outputs = generate_outputs(rules_text, kb_name, model_id, kbname_max_size, report_id)
        logger.info(f"Generated Outputs: {generated_outputs}")
        
        # Step 6.2: Generate outputs for figure rule
        ################ placeholders_fig := ["<Figure 1:>", "<Figure 2:>"],  rules_fig = ["Figure 2", "Figure 3, Figure 4, Figure 5"]
        rules_dict = {}
        result_dict_fig = {}
        rules_dict_tab = {}
        
        if len(placeholders_tab) > 0:
            for placeholder, rule in zip(placeholders_tab, rules_tab):
                # Split by comma and strip spaces
                sheet_name = rule.strip() 
                rules_dict_tab[placeholder] = sheet_name
        
        if len(placeholders_fig) > 0:
            for placeholder, rule in zip(placeholders_fig, rules_fig):
                # Split by comma and strip spaces
                split_rules = [part.strip() for part in rule.split(",")]
                rules_dict[placeholder] = split_rules

            main_s3_key = []   
            for i in range(len(sourcepdftyp_fig)):
                full_doc_type = get_document_fulltyp(sourcepdftyp_fig[i].strip(), table_template_master, template_fullname)
                main_document_name = source_file_names[source_file_types.index(full_doc_type)]
    
                main_s3_key.append(f"{upload_folder}{main_document_name}")
                logger.info(f"File Location to extracted images : {main_s3_key[i]}")
                # logger.info(f"Rules to extracted images : {rules_dict}")

            # Build coordinates_dict for placeholders with custom coordinates
            # coordinates_dict = {}
            # for placeholder in placeholders_fig:
            #     row = df_filtered[df_filtered[COLUMN_PLACEHOLDER] == placeholder]
            #     if not row.empty:
            #         coords = {
            #             "x0": row.iloc[0].get("x0"),
            #             "y0": row.iloc[0].get("y0"),
            #             "x1": row.iloc[0].get("x1"),
            #             "y1": row.iloc[0].get("y1")
            #         }
            #         # Optional: Convert to float if values are not None
            #         coords = {k: safe_float(row.iloc[0].get(k)) for k in ["x0", "y0", "x1", "y1"]}
            #         coordinates_dict[placeholder] = coords

            # for placeholder, coords in coordinates_dict.items():
            #     missing = [k for k in ["x0", "y0", "x1", "y1"] if coords.get(k) is None]
            #     if missing:
            #         logger.warning(f"Coordinates missing for {placeholder}: {missing}")

            # result_dict_fig = extract_images_and_upload(rules_dict, main_s3_key, upload_folder, bucket_name, coordinates_dict)
            result_dict_fig = llm_extract_images_and_upload(rules_dict, main_s3_key, upload_folder, bucket_name)

        ###### example of rule_dict_fig
        #{"Figure 1:": ["tmp/img1.png"], "Figure 2:": ["tmp/img2.png", "tmp/img3.png", "tmp/img3.png"]}
        
        # Step 7: Combine placeholders and outputs into a dictionary
        if len(placeholders_text) != len(generated_outputs):
            raise ValueError("Mismatch between number of placeholders and generated outputs.")
        result_dict_text = dict(zip(placeholders_text, generated_outputs))
        logger.info(f"Final Placeholder-to-Text Mapping: {result_dict_text}")
        
        return result_dict_text, result_dict_fig, rules_dict_tab
    except Exception as e:
        logger.error(f"Failed to generate mapping list: {e}")
        raise

# Backed up 18th Aug to add coordinates
# def get_mapping_list_sv(bucket_name, excel_file_path, az_mapping_sheet_name, kb_name, model_id, kbname_max_size, report_id, source_file_names, source_file_types, upload_folder, table_template_master, template_fullname):
#     """
#     Extracts placeholder-to-value mappings from an Excel file stored in an S3 bucket and generates
#     textual outputs based on rules defined in the Excel sheet.

#     Args:
#         bucket_name (str): The name of the S3 bucket where the Excel file is stored.
#         excel_file_path (str): The key/path of the Excel file in the S3 bucket.
#         az_mapping_sheet_name (str): The name of the sheet in the Excel file containing the mappings.
#         kb_name (str): The name of the knowledge base used for generating outputs.
#         model_id (str): The ID of the model used for generating responses.
#         kbname_max_size (int): Maximum allowed size of the knowledge base name for the model input.
#         report_id (str): The identifier of the report, used to provide context for generation.
#         source_file_names (list): List of source file names uploaded to the system.

#     Returns:
#         dict: A dictionary mapping placeholder names to their corresponding generated text outputs.
#     """
#     try:
#         # Step 1: Extract source file prefixes
#         source_file_names_pref_list = extract_prefixes(source_file_names)
#         logger.info(f"Source File Names: {source_file_names}")
#         logger.info(f"Source File Prefixes: {source_file_names_pref_list}")

#         # Step 2: Load the Excel sheet from S3
#         df_mapping = load_excel_from_s3(bucket_name, excel_file_path, az_mapping_sheet_name)

#         # Step 3: Identify source types that have text placeholders
#         source_pdf_type_list = extract_unique_source_types(df_mapping)
#         logger.info(f"Source PDF Types: {source_pdf_type_list}")

#         # Step 4: Filter mappings to only those relevant source types
#         df_filtered = filter_mapping_data(df_mapping, source_pdf_type_list)
#         logger.info(f"Original Mapping Shape: {df_mapping.shape}")
#         logger.info(f"Filtered Mapping Shape: {df_filtered.shape}")

#         # Step 5: Extract placeholders and corresponding rules
#         if COLUMN_PLACEHOLDER_TYPE not in df_filtered.columns or COLUMN_PLACEHOLDER not in df_filtered.columns or COLUMN_RULES not in df_filtered.columns:
#             raise KeyError("One or more required columns missing in filtered DataFrame.")
#         placeholders_text = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TEXT][COLUMN_PLACEHOLDER].tolist()
#         rules_text = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TEXT][COLUMN_RULES].tolist()
        
#         placeholders_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_FIG][COLUMN_PLACEHOLDER].tolist()
#         sourcepdftyp_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE]== PLACEHOLDER_TYPE_FIG][COLUMN_SOURCE_TYPE].tolist()
#         rules_fig = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_FIG][COLUMN_RULES].tolist()
        
#         placeholders_tab = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TAB][COLUMN_PLACEHOLDER].tolist()
#         rules_tab = df_filtered[df_filtered[COLUMN_PLACEHOLDER_TYPE] == PLACEHOLDER_TYPE_TAB][COLUMN_RULES].tolist()
        
#         # logger.info(f"Placeholders: {placeholders}")
#         # logger.info(f"Rules: {rules}")

#         # Step 6: Generate outputs for each rule
        
#         # Step 6.1: Generate outputs for text rule
#         generated_outputs = generate_outputs(rules_text, kb_name, model_id, kbname_max_size, report_id)
#         logger.info(f"Generated Outputs: {generated_outputs}")
        
#         # Step 6.2: Generate outputs for figure rule
#         ################ placeholders_fig := ["<Figure 1:>", "<Figure 2:>"],  rules_fig = ["Figure 2", "Figure 3, Figure 4, Figure 5"]
#         rules_dict = {}
#         result_dict_fig = {}
#         rules_dict_tab = {}
        
        
#         if len(placeholders_tab) > 0:
#             for placeholder, rule in zip(placeholders_tab, rules_tab):
#                 # Split by comma and strip spaces
#                 sheet_name = rule.strip() 
#                 rules_dict_tab[placeholder] = sheet_name

            
            
        
#         if len(placeholders_fig) > 0:
#             for placeholder, rule in zip(placeholders_fig, rules_fig):
#                 # Split by comma and strip spaces
#                 split_rules = [part.strip() for part in rule.split(",")]
#                 rules_dict[placeholder] = split_rules


#             full_doc_type = get_document_fulltyp(sourcepdftyp_fig[0].strip(), table_template_master, template_fullname)
#             main_document_name = source_file_names[source_file_types.index(full_doc_type)]


#             main_s3_key = f"{upload_folder}{main_document_name}"
#             logger.info(f"File Location to extracted images : {main_s3_key}")
#             logger.info(f"Rules to extracted images : {rules_dict}")
#             result_dict_fig = extract_images_and_upload(rules_dict, main_s3_key, upload_folder, bucket_name)
#         ###### example of rule_dict_fig
#         #{"Figure 1:": ["tmp/img1.png"], "Figure 2:": ["tmp/img2.png", "tmp/img3.png", "tmp/img3.png"]}
        
#         # Step 7: Combine placeholders and outputs into a dictionary
#         if len(placeholders_text) != len(generated_outputs):
#             raise ValueError("Mismatch between number of placeholders and generated outputs.")
#         result_dict_text = dict(zip(placeholders_text, generated_outputs))
#         logger.info(f"Final Placeholder-to-Text Mapping: {result_dict_text}")
        
       
#         # result_dict = result_dict_text.copy()
#         # result_dict.update(result_dict_fig)
        
#         return result_dict_text, result_dict_fig, rules_dict_tab
#     except Exception as e:
#         logger.error(f"Failed to generate mapping list: {e}")
#         raise


### Read the Mapping column wise
def get_mapping_list(bucket_name, excel_file_path , az_mapping_sheet_name):
    #excel_file_path = f"{MAPPING_FILE_PATH}"
    #az_mapping_sheet_name = "Mapping"

    # Get the Excel file from S3
    response = s3.get_object(Bucket=bucket_name, Key=excel_file_path)
    excel_data = response['Body'].read()
    df_mapping = pd.read_excel(io.BytesIO(excel_data), sheet_name=az_mapping_sheet_name)

    section_names_ispr = df_mapping["ISPR Structur Section names"].tolist()
    section_names_keysearch = [part.strip().lower() 
                           for item in df_mapping["Section names key search"] 
                           for part in item.split(';')]
    ispr_summary_flag = df_mapping["Integration type in ISPR Flag"].tolist()
    ispr_map_prompt_ls = df_mapping["Map Prompt Template"].tolist()
    ispr_combine_prompt_ls = df_mapping["Combine Prompt Template"].tolist()

    return {
        "section_names_ispr": section_names_ispr,
        "section_names_keysearch": section_names_keysearch,
        "ispr_summary_flag": ispr_summary_flag,
        "ispr_map_prompt_ls": ispr_map_prompt_ls,
        "ispr_combine_prompt_ls": ispr_combine_prompt_ls

    }


# def get_mapping_list(bucket_name: str, excel_file_path: str, az_mapping_sheet_name: str) -> dict:
#     """
#     Retrieves and parses mapping data from an Excel file stored in an S3 bucket.

#     This function reads a specified sheet from the Excel file and extracts relevant
#     columns into lists for use in downstream ISPR processing logic.

#     Args:
#         bucket_name (str): The name of the S3 bucket.
#         excel_file_path (str): The path/key to the Excel file in the S3 bucket.
#         az_mapping_sheet_name (str): The name of the sheet to read from the Excel file.

#     Returns:
#         dict: A dictionary containing:
#             - section_names_ispr (list): Section names for ISPR structure.
#             - section_names_keysearch (list): Flattened and cleaned keysearch terms.
#             - ispr_summary_flag (list): Flags indicating ISPR summary types.
#             - ispr_map_prompt_ls (list): Prompt templates for mapping.
#             - ispr_combine_prompt_ls (list): Prompt templates for combining.
#     """
#     logger.info("get_mapping_list: Fetching Excel file from S3")

#     # Get the Excel file content from S3
#     response = s3.get_object(Bucket=bucket_name, Key=excel_file_path)
#     excel_data = response['Body'].read()

#     logger.info("get_mapping_list: Reading Excel data into DataFrame")

#     # Load Excel data into pandas DataFrame
#     df_mapping = pd.read_excel(io.BytesIO(excel_data), sheet_name=az_mapping_sheet_name)

#     logger.info("get_mapping_list: Extracting columns from DataFrame")

#     # Extract columns into corresponding lists
#     section_names_ispr = df_mapping[COLUMN_SECTION_NAMES_ISPR].tolist()
#     section_names_keysearch = [
#         part.strip().lower()
#         for item in df_mapping[COLUMN_SECTION_NAMES_KEYSEARCH]
#         for part in str(item).split(KEYSEARCH_DELIMITER)
#     ]
#     ispr_summary_flag = df_mapping[COLUMN_ISPR_SUMMARY_FLAG].tolist()
#     ispr_map_prompt_ls = df_mapping[COLUMN_MAP_PROMPT_TEMPLATE].tolist()
#     ispr_combine_prompt_ls = df_mapping[COLUMN_COMBINE_PROMPT_TEMPLATE].tolist()

#     logger.info("get_mapping_list: Successfully extracted all required lists")

#     return {
#         "section_names_ispr": section_names_ispr,
#         "section_names_keysearch": section_names_keysearch,
#         "ispr_summary_flag": ispr_summary_flag,
#         "ispr_map_prompt_ls": ispr_map_prompt_ls,
#         "ispr_combine_prompt_ls": ispr_combine_prompt_ls
#     }


#### Get the Summary of sections from PQRs
def get_abs_summarize(llm, docs,  ispr_map_prompt, ispr_combine_prompt, site_name):
    ispr_map_prompt_tmp = PromptTemplate(template=ispr_map_prompt, input_variables=["text"])
    ispr_combine_prompt_prompt_tmp = PromptTemplate(template=ispr_combine_prompt, input_variables=["text"])
    
    print(f'Call of get_abs_summarize for site: {site_name}')
    try:
        chain = load_summarize_chain(llm, chain_type="map_reduce", map_prompt=ispr_map_prompt_tmp, combine_prompt=ispr_combine_prompt_prompt_tmp )
        #summary = chain.run(docs)
        summary = chain.invoke(docs)
        logger.info(f"CONTEXT: get_abs_summarize: {docs}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    #except:
    #docs = docs[:-1]
    return summary


# def get_abs_summarize(llm, docs, ispr_map_prompt: str, ispr_combine_prompt: str, site_name: str):
#     """
#     Generate a summary from input documents using the provided LLM and ISPR prompt templates.

#     This function sets up a map-reduce summarization chain using two prompt templates (map and combine),
#     and applies it to a list of documents.

#     Args:
#         llm: A language model instance compatible with LangChain.
#         docs: The documents to be summarized.
#         ispr_map_prompt (str): The map prompt template string.
#         ispr_combine_prompt (str): The combine prompt template string.
#         site_name (str): Identifier for logging the source site.

#     Returns:
#         str: The generated summary from the documents.

#     Raises:
#         HTTPException: If an error occurs during the summarization process.
#     """
#     logger.info(f"get_abs_summarize: Starting summarization for site: {site_name}")

#     # Create prompt templates
#     ispr_map_prompt_tmp = PromptTemplate(template=ispr_map_prompt, input_variables=INPUT_VARS)
#     ispr_combine_prompt_tmp = PromptTemplate(template=ispr_combine_prompt, input_variables=INPUT_VARS)

#     logger.info("get_abs_summarize: Prompt templates created")

#     try:
#         # Load the summarize chain with provided prompts
#         chain = load_summarize_chain(
#             llm,
#             chain_type=CHAIN_TYPE,
#             map_prompt=ispr_map_prompt_tmp,
#             combine_prompt=ispr_combine_prompt_tmp
#         )

#         logger.info("get_abs_summarize: Summarization chain loaded, invoking on documents")

#         # Execute the chain to generate summary
#         summary = chain.invoke(docs)

#         logger.info("get_abs_summarize: Summary generation successful")

#     except Exception as e:
#         logger.info(f"get_abs_summarize: An error occurred - {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

#     return summary


###################### Extract features from the first pages of a PQR
def extract_reporting_period_product_name_site_name(
    first_page_text, date_pattern, list_site_names, list_product_names
):

    site_name = None
    product_name = None
    reporting_period = None,
    reporting_period_startdate = None
    reporting_period_enddate = None
    version_no = "v0.0"
    ###### Extract Site Name from the first page
    for site in list_site_names:
        if site.lower() in first_page_text.lower():
            site_name = site

    ##### Extract Product Name from the first page
    for product in list_product_names:
        if product.lower() in first_page_text.lower():
            product_name = product

    # Extract Reporting Period from the first page
    date_array = extract_dates(first_page_text)
    
    if len(date_array)>0:
        
        date_array = list(set(date_array))  
        date_array.sort(key=lambda x: datetime.strptime(x, "%d%b%Y"))  
        
        reporting_period_startdate = str(date_array[0])
        reporting_period_enddate = str(date_array[1])
        reporting_period = reporting_period_startdate + "_" + reporting_period_enddate
       
    version_no = extract_version(first_page_text)
        
    return reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no


# def extract_reporting_period_product_name_site_name(
#     first_page_text: str,
#     date_pattern: str,
#     list_site_names: list,
#     list_product_names: list
# ):
#     """
#     Extracts reporting period, product name, site name, and version number from the first page text.

#     This function performs the following:
#     - Identifies the site name and product name by checking if any known name appears in the first page text.
#     - Extracts date strings and determines the reporting period based on the earliest and latest dates.
#     - Extracts version number from the text using an external utility.

#     Args:
#         first_page_text (str): The raw text from the first page of the document.
#         date_pattern (str): Unused here but assumed for future pattern filtering or parsing logic.
#         list_site_names (list): List of known site names to match against the text.
#         list_product_names (list): List of known product names to match against the text.

#     Returns:
#         tuple: (
#             reporting_period (str),
#             reporting_period_startdate (str),
#             reporting_period_enddate (str),
#             product_name (str),
#             site_name (str),
#             version_no (str)
#         )
#     """
#     logger.info("extract_reporting_period_product_name_site_name: Start extracting metadata from first page")

#     site_name = None
#     product_name = None
#     reporting_period = None
#     reporting_period_startdate = None
#     reporting_period_enddate = None
#     version_no = DEFAULT_VERSION

#     # Extract site name from first page
#     for site in list_site_names:
#         if site.lower() in first_page_text.lower():
#             site_name = site
#             logger.info(f"extract_reporting_period_product_name_site_name: Site name found - {site_name}")
#             break

#     # Extract product name from first page
#     for product in list_product_names:
#         if product.lower() in first_page_text.lower():
#             product_name = product
#             logger.info(f"extract_reporting_period_product_name_site_name: Product name found - {product_name}")
#             break

#     # Extract dates from the text
#     date_array = extract_dates(first_page_text)
#     logger.info(f"extract_reporting_period_product_name_site_name: Dates extracted - {date_array}")

#     if len(date_array) > 0:
#         date_array = list(set(date_array))  # Remove duplicates
#         date_array.sort(key=lambda x: datetime.strptime(x, DATE_FORMAT))  # Sort by date

#         reporting_period_startdate = str(date_array[0])
#         reporting_period_enddate = str(date_array[1]) if len(date_array) > 1 else reporting_period_startdate
#         reporting_period = f"{reporting_period_startdate}_{reporting_period_enddate}"

#         logger.info(f"extract_reporting_period_product_name_site_name: Reporting period determined - {reporting_period}")

#     # Extract version number from text
#     version_no = extract_version(first_page_text)
#     logger.info(f"extract_reporting_period_product_name_site_name: Version extracted - {version_no}")

#     logger.info("extract_reporting_period_product_name_site_name: Metadata extraction complete")

#     return (
#         reporting_period,
#         reporting_period_startdate,
#         reporting_period_enddate,
#         product_name,
#         site_name,
#         version_no
#     )


####################
################# Extract features from the first pages of a PQR
def extract_text_tables_images_by_sections(
    bucket_name,
    pdf_path,
    section_names, date_pattern, header_pattern, footer_pattern,
    single_filename,
    reporting_period,
    reporting_period_startdate,
    reporting_period_enddate,
    product_name,
    site_name,
    output_dir,
    chunk_min,
    chunk_max,
):
    """
    Extract text, tables, and images from a PDF file, organized by sections.

    This function performs the following tasks:
    1. Reads a PDF file from an S3 bucket.
    2. Iterates through each page of the PDF.
    3. Extracts text, tables, and images from each page.
    4. Organizes the extracted content into sections based on provided section names.
    5. Stores the extracted content (text, tables, images) in dictionaries for each section.
    6. Uploads the extracted tables and images to an S3 bucket.
    7. Creates document objects for a vector database, representing the content of each section chunk.
    8. Returns a dictionary containing the extracted sections, chunk sections, documents, and metadata.

    Args:
        bucket_name (str): The name of the S3 bucket where the PDF file is stored.
        pdf_path (str): The path to the PDF file in the S3 bucket.
        section_names (list): A list of section names to extract from the PDF.
        date_pattern (str): A regular expression pattern to match dates in the PDF.
        header_pattern (str): A regular expression pattern to match headers in the PDF.
        footer_pattern (str): A regular expression pattern to match footers in the PDF.
        single_filename (str): The filename of the PDF file.
        reporting_period (str): The reporting period for the PDF file.
        reporting_period_startdate (str): The start date of the reporting period.
        reporting_period_enddate (str): The end date of the reporting period.
        product_name (str): The name of the product associated with the PDF file.
        site_name (str): The name of the site associated with the PDF file.
        output_dir (str): The directory path where extracted tables and images will be stored.
        chunk_min (int): The minimum length of text for a section chunk.
        chunk_max (int): The maximum length of text for a section chunk.

    Returns:
        dict: A dictionary containing the following keys:
            - 'sections': A list of dictionaries, where each dictionary represents a section
              with keys for section_name, file_name, reporting_period, reporting_period_startdate,
              reporting_period_enddate, product_name, site_name, page_num, images, tables, and text.
            - 'chunk_sections': A list of dictionaries, similar to 'sections', but representing
              smaller chunks of text within sections.
            - 'documents': A list of Document objects representing the content of each section chunk
              for a vector database.
            - 'reporting_period', 'reporting_period_startdate', 'reporting_period_enddate',
              'product_name', 'site_name': The corresponding metadata values.
    """

    '''
    This function is using the pdfplumber library to extract text, tables, and images from a PDF file. 
    Here's a breakdown of what it's doing with the pdfplumber extraction and matching:
    
    1. Text Extraction:
       - The function iterates through each page of the PDF file using pdf.pages.
       - For each page, it extracts the text content using page.extract_text().
       - It then splits the text into lines using text.split("\n").
       - It skips the first and second pages (page numbers 0 and 1) assuming they are the cover page and table of contents.
       - For each line of text, it checks for the presence of header and footer patterns using regular expressions (re.search(header_pattern, line) and re.search(footer_pattern, line)). If a header or footer pattern is found, it sets the corresponding header_flag or footer_flag to skip those lines.
       - If a line does not match a header or footer pattern, it tries to match the line with any of the provided section names using the match_section function.
       
    2. Section Matching:
       - The match_section function is responsible for determining if a line of text represents the start of a new section.
       - It first normalizes the line by converting it to lowercase and stripping any leading/trailing whitespaces.
       - If the line starts with a digit, it checks if the remaining part of the line (after the first word) starts with any of the normalized section names (normalized_section_names).
       - If a match is found, it returns the matched section name; otherwise, it returns None.
       
    3. Section Handling:
   - If a line matches a section name (`matched_section`), the function does the following:
     - If there was a previously active section (`current_section`), it appends the `current_section` dictionary to the `sections` list.
     - It creates a new `current_section` dictionary with the matched section name, file name, reporting period, product name, site name, page number, and empty lists for images and tables.
     - It also creates a new `chunk_current_section` dictionary with similar information, which will be used for creating document objects for the vector database.
   - If the function is inside a section (`current_section["section_name"]`), it appends the line text to the `current_section["text"]` field.
   - Additionally, it checks if the length of the `chunk_current_section["text"]` exceeds the `chunk_max` limit. If it does, and the length is greater than `chunk_min`, it creates a document object using `Document` and appends it to the `docs` list and the `chunk_current_section` dictionary to the `chunk_sections` list.

    4. Table Extraction:
    - For each page, the function extracts any tables using `page.extract_tables()`.
    - It converts each table into a Pandas DataFrame using `pd.DataFrame(table)`.
    - It saves each table as a CSV file and uploads it to the specified S3 bucket using `df.to_csv` and `s3.put_object`.
    - It appends the S3 path of the uploaded CSV file to the `current_section["tables"]` list.

    5. Image Extraction:
    - For each page, the function checks if there are any images using `page.images`.
    - For each image, it extracts the image bytes using `page.within_bbox(image_bbox).to_image()`.
    - It saves the image as a PNG file and uploads it to the specified S3 bucket using `s3.put_object`.
    - It appends the S3 path of the uploaded PNG file to the `current_section["images"]` list.

    After processing all pages, the function appends the final `current_section` and `chunk_current_section` dictionaries to the `sections` and `chunk_sections` lists, respectively. It also creates a document object for the final chunk and appends it to the `docs` list.

    Finally, it returns a dictionary containing the `sections`, `chunk_sections`, `documents` (the list of document objects), and the metadata related to the reporting period, product name, and site name.
    '''

    logger.info("extract_text_tables_images_by_sections: ---- 1 ----")
    ####
    header_flag = 0
    footer_flag = 0
    
  
    # Normalize section names for matching
    normalized_section_names = [name.lower() for name in section_names]

    # Initialize data structure
    sections = []
    chunk_sections = []
    docs = []  ### list of document object for Vector Database
    current_section = {
        "section_name": None,
        "file_name": single_filename,
        "reporting_period": None,
        "reporting_period_startdate": None,
        "reporting_period_enddate": None,
        "product_name": None,
        "site_name": None,
        "page_num": None,
        "images": [],
        "tables": [],
        "text": "",
    }

    chunk_current_section = {
        "section_name": None,
        "file_name": single_filename,
        "reporting_period": None,
        "reporting_period_startdate": None,
        "reporting_period_enddate": None,
        "product_name": None,
        "site_name": None,
        "page_num": None,
        "images": [],
        "tables": [],
        "text": "",
    }

    logger.info("extract_text_tables_images_by_sections: ---- 2 ----")
    # Function to find if a text matches any section name
    def match_section(text):
        # normalized_text = ((text.strip().lower().split(' '))[1]).strip()
        textsplit = text.strip().lower().split(" ")
        if len(textsplit) > 1:
            logger.info("extract_text_tables_images_by_sections: match_section---- 2.1 --len(textsplit) > 1")
            firstchar = ((text.strip().lower().split(" "))[0]).strip()
            restchar = (" ".join((text.strip().lower().split(" "))[1:])).strip()
            if firstchar.isdigit():
                logger.info("match_section---- 2.2 --firstchar.isdigit()")
                # return next((name for name in normalized_section_names if restchar.startswith(name)), None)
                return next(
                    (name for name in normalized_section_names if name in restchar),
                    None,
                )
        else:
            logger.info("extract_text_tables_images_by_sections: match_section---- 2.2 --len(textsplit) < 1")
        return None

    # Open the PDF using pdfplumber
    pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=pdf_path)
    pqr_file_data = pqr_file_obj["Body"].read()
    logger.info("extract_text_tables_images_by_sections: ---- 2.3 ----")

    with pdfplumber.open(io.BytesIO(pqr_file_data)) as pdf:
        # with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            logger.info("extract_text_tables_images_by_sections: ---- 3 ----")
            text = page.extract_text() or ""
            if page_num in [0,1]: ##### Jump the First page and Table of Content Pages
                continue
#             if (
#                 page_num == 0
#             ):  ##### Extract reporting period, product name and site name from the first page
                
#                 reporting_period =  reporting_period
#                 reporting_period_startdate = reporting_period_startdate
#                 reporting_period_enddate =  reporting_period_enddate
#                 product_name = product_name
#                 site_name = site_name
                    
                # (
                #     reporting_period,
                #     reporting_period_startdate,
                #     reporting_period_enddate,
                #     product_name,
                #     site_name,
                # ) = extract_reporting_period_product_name_site_name(
                #     text, date_pattern, list_site_names, list_product_names
                # )

            # elif page_num == 1:  ##### Jump the Table of Content
            #     continue

            # Split text into lines for matching section names
            lines = text.split("\n")
            for line in lines:
                logger.info("extract_text_tables_images_by_sections: ---- 4 ----")
                ######### Skip the header and footer
                if re.search(header_pattern, line):
                    header_flag = 1
                    continue
                if header_flag == 1:
                    header_flag = 0
                    continue
                if re.search(footer_pattern, line):
                    footer_flag = 1
                    continue
                if footer_flag == 1:
                    footer_flag = 0
                    continue

                ### No Header, No Footer, No Page 1
                matched_section = match_section(line)
                logger.info("extract_text_tables_images_by_sections: ---- 4.1 ----")
                if matched_section:
                    logger.info("extract_text_tables_images_by_sections: ---- 5 ----")
                    # Save the previous section
                    if chunk_current_section["section_name"]:
                        doc = Document(
                            page_content=chunk_current_section["text"],
                            metadata=chunk_current_section,
                        )
                        docs.append(doc)
                        chunk_sections.append(chunk_current_section)
                    else:
                        logger.info("extract_text_tables_images_by_sections: ---- 5.1 ----No chunk section_name")

                    if current_section["section_name"]:
                        logger.info("extract_text_tables_images_by_sections: ---- 6 ----")
                        sections.append(current_section)
                    else:
                        logger.info("extract_text_tables_images_by_sections: ---- 6.1 ----No section_name")

                    # Start a new section
                    logger.info("extract_text_tables_images_by_sections: ---- 6.2 ----Start new section")
                    current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period": reporting_period,
                        "reporting_period_startdate": reporting_period_startdate,
                        "reporting_period_enddate": reporting_period_enddate,
                        "product_name": product_name,
                        "site_name": site_name,
                        "page_num": page_num,
                        "images": [],
                        "tables": [],
                        "text": "",
                    }

                    # Start a new section chunk
                    chunk_current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period": reporting_period,
                        "reporting_period_startdate": reporting_period_startdate,
                        "reporting_period_enddate": reporting_period_enddate,
                        "product_name": product_name,
                        "site_name": site_name,
                        "page_num": page_num,
                        "images": [],
                        "tables": [],
                        "text": "",
                    }

                # Add the line text to the current section if inside a section
                if current_section["section_name"]:
                    logger.info("extract_text_tables_images_by_sections: ---- 7 ----")
                    current_section["text"] += line.strip() + "\n"
                else:
                    logger.info("extract_text_tables_images_by_sections: ---- 7.1 ----No section_name")

                if chunk_current_section["section_name"]:
                    logger.info("extract_text_tables_images_by_sections: ---- 8 ----")
                    # current_section["text"] += line.strip() + "\n"
                    # check if the size of a chunk text into a section is within the chunk size
                    if (
                        len(chunk_current_section["text"] + line.strip()) > chunk_max
                    ) and (len(chunk_current_section["text"]) > chunk_min):
                        # Save a section chunk
                        chunk_current_section_tmp = chunk_current_section.copy()
                        chunk_current_section_tmp["text"] += line.strip() + "\n"
                        ################ create a doc object
                        doc = Document(
                            page_content=chunk_current_section_tmp["text"],
                            metadata=chunk_current_section_tmp,
                        )
                        docs.append(doc)
                        chunk_sections.append(chunk_current_section_tmp)
                        # sections.append(current_section_tmp)

                        chunk_current_section["text"] = ""

                    chunk_current_section["text"] += (
                        line.strip() + "\n"
                    )  # print(sections)
                else:
                    logger.info("extract_text_tables_images_by_sections: ---- 9 ----No chunk section")

            # Extract tables
            tables = page.extract_tables()
            if tables:
                # for table in tables:

                for table_index, table in enumerate(tables):
                    # Convert the table into a pandas DataFrame
                    df = pd.DataFrame(table)

                    # Create a unique filename for each table (based on page number and table index)
                    sfilename = single_filename.split(".")[0]
                    table_filename = f"{sfilename}_table_page_{page_num + 1}_table_{table_index + 1}.csv"
                    output_csv = f"{output_dir}/{table_filename}"
                    tmp_file = "/tmp/" + table_filename

                    # Save the DataFrame as a CSV file
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False, header=True)

                    # Upload CSV to S3
                    s3.put_object(
                        Bucket=bucket_name, Key=output_csv, Body=csv_buffer.getvalue()
                    )

                    # table_count += 1
                    # print(f"Table {table_count} saved to {output_csv}")
                    current_section["tables"].append(output_csv)

            # Extract images
            if page.images:
                for img_idx, image_dict in enumerate(page.images):
                    # Extract the image bytes
                    # print("image_dict", image_dict)
                    page_height = page.height
                    image_bbox = (
                        image_dict["x0"],
                        page_height - image_dict["y1"],
                        image_dict["x1"],
                        page_height - image_dict["y0"],
                    )
                    # image = page.within_bbox(image_dict['bbox']).to_image()
                    image = page.within_bbox(image_bbox).to_image()
                    image_bytes = image.original
                    image_ext = "png"  # Can modify based on your preference

                    # Convert image to bytes
                    img_buffer = io.BytesIO()
                    image_bytes.save(img_buffer, format="PNG")
                    img_buffer.seek(0)

                    # Save the image to a file
                    sfilename = single_filename.split(".")[0]
                    image_filename = (
                        f"{sfilename}_page_{page_num + 1}_img_{img_idx + 1}.{image_ext}"
                    )
                    # print("titititititti")
                    # print(image_filename)
                    image_path = f"{output_dir}/{image_filename}"

                    # Upload PNG to S3
                    s3.put_object(Bucket=bucket_name, Key=image_path, Body=img_buffer)

                    # shutil.move(tmp_file, image_path )
                    current_section["images"].append(image_path)

    # Append the last section
    if current_section["section_name"]:
        logger.info("extract_text_tables_images_by_sections: ---- 9 ----")
        doc = Document(
            page_content=chunk_current_section["text"],
            metadata=chunk_current_section,
        )
        docs.append(doc)
        sections.append(current_section)
        chunk_sections.append(chunk_current_section)
        logger.info(f"extract_text_tables_images_by_sections: ---- 10 ---sections len: {len(sections)}")
        logger.info(f"extract_text_tables_images_by_sections: ---- 11 ---chunk_current_section len: {len(chunk_current_section)}")

    return {
        "sections": sections,
        "chunk_sections": chunk_sections,
        "documents": docs,
        "reporting_period": reporting_period,
        "reporting_period_startdate": reporting_period_startdate,
        "reporting_period_enddate": reporting_period_enddate,
        "product_name": product_name,
        "site_name": site_name,
    }

# def extract_text_tables_images_by_sections(
#     bucket_name,
#     pdf_path,
#     section_names,
#     date_pattern,
#     header_pattern,
#     footer_pattern,
#     single_filename,
#     reporting_period,
#     reporting_period_startdate,
#     reporting_period_enddate,
#     product_name,
#     site_name,
#     output_dir,
#     chunk_min,
#     chunk_max
# ):
#     """
#     Extract text, tables, and images from a PDF by identifying specific sections.

#     Returns:
#         dict: Extracted sections, chunks, document objects, and metadata.
#     """
#     def match_section(text):
#         """Check if a line matches any section name."""
#         text_split = text.strip().lower().split(" ")
#         if len(text_split) > 1:
#             logger.info("match_section: line split > 1")
#             first_char = text_split[0]
#             rest_chars = " ".join(text_split[1:]).strip()
#             if first_char.isdigit():
#                 logger.info("match_section: line starts with digit")
#                 return next((name for name in normalized_section_names if name in rest_chars), None)
#         else:
#             logger.info("match_section: line too short")
#         return None

#     def create_section_template(section_name=None, page_num=None):
#         """Create a base dictionary for sections and chunks."""
#         return {
#             "section_name": section_name,
#             "file_name": single_filename,
#             "reporting_period": reporting_period,
#             "reporting_period_startdate": reporting_period_startdate,
#             "reporting_period_enddate": reporting_period_enddate,
#             "product_name": product_name,
#             "site_name": site_name,
#             "page_num": page_num,
#             "images": [],
#             "tables": [],
#             "text": "",
#         }

#     logger.info("extract_text_tables_images_by_sections: Start")
#     header_flag = HEADER_FLAG_INIT
#     footer_flag = FOOTER_FLAG_INIT
#     normalized_section_names = [name.lower() for name in section_names]

#     sections, chunk_sections, documents = [], [], []
#     current_section = create_section_template()
#     chunk_current_section = create_section_template()

#     pdf_obj = s3.get_object(Bucket=bucket_name, Key=pdf_path)
#     pdf_data = pdf_obj["Body"].read()
#     logger.info("PDF loaded from S3")

#     with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
#         for page_num, page in enumerate(pdf.pages):
#             logger.info(f"Processing page {page_num}")
#             if page_num in [0, 1]:
#                 continue

#             text = page.extract_text() or ""
#             for line in text.split("\n"):
#                 if re.search(header_pattern, line):
#                     header_flag = 1
#                     continue
#                 if header_flag:
#                     header_flag = 0
#                     continue
#                 if re.search(footer_pattern, line):
#                     footer_flag = 1
#                     continue
#                 if footer_flag:
#                     footer_flag = 0
#                     continue

#                 matched = match_section(line)
#                 if matched:
#                     logger.info(f"New section matched: {matched}")
#                     if chunk_current_section["section_name"]:
#                         documents.append(Document(chunk_current_section["text"], chunk_current_section))
#                         chunk_sections.append(chunk_current_section)
#                     if current_section["section_name"]:
#                         sections.append(current_section)

#                     current_section = create_section_template(matched, page_num)
#                     chunk_current_section = create_section_template(matched, page_num)

#                 if current_section["section_name"]:
#                     current_section["text"] += line.strip() + "\n"

#                 if chunk_current_section["section_name"]:
#                     combined_text = chunk_current_section["text"] + line.strip()
#                     if len(combined_text) > chunk_max and len(chunk_current_section["text"]) > chunk_min:
#                         temp = chunk_current_section.copy()
#                         temp["text"] += line.strip() + "\n"
#                         documents.append(Document(temp["text"], temp))
#                         chunk_sections.append(temp)
#                         chunk_current_section["text"] = ""
#                     chunk_current_section["text"] += line.strip() + "\n"

#             # Tables
#             for idx, table in enumerate(page.extract_tables() or []):
#                 df = pd.DataFrame(table)
#                 table_key = f"{output_dir}/{single_filename.split('.')[0]}_table_page_{page_num + 1}_table_{idx + 1}.csv"
#                 buffer = io.StringIO()
#                 df.to_csv(buffer, index=False, header=True)
#                 s3.put_object(Bucket=bucket_name, Key=table_key, Body=buffer.getvalue())
#                 current_section["tables"].append(table_key)

#             # Images
#             for img_idx, img in enumerate(page.images or []):
#                 bbox = (img["x0"], page.height - img["y1"], img["x1"], page.height - img["y0"])
#                 image = page.within_bbox(bbox).to_image().original
#                 buffer = io.BytesIO()
#                 image.save(buffer, format="PNG")
#                 buffer.seek(0)
#                 img_key = f"{output_dir}/{single_filename.split('.')[0]}_page_{page_num + 1}_img_{img_idx + 1}.png"
#                 s3.put_object(Bucket=bucket_name, Key=img_key, Body=buffer)
#                 current_section["images"].append(img_key)

#     # Final appends
#     if current_section["section_name"]:
#         documents.append(Document(chunk_current_section["text"], chunk_current_section))
#         sections.append(current_section)
#         chunk_sections.append(chunk_current_section)
#         logger.info(f"Final section appended. Total sections: {len(sections)}")

#     return {
#         "sections": sections,
#         "chunk_sections": chunk_sections,
#         "documents": documents,
#         "reporting_period": reporting_period,
#         "reporting_period_startdate": reporting_period_startdate,
#         "reporting_period_enddate": reporting_period_enddate,
#         "product_name": product_name,
#         "site_name": site_name,
#     }


#####
# def ingest_pqr_file(tmp_pqr_pdf_path, date_pattern, list_site_names, list_product_names):
#     with pdfplumber.open(io.BytesIO(tmp_pqr_pdf_path)) as pdf:
#         # Get the first page
#         first_page = pdf.pages[0]
#         # Extract text from the first page
#         first_page_text = first_page.extract_text()
#         return extract_reporting_period_product_name_site_name(
#             first_page_text, date_pattern, list_site_names, list_product_names
#         )

def ingest_pqr_file(tmp_pqr_pdf_path, date_pattern, list_site_names, list_product_names):
    """
    Extracts reporting period, product name, and site name from the first page of a PDF.

    Parameters:
        tmp_pqr_pdf_path (bytes): The PDF file content in bytes.
        date_pattern (str): Regex pattern to identify date strings in the PDF.
        list_site_names (list): List of site names to search for in the PDF text.
        list_product_names (list): List of product names to search for in the PDF text.

    Returns:
        tuple: Extracted (reporting_period, product_name, site_name) from the PDF.
    """
    logger.info("ingest_pqr_file: Starting ingestion of PQR file.")

    with pdfplumber.open(io.BytesIO(tmp_pqr_pdf_path)) as pdf:
        logger.info("ingest_pqr_file: Opened PDF successfully.")

        # Get the first page
        first_page = pdf.pages[FIRST_PAGE_INDEX]
        logger.info("ingest_pqr_file: Retrieved first page of PDF.")

        # Extract text from the first page
        first_page_text = first_page.extract_text()
        logger.info("ingest_pqr_file: Extracted text from first page.")

        # Extract relevant information using helper function
        result = extract_reporting_period_product_name_site_name(
            first_page_text, date_pattern, list_site_names, list_product_names
        )
        logger.info("ingest_pqr_file: Successfully extracted reporting details.")

        return result


def ingest_phase_1(bucket_name, upload_folder, input_folder, pqr_file_name, date_pattern, list_site_names, list_product_names, reporting_period_, product_name_):
    tmp_pqr_pdf_path = f"{upload_folder}{pqr_file_name}"
    logger.info("Ingest_phase_1 ---1 Start")
    print(pqr_file_name)

    pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=tmp_pqr_pdf_path)
    pqr_file_data = pqr_file_obj["Body"].read()
    logger.info("Ingest_phase_1 --- 2")
    
    reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no  = ingest_pqr_file(pqr_file_data, date_pattern, list_site_names, list_product_names)
    logger.info("Ingest_phase_1 --- 3")

    if product_name and site_name:    
        # reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name = res_ingest
    #reporting_period = reporting_period_startdate + "_" + reporting_period_enddate

    #### create the directory structure if not exist: reporting_period/product_name
    # input_folder_pqr_reporting_period = f"{input_folder}/{product_name}"
        input_folder_pqr_reporting_period_product_name = (
            f"{input_folder}{product_name_}/{reporting_period_}"
        )
    # print(input_folder_pqr_reporting_period_product_name )
    # check_create_dir(input_folder_pqr_reporting_period)
        check_create_dir(bucket_name, input_folder_pqr_reporting_period_product_name)
        logger.info("Ingest_phase_1 --- 4")
    #### move file from tmp to reporting_period/product_name
        s3.copy(
            {"Bucket": bucket_name, "Key": tmp_pqr_pdf_path},
            bucket_name,
            f"{input_folder_pqr_reporting_period_product_name}/{pqr_file_name}",
        )
    logger.info(f"Ingest_phase_1 --- 5.1 input_folder_pqr_reporting_period_product_name: {input_folder_pqr_reporting_period_product_name}")
    logger.info(f"Ingest_phase_1 --- 5.2 pqr_file_name: {pqr_file_name}")
    logger.info(f"Ingest_phase_1 --- 5.3 tmp_pqr_pdf_path: {tmp_pqr_pdf_path}")
    logger.info(f"Ingest_phase_1 --- 5.4 len(pqr_file_data): {len(pqr_file_data)}")

    # s3.delete_object(Bucket=bucket_name, Key=tmp_pqr_pdf_path)
    logger.info("Ingest_phase_1 --- 6")
    #return reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name
    return reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no

# def ingest_phase_1(bucket_name, upload_folder, input_folder, pqr_file_name, date_pattern,
#                    list_site_names, list_product_names, reporting_period_, product_name_):
#     """
#     Handles ingestion of a PQR (Product Quality Review) file from an S3 bucket.

#     This function downloads a PDF from S3, extracts metadata from it, organizes
#     the file into a structured directory path, and copies the file to that path.

#     Parameters:
#         bucket_name (str): Name of the S3 bucket.
#         upload_folder (str): Path prefix in S3 where the file is initially uploaded.
#         input_folder (str): Base input folder to organize ingested files.
#         pqr_file_name (str): The name of the PQR file.
#         date_pattern (str): Regex pattern to extract date range from the file.
#         list_site_names (list): List of valid site names to search in the file.
#         list_product_names (list): List of valid product names to search in the file.
#         reporting_period_ (str): External reporting period reference for organizing output.
#         product_name_ (str): External product name reference for organizing output.

#     Returns:
#         tuple: (reporting_period, reporting_period_startdate, reporting_period_enddate,
#                 product_name, site_name, version_no)
#     """
#     logger.info("ingest_phase_1: Start")
#     print(pqr_file_name)

#     # Construct the full temporary path of the PQR file
#     tmp_pqr_pdf_path = f"{upload_folder}{pqr_file_name}"

#     # Fetch the PDF file from the S3 bucket
#     pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=tmp_pqr_pdf_path)
#     pqr_file_data = pqr_file_obj["Body"].read()
#     logger.info("ingest_phase_1: Retrieved file from S3")

#     # Extract metadata from PDF
#     reporting_period, reporting_period_startdate, reporting_period_enddate, \
#     product_name, site_name, version_no = ingest_pqr_file(
#         pqr_file_data, date_pattern, list_site_names, list_product_names
#     )
#     logger.info("ingest_phase_1: Extracted metadata from PDF")

#     # Continue processing only if product_name and site_name were successfully extracted
#     if product_name and site_name:
#         # Construct the final path where the file will be stored
#         input_folder_pqr_reporting_period_product_name = (
#             f"{input_folder}{product_name_}/{reporting_period_}"
#         )

#         # Ensure the directory exists
#         check_create_dir(bucket_name, input_folder_pqr_reporting_period_product_name)
#         logger.info("ingest_phase_1: Created/check directory structure in S3")

#         # Move the file to the final directory
#         s3.copy(
#             {"Bucket": bucket_name, "Key": tmp_pqr_pdf_path},
#             bucket_name,
#             f"{input_folder_pqr_reporting_period_product_name}/{pqr_file_name}",
#         )

#     # Log key details for traceability
#     logger.info(TMP_TO_FINAL_LOG_TEMPLATE.format(index="1", label="input_folder_pqr_reporting_period_product_name", value=input_folder_pqr_reporting_period_product_name))
#     logger.info(TMP_TO_FINAL_LOG_TEMPLATE.format(index="2", label="pqr_file_name", value=pqr_file_name))
#     logger.info(TMP_TO_FINAL_LOG_TEMPLATE.format(index="3", label="tmp_pqr_pdf_path", value=tmp_pqr_pdf_path))
#     logger.info(TMP_TO_FINAL_LOG_TEMPLATE.format(index="4", label="len(pqr_file_data)", value=len(pqr_file_data)))

#     logger.info("ingest_phase_1: Completed processing")
#     return reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no

#### Check if an entry is already available in Pinececone.
# def opensearch_document_exists(
#      opensearch_client: OpenSearch, opensearch_index: str, product_name, reporting_period, site_name
# ):
    
#     # Perform the metadata search
#     response = opensearch_client.search(
#         index=opensearch_index,
#         body={
#             "query": {
#                 "bool": {
#                     "must": [
#                         {"term": {"metadata.product_name.keyword": product_name}},
#                         {"term": {"metadata.reporting_period.keyword": reporting_period}},
#                         {"term": {"metadata.site_name.keyword": site_name}}
#                     ]
#                 }
#             }
#         }
#     )

#     # Check if any results were returned
#     if response['hits']['total']['value'] > 0:
#         #print("Metadata already exists:", response['hits']['hits'])
#         return 1
#     else:
#         #print("Metadata does not exist.")
#         return 0
################################################################################################

# from opensearchpy import OpenSearch

# # Constants
# METADATA_PRODUCT_NAME_KEY = "metadata.product_name.keyword"
# METADATA_REPORTING_PERIOD_KEY = "metadata.reporting_period.keyword"
# METADATA_SITE_NAME_KEY = "metadata.site_name.keyword"

# # Assuming logger is defined elsewhere in the module
# import logging
# logger = logging.getLogger(__name__)

def opensearch_document_exists(
    opensearch_client: OpenSearch,
    opensearch_index: str,
    product_name: str,
    reporting_period: str,
    site_name: str
) -> int:
    """
    Check if a document with the given metadata exists in the OpenSearch index.

    Args:
        opensearch_client (OpenSearch): An instance of the OpenSearch client.
        opensearch_index (str): Name of the OpenSearch index to search.
        product_name (str): Product name to match in the document metadata.
        reporting_period (str): Reporting period to match in the document metadata.
        site_name (str): Site name to match in the document metadata.

    Returns:
        int: 1 if a matching document exists, 0 otherwise.
    """
    logger.info("opensearch_document_exists: Initiating document existence check.")

    # Construct the query to match the provided metadata
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {METADATA_PRODUCT_NAME_KEY: product_name}},
                    {"term": {METADATA_REPORTING_PERIOD_KEY: reporting_period}},
                    {"term": {METADATA_SITE_NAME_KEY: site_name}}
                ]
            }
        }
    }

    logger.info("opensearch_document_exists: Query prepared, executing search.")

    # Perform the metadata search
    response = opensearch_client.search(
        index=opensearch_index,
        body=query_body
    )

    logger.info("opensearch_document_exists: Search executed, analyzing results.")

    # Check if any results were returned
    if response['hits']['total']['value'] > 0:
        logger.info("opensearch_document_exists: Matching document found.")
        return 1
    else:
        logger.info("opensearch_document_exists: No matching document found.")
        return 0

# 

# from opensearchpy.exceptions import OpenSearchException
# from langchain_community.vectorstores.opensearch_vector_search import OpenSearchVectorSearch

# # Constants
# LOG_INSERT_SUCCESS = "opensearch_insert_docs: Successfully added documents to OpenSearch vectorstore."
# LOG_INSERT_COUNT = "opensearch_insert_docs: Preparing to insert {} documents into OpenSearch vectorstore."
# LOG_OPENSEARCH_ERROR = "opensearch_insert_docs: OpenSearch error occurred: {}"
# LOG_UNEXPECTED_ERROR = "opensearch_insert_docs: An unexpected error occurred: {}"

# # Assuming logger is defined elsewhere in the module
# import logging
# logger = logging.getLogger(__name__)

def opensearch_insert_docs(opensearch_vdb: OpenSearchVectorSearch, documents: list) -> None:
    """
    Insert a list of documents into the OpenSearch vector store.

    Args:
        opensearch_vdb (OpenSearchVectorSearch): The OpenSearch vector database object.
        documents (list): A list of documents to insert.

    Returns:
        None
    """
    logger.info(LOG_INSERT_COUNT.format(len(documents)))

    try:
        # Prepare embeddings and documents for insertion
        opensearch_vdb.add_documents(documents=documents)
        logger.info(LOG_INSERT_SUCCESS)

    except OpenSearchException as e:
        logger.info(LOG_OPENSEARCH_ERROR.format(str(e)))

    except Exception as e:
        logger.info(LOG_UNEXPECTED_ERROR.format(str(e)))


def ingest_phase_2(bucket_name, input_folder, product_name, reporting_period, reporting_period_startdate, reporting_period_enddate, site_name, section_names, date_pattern, header_pattern, footer_pattern, pqr_file_name):
    filename_to_proceed = (
        f"{input_folder}{product_name}/{reporting_period}/{pqr_file_name}"
    )

    logger.info(f"Ingest_phase_2 --- 1 ---filename_to_proceed: {filename_to_proceed}")
    output_path_images_tables = f"{input_folder}{product_name}/{reporting_period}"
    #### if the doc link (pdf, docx, txt, doc) was already saved in Pinecone, then ignore, otherwise insert.
    # if (pinecone_document_noexists(pqr_file_name, reporting_period, product_name, site_name, INDEX_NAME)== 1):
    if pqr_file_name.endswith(".pdf"):
        sec_doc = extract_text_tables_images_by_sections(
            bucket_name,
            filename_to_proceed,
            section_names, date_pattern, header_pattern, footer_pattern,
            pqr_file_name,
            reporting_period,
            reporting_period_startdate,
            reporting_period_enddate,
            product_name,
            site_name,
            output_path_images_tables,
            1024,
            1500,
        )
        logger.info("Ingest_phase_2 --- 2 ---")
        return sec_doc

#         # section_chunks = section_chunks  + sec_doc["sections"]

#         # else:
#         #     if single_filename.endswith(".docx"):
#         #        section_chunks = section_chunks + (ingest_docx(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name))
#         # pinecone_insert_docs(sec_doc["documents"], INDEX_NAME)
#         # opensearch_insert_docs(sec_doc["documents"])


# # Constants
# SUPPORTED_FILE_EXTENSION = ".pdf"
# IMAGE_TABLE_WIDTH = 1024
# IMAGE_TABLE_HEIGHT = 1500

# # Assuming logger is defined elsewhere in the module
# # import logging
# # logger = logging.getLogger(__name__)

# def ingest_phase_2(
#     bucket_name: str,
#     input_folder: str,
#     product_name: str,
#     reporting_period: str,
#     reporting_period_startdate: str,
#     reporting_period_enddate: str,
#     site_name: str,
#     section_names: list,
#     date_pattern: str,
#     header_pattern: str,
#     footer_pattern: str,
#     pqr_file_name: str
# ):
#     """
#     Processes a PDF file from a cloud bucket and extracts text, tables, and images by sections.

#     Args:
#         bucket_name (str): The name of the cloud storage bucket.
#         input_folder (str): The base input folder path.
#         product_name (str): The product name to construct the path.
#         reporting_period (str): Reporting period used in path and metadata.
#         reporting_period_startdate (str): Start date of the reporting period.
#         reporting_period_enddate (str): End date of the reporting period.
#         site_name (str): Name of the site for metadata.
#         section_names (list): List of section names to look for in the document.
#         date_pattern (str): Regex pattern to identify dates in the document.
#         header_pattern (str): Regex pattern to identify headers.
#         footer_pattern (str): Regex pattern to identify footers.
#         pqr_file_name (str): The specific file name to process.

#     Returns:
#         dict: A dictionary containing extracted sections, documents, images, and tables.
#     """
#     # Construct the full path to the input file
#     filename_to_proceed = f"{input_folder}{product_name}/{reporting_period}/{pqr_file_name}"
#     logger.info(f"ingest_phase_2: Constructed file path: {filename_to_proceed}")

#     # Set the output directory for extracted images and tables
#     output_path_images_tables = f"{input_folder}{product_name}/{reporting_period}"

#     # Process only PDF files (extendable in the future for other formats)
#     if pqr_file_name.endswith(SUPPORTED_FILE_EXTENSION):
#         logger.info("ingest_phase_2: File is a PDF, starting section extraction.")

#         sec_doc = extract_text_tables_images_by_sections(
#             bucket_name=bucket_name,
#             pdf_path=filename_to_proceed,
#             section_names=section_names,
#             date_pattern=date_pattern,
#             header_pattern=header_pattern,
#             footer_pattern=footer_pattern,
#             single_filename=pqr_file_name,
#             reporting_period=reporting_period,
#             reporting_period_startdate=reporting_period_startdate,
#             reporting_period_enddate=reporting_period_enddate,
#             product_name=product_name,
#             site_name=site_name,
#             output_dir=output_path_images_tables,
#             chunk_min = IMAGE_TABLE_WIDTH,
#             chunk_max = IMAGE_TABLE_HEIGHT
#             # image_width=IMAGE_TABLE_WIDTH,
#             # image_height=IMAGE_TABLE_HEIGHT,
#         )
        

#         logger.info("ingest_phase_2: Section extraction completed.")
#         return sec_doc


# def save_chunks_to_json(chunks, bucket_name, output_folder, json_filename):
#     """Save the chunked sections to a JSON file."""

#     # tmp_file = "/tmp/" + json_filename
#     output_path = f"{output_folder}{json_filename}"

#     # Convert the list of dictionaries to a JSON string
#     json_str = json.dumps(chunks, indent=4)

#     # Convert the JSON string to a bytes object
#     json_bytes = io.BytesIO(json_str.encode("utf-8"))

#     s3.upload_fileobj(json_bytes, bucket_name, output_path)

# import json
# import io

# # Constants
# ENCODING_FORMAT = "utf-8"
# JSON_INDENTATION = 4

# # Assuming logger and s3 are defined elsewhere in the module
# # import logging
# # logger = logging.getLogger(__name__)

def save_chunks_to_json(chunks: list, bucket_name: str, output_folder: str, json_filename: str) -> None:
    """
    Save a list of chunked sections (as dictionaries) to a JSON file and upload it to an S3-compatible bucket.

    Args:
        chunks (list): A list of dictionary objects representing chunked sections of a document.
        bucket_name (str): Name of the S3 bucket to upload to.
        output_folder (str): The folder path in the bucket where the file will be saved.
        json_filename (str): The name of the JSON file to be saved.

    Returns:
        None
    """
    logger.info("save_chunks_to_json: Starting JSON serialization process.")

    # Construct full output path
    output_path = f"{output_folder}{json_filename}"
    logger.info(f"save_chunks_to_json: Constructed output path: {output_path}")

    # Convert chunks to a formatted JSON string
    json_str = json.dumps(chunks, indent=JSON_INDENTATION)
    logger.info("save_chunks_to_json: Converted chunks to JSON string.")

    # Encode JSON string into bytes for upload
    json_bytes = io.BytesIO(json_str.encode(ENCODING_UTF8))
    logger.info("save_chunks_to_json: Encoded JSON string to bytes.")

    # Upload the file to the specified S3 bucket
    s3.upload_fileobj(json_bytes, bucket_name, output_path)
    logger.info(f"save_chunks_to_json: Successfully uploaded JSON to bucket '{bucket_name}' at '{output_path}'.")


#### Save product_name, reporting_period and site_names extracted from the uploaded pqrfiles
# def save_pqr_paramter_to_json(bucket_name, output_folder, product_name, reporting_period, site_names,  pqr_file_names, list_versions, json_filename):
#     """Save the chunked sections to a JSON file."""

#     # tmp_file = "/tmp/" + json_filename
#     output_path = f"{output_folder}{json_filename}"

#     #Create a dictionary with the lists
#     data = {
#         "product_name": product_name,
#         "reporting_period": reporting_period,
#         "site_names": site_names,
#         "pqr_file_names": pqr_file_names,
#         "pqr_versions":list_versions
#     }
#     # Convert the list of dictionaries to a JSON string
#     json_str = json.dumps(data, indent=4)

#     # Convert the JSON string to a bytes object
#     json_bytes = io.BytesIO(json_str.encode("utf-8"))

#     s3.upload_fileobj(json_bytes, bucket_name, output_path)


# import json
# import io

# # Constants
# ENCODING_FORMAT = "utf-8"
# JSON_INDENTATION = 4

# # Assuming logger and s3 are defined elsewhere in the module
# import logging
# logger = logging.getLogger(__name__)

def save_pqr_parameter_to_json(
    bucket_name: str,
    output_folder: str,
    product_name: str,
    reporting_period: str,
    site_names: list,
    pqr_file_names: list,
    list_versions: list,
    json_filename: str
) -> None:
    """
    Save PQR metadata parameters to a JSON file and upload it to a specified S3-compatible bucket.

    Args:
        bucket_name (str): The name of the S3 bucket to upload the JSON to.
        output_folder (str): The folder path in the bucket for saving the file.
        product_name (str): Name of the product.
        reporting_period (str): The reporting period associated with the data.
        site_names (list): List of site names.
        pqr_file_names (list): List of PQR file names.
        list_versions (list): List of versions corresponding to the PQR files.
        json_filename (str): Desired name of the output JSON file.

    Returns:
        None
    """
    logger.info("save_pqr_parameter_to_json: Starting JSON creation process.")

    # Construct the full output path
    output_path = f"{output_folder}{json_filename}"
    logger.info(f"save_pqr_parameter_to_json: Output path constructed: {output_path}")

    # Create a dictionary to be serialized
    data = {
        "product_name": product_name,
        "reporting_period": reporting_period,
        "site_names": site_names,
        "pqr_file_names": pqr_file_names,
        "pqr_versions": list_versions
    }
    logger.info("save_pqr_parameter_to_json: Metadata dictionary created.")

    # Convert the dictionary to a formatted JSON string
    json_str = json.dumps(data, indent=JSON_INDENTATION)
    logger.info("save_pqr_parameter_to_json: JSON string serialization completed.")

    # Encode JSON string into bytes for uploading
    json_bytes = io.BytesIO(json_str.encode(ENCODING_FORMAT))
    logger.info("save_pqr_parameter_to_json: JSON encoded into byte stream.")

    # Upload JSON to the specified S3 bucket
    s3.upload_fileobj(json_bytes, bucket_name, output_path)
    logger.info(f"save_pqr_parameter_to_json: JSON file successfully uploaded to '{bucket_name}/{output_path}'.")


# def extract_dates(text):    
    
#     date_patterns = [
#         r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b',  # DD-MM-YYYY or DD/MM/YYYY
#         r'\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',    # YYYY-MM-DD or YYYY/MM/DD
#         r'\b(?:\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{4})\b',  # DD Mon YYYY
#         r'\b(?:\d{1,2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\d{4})\b',  # DDMonYYYY
#         r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s(?:\d{1,2},\s\d{4})\b',  # Mon DD, YYYY
#         r'\b(?:\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})\b',  # DD Month YYYY
#         r'\b(?:\w+\s\d{1,2},\s\d{4})\b',  # Month DD, YYYY
#         r'\b(?:\d{2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{2})\b',  # DDMonYY
#         r'\b(?:\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2})\b'  # ISO 8601 with time (YYYY-MM-DDThh:mm:ss)
#     ]
#     # Combine patterns into a single regex
#     combined_pattern = "|".join(date_patterns)
#     matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)
#     extracted_dates = []
#     for match in matches:
#         try:
#             for fmt in (
#                 "%Y-%m-%d",       # Example: 2024-07-24
#                 "%d-%m-%Y",       # Example: 24-07-2024
#                 "%m/%d/%Y",       # Example: 07/24/2024
#                 "%d %B %Y",       # Example: 24 July 2024
#                 "%d %b %Y",       # Example: 24 Jul 2024
#                 "%d%b%Y",         # Example: 24Jul2024
#                 "%B %d, %Y",      # Example: July 24, 2024
#                 "%b %d, %Y",      # Example: Jul 24, 2024
#                 "%d%b%y",         # Example: 01May23 (DDMonYY)
#                 "%Y-%m-%dT%H:%M:%S"  # Example: 2024-07-24T14:21:54 (ISO 8601 with time)
#             ):
#                 try:                    
#                     normalized_date = datetime.strptime(match, fmt).date().strftime("%d%b%Y")
#                     extracted_dates.append(normalized_date)
#                     break
#                 except ValueError as ve:
#                     continue
#         except ValueError:
#             continue  # Skip invalid formats
            
#     extracted_dates.sort(key=lambda x: datetime.strptime(x, "%d%b%Y"))    
#     return extracted_dates


# import re
# from datetime import datetime

# # Constants
# DATE_PATTERNS = [
#     r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b',  # DD-MM-YYYY or DD/MM/YYYY
#     r'\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',    # YYYY-MM-DD or YYYY/MM/DD
#     r'\b(?:\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{4})\b',  # DD Mon YYYY
#     r'\b(?:\d{1,2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\d{4})\b',  # DDMonYYYY
#     r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s(?:\d{1,2},\s\d{4})\b',  # Mon DD, YYYY
#     r'\b(?:\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})\b',  # DD Month YYYY
#     r'\b(?:\w+\s\d{1,2},\s\d{4})\b',  # Month DD, YYYY
#     r'\b(?:\d{2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{2})\b',  # DDMonYY
#     r'\b(?:\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2})\b'  # ISO 8601 with time (YYYY-MM-DDThh:mm:ss)
# ]

# DATE_FORMATS = [
#     "%Y-%m-%d",       # 2024-07-24
#     "%d-%m-%Y",       # 24-07-2024
#     "%m/%d/%Y",       # 07/24/2024
#     "%d %B %Y",       # 24 July 2024
#     "%d %b %Y",       # 24 Jul 2024
#     "%d%b%Y",         # 24Jul2024
#     "%B %d, %Y",      # July 24, 2024
#     "%b %d, %Y",      # Jul 24, 2024
#     "%d%b%y",         # 01May23
#     "%Y-%m-%dT%H:%M:%S"  # 2024-07-24T14:21:54
# ]

# Assuming logger is defined elsewhere in the module
# import logging
# logger = logging.getLogger(__name__)

def extract_dates(text: str) -> list:
    """
    Extracts date strings from input text, normalizes them to 'DDMonYYYY' format,
    and returns a chronologically sorted list of unique dates.

    Args:
        text (str): The input text from which to extract dates.

    Returns:
        list: A sorted list of date strings in 'DDMonYYYY' format (e.g., '24Jul2024').
    """
    logger.info("extract_dates: Starting date extraction from text.")

    # Combine all date regex patterns into a single regex
    combined_pattern = "|".join(DATE_PATTERNS)
    matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)
    logger.info(f"extract_dates: Found {len(matches)} raw date matches.")

    extracted_dates = []

    # Normalize matched date strings into consistent format
    for match in matches:
        try:
            for fmt in DATE_FORMATS:
                try:
                    normalized_date = datetime.strptime(match, fmt).date().strftime("%d%b%Y")
                    extracted_dates.append(normalized_date)
                    break  # Stop checking formats once successfully parsed
                except ValueError:
                    continue
        except ValueError:
            continue  # Skip invalid date formats

    logger.info(f"extract_dates: Successfully parsed {len(extracted_dates)} valid dates.")

    # Sort dates chronologically
    extracted_dates.sort(key=lambda x: datetime.strptime(x, "%d%b%Y"))
    logger.info("extract_dates: Completed sorting of extracted dates.")

    return extracted_dates


# def extract_pdf_contents(file_bytes: bytes) -> str:
#     """Extract text content from a PDF file."""
#     try:
#         pdf_stream = BytesIO(file_bytes)
#         reader = PyPDF2.PdfReader(pdf_stream)
#         text = ""
#         for page in range(len(reader.pages)):
#             page_obj = reader.pages[page]
#             text += page_obj.extract_text() or ""
#         return text
#     except Exception as e:
#          raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
    
# def get_file_type(file_name):
#     try:
#         return Path(file_name).suffix.lower()
#     except Exception as e:
#          raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")


# def extract_text_from_word(byte_array):
#     try:
#         doc_file = io.BytesIO(byte_array)    
#         # Read the Word document
#         doc = Document_docx(doc_file)
#         text = ''   
#         for para in doc.paragraphs:
#             text += para.text + '\n'        
#         return text
#     except Exception as e:
#          raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")


# import io
# from io import BytesIO
# from pathlib import Path
# import logging

# from fastapi import HTTPException
# import PyPDF2
# from docx import Document as Document_docx

# # Constants
# SUPPORTED_FILE_TYPES = ['.pdf', '.docx']

# # Logger setup
# logger = logging.getLogger(__name__)


def extract_pdf_contents(file_bytes: bytes) -> str:
    """
    Extract text content from a PDF file.

    Parameters:
        file_bytes (bytes): The binary content of the PDF file.

    Returns:
        str: The extracted text from the PDF.
    """
    logger.info("extract_pdf_contents: Starting PDF extraction")
    try:
        pdf_stream = BytesIO(file_bytes)
        reader = PyPDF2.PdfReader(pdf_stream)
        text = ""

        for page in range(len(reader.pages)):
            page_obj = reader.pages[page]
            page_text = page_obj.extract_text() or ""
            text += page_text
            logger.info(f"extract_pdf_contents: Extracted text from page {page + 1}")

        logger.info("extract_pdf_contents: Completed PDF extraction successfully")
        return text

    except Exception as e:
        logger.info(f"extract_pdf_contents: Failed to read PDF - {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")


def get_file_type(file_name: str) -> str:
    """
    Get the file extension/type from the file name.

    Parameters:
        file_name (str): Name of the file.

    Returns:
        str: The file extension in lowercase.
    """
    logger.info("get_file_type: Extracting file type")
    try:
        file_type = Path(file_name).suffix.lower()
        logger.info(f"get_file_type: File type is {file_type}")
        return file_type
    except Exception as e:
        logger.info(f"get_file_type: Failed to extract file type - {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")


def extract_text_from_word(byte_array: bytes) -> str:
    """
    Extract text content from a Word (.docx) document.

    Parameters:
        byte_array (bytes): The binary content of the Word file.

    Returns:
        str: The extracted text from the document.
    """
    logger.info("extract_text_from_word: Starting Word extraction")
    try:
        doc_file = io.BytesIO(byte_array)

        # Read the Word document
        doc = Document_docx(doc_file)
        text = ""

        for para in doc.paragraphs:
            text += para.text + '\n'
        logger.info("extract_text_from_word: Completed Word extraction successfully")
        return text

    except Exception as e:
        logger.info(f"extract_text_from_word: Failed to read Word file - {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")


# def generate_prompt(content: str, additional_instructions: Optional[str]) -> str:
#     """Generate a prompt for the language model based on content and additional instructions."""
#     # Define the base template for the prompt
#     base_template = """
#     %INSTRUCTIONS:"You are an assistant and your job is to answer questions based on the documents that are uploaded by the user. Don't give answer to the questions that are not relevant to the document content. Maintain the chat conversation using the previous chat interactions.    
#     """
#     # Add the content of the file to the template
#     if content:
#         base_template += f"\n\n%TEXT:\n{content}\n"
        
#     # Append the user query (additional instructions) if provided
#     if additional_instructions:
#         base_template += f"\n\n%USER QUERY:\n{additional_instructions}\n"

#     # The PromptTemplate can now be created using the combined template
#     prompt = PromptTemplate(
#         input_variables=[],  # No need for dynamic variables here as we have formatted the string manually
#         template=base_template,
#     )
#     # Return the final prompt with all components
#     return base_template


# def delete_incoming_files(bucket_name: str, upload_folder:str, output_folder: str, pqr_param_json_filename: str):
#     try:
#         product_name, reporting_period, site_names, pqr_file_names, pqr_versions = retrieve_context(bucket_name, output_folder, pqr_param_json_filename)
#     except ClientError as err:
#         logger.info(f"Error retrieving context for {product_name}: {str(err)}")
#         raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")
    
#     try:
#         tmp_response = delete_s3_folder(bucket_name, upload_folder)
#     except ClientError as err:
#         logger.info(f"Error deleting {product_name} tmp S3 folders: {str(err)}")
#         raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")
    
#     try:
#         input_response = delete_s3_folder(bucket_name, f"inputs/{product_name}")
#     except ClientError as err:
#         logger.info(f"Error deleting {product_name} input S3 folders: {str(err)}")
#         raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")


# def delete_s3_folder(bucket_name: str, folder_name: str = "temp/"):
#     try:
#         s3_res = boto3.resource('s3')
#         bucket = s3_res.Bucket(bucket_name)
#         objects_to_delete = bucket.objects.filter(Prefix=folder_name)
#         delete_requests = [{'Key': obj.key} for obj in objects_to_delete]
        
#         if delete_requests:
#             response = bucket.delete_objects(
#                 Delete={
#                     'Objects': delete_requests,
#                     'Quiet': True
#                 }
#             )
#             return response
#         else:
#             return {"Deleted": [], "Errors": []}
    
#     except ClientError as err:
#         logger.error(str(err))
#         return {"Deleted": [], "Errors": [str(err)]}
    
# ################################### Changed maded (31.01.2025) to control mutilple users
# def extract_version(text):
#     """
#     Extract the version number from a given text.

#     Args:
#         text (str): The input text from which the version number needs to be extracted.

#     Returns:
#         str or None: The extracted version number as a string if found, otherwise None.

#     The function uses a regular expression to find all occurrences of version numbers
#     in the given text. The pattern `r"\d+\.\d+"` matches any sequence of one or more
#     digits, followed by a period, and then one or more digits again. The first match
#     is returned as the version number. If no matches are found, the function returns
#     None.
#     """
#     # Regular expression pattern to match version numbers
#     version_patterns = [r"\sv\d+\.\d+", r"\d+\.\d+"]
#     # Combine patterns into a single regex
#     combined_pattern = "|".join(version_patterns)
#     # Find all occurrences of version numbers in the text
#     matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)

    
#     # If at least one match is found, return the first match
#     if matches:
#         if "v" in matches[0]:            
#             return  matches[0].strip()
#         else: return  "v" + matches[0].strip()
#     # If no matches are found, return None
#     else:
#         return "v0.0"
    

# import re
# import boto3
# from typing import Optional
# from fastapi import HTTPException
# from botocore.exceptions import ClientError
# from some_prompt_library import PromptTemplate  # Replace with actual import
# from some_module import retrieve_context  # Replace with actual import
# import logging

# # Constants
# DEFAULT_FOLDER_NAME = "temp/"
# DEFAULT_VERSION = "v0.0"
# VERSION_PATTERNS = [r"\sv\d+\.\d+", r"\d+\.\d+"]

# # Logger setup
# logger = logging.getLogger(__name__)


# def generate_prompt(content: str, additional_instructions: Optional[str]) -> str:
#     """
#     Generate a prompt for the language model based on content and additional instructions.

#     Parameters:
#         content (str): The content from the uploaded file.
#         additional_instructions (Optional[str]): Additional user instructions or query.

#     Returns:
#         str: A formatted string prompt to be used with the language model.
#     """
#     logger.info("generate_prompt: Starting to generate prompt")
    
#     base_template = """
#          %INSTRUCTIONS: You are a helpful and precise assistant specializing in analyzing document content and leveraging conversation history to answer user questions.

#         Your primary task is to answer the user's question based on the content of the provided document AND any relevant information from previous chat interactions within the same session. Pay close attention to the document content and prior conversation history, referencing them directly when answering the question. If information is contained within the document, then provide the information directly and not simply state 'The document contains the answer to your question'.
#         **Under no circumstances should you include phrases like "Thank you," "You're welcome," "I hope this helps," or any similar expressions. Your responses must be factual and directly answer the user's question.**

#         First, identify whether the user's query is a request for a summary or a direct question:

#         1. **If the user's query is a direct question (e.g., "What are the payment terms?"):**
#            Extract the relevant information from the document and chat history to provide a direct and accurate answer.


#         If the document and chat history do not contain the answer to the user's question, state that you cannot provide an answer based on the available information.

#         **PLEASE PAY CLOSE ATTENTION**: Validate if the USER_QUERY is not relevant to the document content (including previous chat interactions) using cosine similarity. If the cosine similarity is below the relevance threshold **OR if you have responded with "I cannot answer this question based on the available information.", then append the keyword 'IRRELEVANT_TOPIC' to the end of your answer.** Do not add any extra words or phrases. Do not frame generalized mitigation steps.
#         **Do not add any closing statements like 'Thank you' or "USER_QUERY" or similar.**

#         Document Content:
#         {content}

#         User Query:
#         {Query}
#         """

# #     if content:
# #         base_template += f"\n\n%TEXT:\n{content}\n"
# #         logger.info("generate_prompt: Added content to prompt")

# #     if additional_instructions:
# #         base_template += f"\n\n%USER QUERY:\n{additional_instructions}\n"
# #         logger.info("generate_prompt: Added additional instructions to prompt")

#     prompt = PromptTemplate(
#         input_variables=["content", "Query"],
#         template=base_template,
#     ).format(content=content, Query=additional_instructions)

#     logger.info("generate_prompt: Prompt generation complete")
#     return prompt


def generate_prompt(content: str, additional_instructions: Optional[str]) -> str:
    """
    Generate a prompt for the language model based on content and additional instructions.

    Parameters:
        content (str): The content from the uploaded file.
        additional_instructions (Optional[str]): Additional user instructions or query.

    Returns:
        str: A formatted string prompt to be used with the language model.
    """
    logger.info("generate_prompt: Starting to generate prompt")
    
    base_template = """
         %INSTRUCTIONS: You are a helpful and precise assistant specializing in analyzing document content and leveraging conversation history to answer user questions.

        Your primary task is to answer the user's question based on the content of the provided document AND any relevant information from previous chat interactions within the same session. Pay close attention to the document content and prior conversation history, referencing them directly when answering the question. If information is contained within the document, then provide the information directly and not simply state 'The document contains the answer to your question'.
        **Under no circumstances should you include phrases like "Thank you," "You're welcome," "I hope this helps," or any similar expressions. Your responses must be factual and directly answer the user's question.**

        First, identify whether the user's query is a request for a summary or a direct question:

        1. **If the user's query is a direct question (e.g., "What are the payment terms?"):**
           Extract the relevant information from the document and chat history to provide a direct and accurate answer.

        If the document and chat history do not contain the answer to the user's question, state that you cannot provide an answer based on the available information.

        **PLEASE PAY CLOSE ATTENTION**: Validate if the USER_QUERY is not relevant to the document content (including previous chat interactions) using cosine similarity. If the cosine similarity is below the relevance threshold **OR if you have responded with "I cannot answer this question based on the available information.", then append the keyword 'Please start a new chat and ask questions related to the content in the uploaded documents or reports in the assistant.' to the end of your answer.** Do not add any extra words or phrases. Do not frame generalized mitigation steps.
        **Do not add any closing statements like 'Thank you' or "USER_QUERY" or similar.**

        Document Content:
        {content}

        User Query:
        {Query}
        """

    prompt = PromptTemplate(
        input_variables=["content", "Query"],
        template=base_template,
    ).format(content=content, Query=additional_instructions)

    logger.info("generate_prompt: Prompt generation complete")
    return prompt


def delete_incoming_files(bucket_name: str, upload_folder: str, output_folder: str, pqr_param_json_filename: str):
    """
    Delete uploaded and input files from the S3 bucket after processing.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        upload_folder (str): Temporary folder where files were uploaded.
        output_folder (str): Folder containing output files.
        pqr_param_json_filename (str): Filename of the PQR parameter JSON.

    Raises:
        HTTPException: If any S3 operation fails.
    """
    logger.info("delete_incoming_files: Attempting to retrieve context")
    try:
        product_name, reporting_period, site_names, pqr_file_names, pqr_versions = retrieve_context(
            bucket_name, output_folder, pqr_param_json_filename
        )
        logger.info(f"delete_incoming_files: Retrieved context for {product_name}")
    except ClientError as err:
        logger.info(f"delete_incoming_files: Error retrieving context for {product_name}: {str(err)}")
        raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")

    try:
        delete_s3_folder(bucket_name, upload_folder)
        logger.info(f"delete_incoming_files: Deleted upload folder {upload_folder}")
    except ClientError as err:
        logger.info(f"delete_incoming_files: Error deleting {product_name} upload folder: {str(err)}")
        raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")

    try:
        delete_s3_folder(bucket_name, f"inputs/{product_name}")
        logger.info(f"delete_incoming_files: Deleted input folder for product {product_name}")
    except ClientError as err:
        logger.info(f"delete_incoming_files: Error deleting input folder for {product_name}: {str(err)}")
        raise HTTPException(status_code=500, detail=f"Error deleting S3 folders: {str(err)}")


def delete_s3_folder(bucket_name: str, folder_name: str = DEFAULT_FOLDER_NAME):
    """
    Delete all objects in the specified folder within an S3 bucket.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        folder_name (str): Name of the folder to delete.

    Returns:
        dict: A dictionary with keys 'Deleted' and 'Errors' listing results.
    """
    logger.info(f"delete_s3_folder: Deleting folder {folder_name} in bucket {bucket_name}")
    try:
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)
        objects_to_delete = bucket.objects.filter(Prefix=folder_name)
        delete_requests = [{'Key': obj.key} for obj in objects_to_delete]

        if delete_requests:
            response = bucket.delete_objects(
                Delete={'Objects': delete_requests, 'Quiet': True}
            )
            logger.info("delete_s3_folder: Deletion completed")
            return response
        else:
            logger.info("delete_s3_folder: No objects found to delete")
            return {"Deleted": [], "Errors": []}

    except ClientError as err:
        logger.error(f"delete_s3_folder: S3 deletion failed - {str(err)}")
        return {"Deleted": [], "Errors": [str(err)]}


def extract_version(text: str) -> str:
    """
    Extract a version number from the input text.

    Parameters:
        text (str): Text containing a potential version number.

    Returns:
        str: Extracted version string (e.g., "v1.0"), or default "v0.0" if not found.
    """
    logger.info("extract_version: Extracting version from text")

    combined_pattern = "|".join(VERSION_PATTERNS)
    matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)

    if matches:
        version = matches[0].strip()
        version = version if version.lower().startswith('v') else f"v{version}"
        logger.info(f"extract_version: Found version {version}")
        return version
    else:
        logger.info("extract_version: No version found, returning default")
        return DEFAULT_VERSION

# def sort_versions(pqr_files: list, site_names: list, versions: list) -> tuple:
#     """
#     Sorts the versions in ascending order and rearranges the corresponding file and site lists accordingly.

#     Args:
#         pqr_files (list): List of file names.
#         site_names (list): List of site names.
#         versions (list): List of version strings.

#     Returns:
#         tuple: Three sorted lists (sorted_pqr_files, sorted_site_names, sorted_versions).
#     """
#     # Check if all lists have the same length.
#     if not (len(pqr_files) == len(site_names) == len(versions)):
#         raise ValueError("All input lists must have the same length.")
        
#     # Extract numeric values from version strings
#     def extract_version_number(version):
#         match = re.match(r"v(\d+)\.(\d+)", version)
#         return (int(match.group(1)), int(match.group(2))) if match else (0, 0)

#     # Create a list of tuples and sort by extracted version numbers
#     sorted_data = sorted(zip(versions, pqr_files, site_names), key=lambda x: extract_version_number(x[0]))

#     # Unzip the sorted tuples back into separate lists
#     sorted_versions, sorted_pqr_files, sorted_site_names = zip(*sorted_data)

#     return list(sorted_pqr_files), list(sorted_site_names), list(sorted_versions)


# ####### Function to update table with input status #######
# def update_ispr_edition_status(SessionId, Timestp, newstatus, tablename):
#     """
#     Updates the PqrTrackingStatus record in the DynamoDB table.

#     Args:
#         table (boto3.resources.factory.dynamodb.Table): The DynamoDB table resource.
#         pqr_tracking_status (PqrTrackingStatus): The PqrTrackingStatus object with updated values.

#     Returns:
#         dict: The response from the update_item operation.
#     """
    
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(tablename)
    
#     # Construct the update expression
#     update_expression = "SET Ispr_Editor_Status = :Ispr_Editor_Status"
#     expression_attribute_values = {":Ispr_Editor_Status": newstatus}

#     key = { 
#             'SessionID': SessionId,
#             'Timestamp': Timestp
#         }

#     # Update the item in DynamoDB
#     update_response = table.update_item(
#         Key=key,
#         UpdateExpression=update_expression,
#         ExpressionAttributeValues=expression_attribute_values,
#         ReturnValues="UPDATED_NEW"
#     )

#     return update_response


# def update_report_locked_status(report_id, timestamp, is_locked, table_name, edit_dates_enabled = False, date_of_edit=None):
#     """
#     Updates the record status and DateofEdition in a DynamoDB table.

#     Args:
#         session_id (str): The unique identifier of the session.
#         timestamp (str): The timestamp associated with the record.
#         is_locked (bool): True if the record should be locked, False otherwise.
#         table_name (str): The name of the DynamoDB table.
#         date_of_edit (str, optional): The date of edition for the record. 
#         If not provided, the DateofEdition column will not be updated.

#     Returns:
#         dict: The response from the update_item operation.
#     """
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(table_name)

#     # Construct the update expression
#     update_expression = "SET locked = :locked_status"
#     expression_attribute_values = {":locked_status": is_locked}

#     # If date_of_edit is provided, add it to the update expression and expression attribute values
#     if edit_dates_enabled:
#         update_expression += ", updated_at = :DateofEdition"
#         expression_attribute_values[":DateofEdition"] = date_of_edit

#     key = {
#         'report_id': report_id,
#         'created_at': timestamp
#     }

#     # Update the item in DynamoDB
#     response = table.update_item(
#         Key=key,
#         UpdateExpression=update_expression,
#         ExpressionAttributeValues=expression_attribute_values,
#         ReturnValues="UPDATED_NEW"
#     )

#     return response

# import boto3
# import re
# import logging

# # Logger setup
# logger = logging.getLogger(__name__)

# # Constants
# DEFAULT_RETURN_VALUES = "UPDATED_NEW"
# VERSION_REGEX_PATTERN = r"v(\d+)\.(\d+)"


def sort_versions(pqr_files: list, site_names: list, versions: list) -> tuple:
    """
    Sorts the version strings in ascending order and rearranges the corresponding
    file and site lists to match the sorted version order.

    Args:
        pqr_files (list): List of PQR file names.
        site_names (list): List of site names.
        versions (list): List of version strings (e.g., "v1.2").

    Returns:
        tuple: Three lists — sorted_pqr_files, sorted_site_names, sorted_versions.
    """
    logger.info("sort_versions: Validating input list lengths")
    if not (len(pqr_files) == len(site_names) == len(versions)):
        raise ValueError("All input lists must have the same length.")

    logger.info("sort_versions: Extracting numeric values from version strings")

    def extract_version_number(version: str):
        match = re.match(VERSION_REGEX_PATTERN, version)
        return (int(match.group(1)), int(match.group(2))) if match else (0, 0)

    logger.info("sort_versions: Sorting data by version numbers")
    sorted_data = sorted(zip(versions, pqr_files, site_names), key=lambda x: extract_version_number(x[0]))

    sorted_versions, sorted_pqr_files, sorted_site_names = zip(*sorted_data)

    logger.info("sort_versions: Sorting complete")
    return list(sorted_pqr_files), list(sorted_site_names), list(sorted_versions)


def update_ispr_edition_status(session_id: str, timestamp: str, new_status: str, table_name: str) -> dict:
    """
    Updates the 'Ispr_Editor_Status' field for a specific record in a DynamoDB table.

    Args:
        session_id (str): Unique session ID.
        timestamp (str): Associated timestamp of the record.
        new_status (str): New status value to set.
        table_name (str): Name of the DynamoDB table.

    Returns:
        dict: Response from the update_item operation.
    """
    logger.info("update_ispr_edition_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    key = {
        'SessionID': session_id,
        'Timestamp': timestamp
    }

    update_expression = "SET Ispr_Editor_Status = :Ispr_Editor_Status"
    expression_attribute_values = {
        ":Ispr_Editor_Status": new_status
    }

    logger.info(f"update_ispr_edition_status: Updating record for session {session_id}")
    update_response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues=DEFAULT_RETURN_VALUES
    )

    logger.info("update_ispr_edition_status: Update successful")
    return update_response


# def update_report_locked_status(
#     report_id: str,
#     timestamp: str,
#     is_locked: bool,
#     table_name: str,
#     edit_dates_enabled: bool = False,
#     date_of_edit: str = None
# ) -> dict:
#     """
#     Updates the 'locked' status (and optionally the edit date) of a report record in DynamoDB.

#     Args:
#         report_id (str): The unique identifier for the report.
#         timestamp (str): The creation timestamp of the report.
#         is_locked (bool): True to lock the report, False to unlock.
#         table_name (str): The name of the DynamoDB table.
#         edit_dates_enabled (bool, optional): Whether to update the 'updated_at' date.
#         date_of_edit (str, optional): The date of the edit to set if enabled.

#     Returns:
#         dict: Response from the update_item operation.
#     """
#     logger.info("update_report_locked_status: Initializing DynamoDB resource")
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(table_name)

#     key = {
#         'report_id': report_id,
#         'created_at': timestamp
#     }

#     update_expression = "SET locked = :locked_status"
#     expression_attribute_values = {
#         ":locked_status": is_locked
#     }

#     if edit_dates_enabled:
#         logger.info("update_report_locked_status: Updating with edit date")
#         update_expression += ", updated_at = :DateofEdition"
#         expression_attribute_values[":DateofEdition"] = date_of_edit

#     logger.info(f"update_report_locked_status: Updating record for report_id {report_id}")
#     response = table.update_item(
#         Key=key,
#         UpdateExpression=update_expression,
#         ExpressionAttributeValues=expression_attribute_values,
#         ReturnValues=DEFAULT_RETURN_VALUES
#     )

#     logger.info("update_report_locked_status: Update successful")
#     return response
    
# def update_ispr_edit_status(session_id, timestamp, is_locked, table_name, edit_dates_enabled = False, date_of_edit=None):
#     """
#     Updates the record status and DateofEdition in a DynamoDB table.

#     Args:
#         session_id (str): The unique identifier of the session.
#         timestamp (str): The timestamp associated with the record.
#         is_locked (bool): True if the record should be locked, False otherwise.
#         table_name (str): The name of the DynamoDB table.
#         date_of_edit (str, optional): The date of edition for the record. 
#         If not provided, the DateofEdition column will not be updated.

#     Returns:
#         dict: The response from the update_item operation.
#     """
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(table_name)

#     # Construct the update expression
#     update_expression = "SET Ispr_Editor_Status = :Ispr_Editor_Status"
#     expression_attribute_values = {":Ispr_Editor_Status": is_locked}

#     # If date_of_edit is provided, add it to the update expression and expression attribute values
#     if edit_dates_enabled:
#         update_expression += ", DateofEdition = :DateofEdition"
#         expression_attribute_values[":DateofEdition"] = date_of_edit

#     key = {
#         'SessionID': session_id,
#         'Timestamp': timestamp
#     }

#     # Update the item in DynamoDB
#     response = table.update_item(
#         Key=key,
#         UpdateExpression=update_expression,
#         ExpressionAttributeValues=expression_attribute_values,
#         ReturnValues="UPDATED_NEW"
#     )

#     return response


# def extract_latest_timestamp_ispr_status(productName, reporting_period, list_site_names, list_versions, tablename):
#     """
#     This function checks the status of an ispr record in the DynamoDB table.

#     Steps:
#     1. Print a message indicating the start of the function.
#     2. Create a DynamoDB resource object using boto3.
#     3. Create a table object representing the specified DynamoDB table.
#     4. Query the table using the ProductName as the partition key and filter by other 
#        fields (ReportingPeriod, SiteName, Versions).
#     5. If the response has items:
#         a. Sort the items in descending order based on the Timestamp field.
#         b. Print a message indicating the response is True.
#         c. Return the 'pqr_status' value from the first item (most recent).
#     6. If the response has no items:
#         a. Print a message indicating the response is False.
#         b. Return None.

#     Args:
#         ispr_records (PqrTrackingStatus): An object containing the ProductName, 
#         ReportingPeriod, SiteName, and Versions fields.
#         tablename (str): The name of the DynamoDB table to query.

#     Returns:
#         str or None: The 'pqr_status' value from the most recent item matching 
#         the query, or None if no matching items are found.
#     """    
    
#     print("\ncheck_ispr_status ------------->")
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(tablename)

#     # Query using ProductName as partition key and filter by active sessions
#     response = table.query(
#             IndexName="ProductName-index",
#             KeyConditionExpression=Key('ProductName').eq(productName),  # Query by UserId
#             FilterExpression=(
#                 Attr('ReportingPeriod').eq(reporting_period) &
#                 Attr('SiteName').eq(list_site_names) &
#                 Attr('Versions').eq(list_versions) 
#             ),
#             ScanIndexForward=False,
#             # Limit = 100
#         )
    
#     # Sort items based on Timestamp
#     print(f"extract timestamp------------->{response}")
#     if response['Items']:
        
#         response['Items'].sort(
#             key=lambda x: datetime.fromisoformat(x['Timestamp']),
#             reverse=True
#         )
#         print("extract timestamp------------->True")
#         return response['Items'][0]['UserId'], response['Items'][0]['Timestamp']
#         # response['Items']['pqr_status']
#     else:
#         print("extract timestamp------------->False")
#         return None, None

# import boto3
# import logging
# from boto3.dynamodb.conditions import Key, Attr
# from datetime import datetime

# # Logger setup
# logger = logging.getLogger(__name__)

# # Constants
# DEFAULT_RETURN_VALUES = "UPDATED_NEW"
# ISPR_EDITOR_STATUS_FIELD = "Ispr_Editor_Status"
# UPDATED_AT_FIELD = "updated_at"
# DATE_OF_EDITION_FIELD = "DateofEdition"
# LOCKED_FIELD = "locked"
# INDEX_NAME = "ProductName-index"

def update_report_locked_status(
    report_id: str,
    timestamp: str,
    is_locked: bool,
    table_name: str,
    edit_dates_enabled: bool = False,
    date_of_edit: str = None
) -> dict:
    """
    Updates the 'locked' status (and optionally the edit date) of a report record in DynamoDB.

    Args:
        report_id (str): The unique identifier for the report.
        timestamp (str): The creation timestamp of the report.
        is_locked (bool): True to lock the report, False to unlock.
        table_name (str): The name of the DynamoDB table.
        edit_dates_enabled (bool, optional): Whether to update the edit date.
        date_of_edit (str, optional): The edit date value if enabled.

    Returns:
        dict: The response from the update_item operation.
    """
    logger.info("update_report_locked_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    key = {
        'report_id': report_id,
        'created_at': timestamp
    }

    update_expression = f"SET {LOCKED_FIELD} = :locked_status"
    expression_attribute_values = {
        ":locked_status": is_locked
    }

    if edit_dates_enabled:
        logger.info("update_report_locked_status: Including updated_at in update")
        update_expression += f", {UPDATED_AT_FIELD} = :DateofEdition"
        expression_attribute_values[":DateofEdition"] = date_of_edit

    logger.info(f"update_report_locked_status: Updating record for report_id {report_id}")
    response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues=DEFAULT_RETURN_VALUES
    )

    logger.info("update_report_locked_status: Update successful")
    return response


def update_ispr_edit_status(
    session_id: str,
    timestamp: str,
    is_locked: bool,
    table_name: str,
    edit_dates_enabled: bool = False,
    date_of_edit: str = None
) -> dict:
    """
    Updates the ISPR editor status (and optionally the edit date) in a DynamoDB table.

    Args:
        session_id (str): Unique session ID.
        timestamp (str): Timestamp for the record.
        is_locked (bool): New value for Ispr_Editor_Status.
        table_name (str): DynamoDB table name.
        edit_dates_enabled (bool, optional): Whether to update the DateofEdition.
        date_of_edit (str, optional): The date of edition value.

    Returns:
        dict: The response from the update_item operation.
    """
    logger.info("update_ispr_edit_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    key = {
        'SessionID': session_id,
        'Timestamp': timestamp
    }

    update_expression = f"SET {ISPR_EDITOR_STATUS_FIELD} = :Ispr_Editor_Status"
    expression_attribute_values = {
        ":Ispr_Editor_Status": is_locked
    }

    if edit_dates_enabled:
        logger.info("update_ispr_edit_status: Including DateofEdition in update")
        update_expression += f", {DATE_OF_EDITION_FIELD} = :DateofEdition"
        expression_attribute_values[":DateofEdition"] = date_of_edit

    logger.info(f"update_ispr_edit_status: Updating record for session_id {session_id}")
    response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues=DEFAULT_RETURN_VALUES
    )

    logger.info("update_ispr_edit_status: Update successful")
    return response


def extract_latest_timestamp_ispr_status(
    product_name: str,
    reporting_period: str,
    site_name: str,
    version: str,
    table_name: str
) -> tuple:
    """
    Retrieves the latest timestamp and user ID for a specific ISPR record from DynamoDB.

    Args:
        product_name (str): Product name used as the partition key.
        reporting_period (str): Reporting period value.
        site_name (str): Site name to filter.
        version (str): Version to filter.
        table_name (str): Name of the DynamoDB table.

    Returns:
        tuple: (user_id, timestamp) of the latest matching record, or (None, None) if no match found.
    """
    logger.info("extract_latest_timestamp_ispr_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    logger.info(f"extract_latest_timestamp_ispr_status: Querying for product {product_name}")
    response = table.query(
        IndexName=INDEX_NAME,
        KeyConditionExpression=Key('ProductName').eq(product_name),
        FilterExpression=(
            Attr('ReportingPeriod').eq(reporting_period) &
            Attr('SiteName').eq(site_name) &
            Attr('Versions').eq(version)
        ),
        ScanIndexForward=False
    )

    logger.info(f"extract_latest_timestamp_ispr_status: Query returned {len(response['Items'])} items")

    if response['Items']:
        response['Items'].sort(
            key=lambda x: datetime.fromisoformat(x['Timestamp']),
            reverse=True
        )
        logger.info("extract_latest_timestamp_ispr_status: Found latest timestamp")
        return response['Items'][0].get('UserId'), response['Items'][0].get('Timestamp')

    logger.info("extract_latest_timestamp_ispr_status: No matching items found")
    return None, None

    
# def check_ispr_status(ispr_records: PqrTrackingStatus, tablename):
#     """
#     This function checks the status of an ispr record in the DynamoDB table.

#     Steps:
#     1. Print a message indicating the start of the function.
#     2. Create a DynamoDB resource object using boto3.
#     3. Create a table object representing the specified DynamoDB table.
#     4. Query the table using the ProductName as the partition key and filter by other 
#        fields (ReportingPeriod, SiteName, Versions).
#     5. If the response has items:
#         a. Sort the items in descending order based on the Timestamp field.
#         b. Print a message indicating the response is True.
#         c. Return the 'pqr_status' value from the first item (most recent).
#     6. If the response has no items:
#         a. Print a message indicating the response is False.
#         b. Return None.

#     Args:
#         ispr_records (PqrTrackingStatus): An object containing the ProductName, 
#         ReportingPeriod, SiteName, and Versions fields.
#         tablename (str): The name of the DynamoDB table to query.

#     Returns:
#         str or None: The 'pqr_status' value from the most recent item matching 
#         the query, or None if no matching items are found.
#     """    
    
#     print("\ncheck_ispr_status ------------->")
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(tablename)

#     # Query using ProductName as partition key and filter by active sessions
#     response = table.query(
#             IndexName="ProductName-index",
#             KeyConditionExpression=Key('ProductName').eq(ispr_records.ProductName),  # Query by UserId
#             FilterExpression=(
#                 Attr('ReportingPeriod').eq(ispr_records.ReportingPeriod) &
#                 Attr('SiteName').eq(ispr_records.SiteName) &
#                 Attr('Versions').eq(ispr_records.Versions) 
#             ),
#             # ScanIndexForward=False,
#             # Limit = 100
#         )
    
#     # Sort items based on Timestamp
#     print(f"check_ispr_status response------------->{response}")
#     if response['Items']:
        
#         response['Items'].sort(
#             key=lambda x: datetime.fromisoformat(x['Timestamp']),
#             reverse=True
#         )
#         print("check_ispr_status response------------->True")
#         return response['Items'][0]['pqr_status'], response['Items'][0]['UserId']
#         # response['Items']['pqr_status']
#     else:
#         print("check_ispr_status response------------->False")
#         return None, None
    

# def store_pqr_status(interaction: PqrTrackingStatus, tablename): 
#     """
#     This function stores the PQR tracking status in a DynamoDB table.

#     Steps:
#     1. Create a DynamoDB resource using the boto3 library.
#     2. Get a reference to the specified DynamoDB table.
#     3. Try the following:
#         a. Convert the PqrTrackingStatus object to a dictionary using the `dict()` method.
#         b. Store the dictionary as an item in the DynamoDB table using the `put_item()` method.
#         c. Return a success message.
#     4. If an exception occurs during the process, raise an HTTPException with status code 500 
#         and the exception message as detail.

#     Args:
#         interaction (PqrTrackingStatus): An instance of the PqrTrackingStatus class containing the PQR tracking status data.
#         tablename (str): The name of the DynamoDB table where the PQR tracking status will be stored.

#     Returns:
#         dict: A dictionary containing a success message if the PQR tracking status is stored successfully.

#     Raises:
#         HTTPException: If an exception occurs during the process, an HTTPException with status code 500 
#         and the exception message as detail is raised.
#     """    
#     print("\nstore_pqr_status ------------->")
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(tablename)
    
#     try:
#         item = interaction.dict()
#         table.put_item(Item=item)
#         return {"message": "ISPR-Edition interaction stored successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# def extract_section_names(text, section_names):
#     """
#     Extracts all section names from the given text based on the provided list of section names.
#     It handles non-English characters and different variations of section names.
#     It also removes duplicates from the output.

#     Args:
#         text (str): The text to search for section names.
#         section_names (list): A list of section names to match against.

#     Returns:
#         list: A list of unique matched section names found in the text.
#     """
#     try:
#         # Create a list of regular expressions for each section name
#         section_patterns = [re.compile(r'(\d+\s*)?({})'.format(re.escape(name)), re.IGNORECASE) for name in section_names]
#         section_patterns.extend([re.compile(r'(\d+\s*)?({}[\w\s]+)'.format(re.escape(name)), re.IGNORECASE) for name in section_names])

#         sections = []
#         for line in text.split('\n'):
#             for pattern in section_patterns:
#                 match = pattern.search(line)
#                 if match:
#                     section_name = match.group(2).strip()
#                     if section_name not in sections:
#                         sections.append(section_name)
#                     break

#         # logger.info(f"Extracted {len(sections)} unique section names from the input text.")
#         return sections

#     except Exception as e:
#         logger.error(f"An error occurred while extracting section names: {e}")
#         return []


# import boto3
# import logging
# import re
# from datetime import datetime
# from fastapi import HTTPException
# from boto3.dynamodb.conditions import Key, Attr

# # Logger setup
# logger = logging.getLogger(__name__)

# # Constants
# INDEX_NAME_PRODUCT_NAME = "ProductName-index"


def check_ispr_status(ispr_records, tablename):
    """
    Checks the status of an ISPR record in the DynamoDB table by querying based on ProductName,
    ReportingPeriod, SiteName, and Versions fields in the ispr_records object.

    Args:
        ispr_records (PqrTrackingStatus): Object containing ProductName, ReportingPeriod, SiteName, Versions.
        tablename (str): DynamoDB table name.

    Returns:
        tuple: (pqr_status, UserId) from the most recent matching record, or (None, None) if none found.
    """
    logger.info("check_ispr_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)

    logger.info(f"check_ispr_status: Querying table {tablename} for ProductName={ispr_records.ProductName}")
    response = table.query(
        IndexName=INDEX_NAME_PRODUCT_NAME,
        KeyConditionExpression=Key('ProductName').eq(ispr_records.ProductName),
        FilterExpression=(
            Attr('ReportingPeriod').eq(ispr_records.ReportingPeriod) &
            Attr('SiteName').eq(ispr_records.SiteName) &
            Attr('Versions').eq(ispr_records.Versions)
        )
    )

    logger.info(f"check_ispr_status: Retrieved {len(response.get('Items', []))} items")
    items = response.get('Items', [])

    if items:
        items.sort(key=lambda x: datetime.fromisoformat(x['Timestamp']), reverse=True)
        logger.info("check_ispr_status: Returning latest pqr_status and UserId")
        return items[0].get('pqr_status'), items[0].get('UserId')

    logger.info("check_ispr_status: No matching items found")
    return None, None


def store_pqr_status(interaction, tablename):
    """
    Stores the PQR tracking status in the specified DynamoDB table.

    Args:
        interaction (PqrTrackingStatus): Object containing PQR tracking status data.
        tablename (str): Name of the DynamoDB table.

    Returns:
        dict: Success message if stored successfully.

    Raises:
        HTTPException: If an error occurs during storage, with status code 500.
    """
    logger.info("store_pqr_status: Initializing DynamoDB resource")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)

    try:
        item = interaction.dict()
        logger.info("store_pqr_status: Putting item into table")
        table.put_item(Item=item)
        logger.info("store_pqr_status: Item stored successfully")
        return {"message": "ISPR-Edition interaction stored successfully"}
    except Exception as e:
        logger.error(f"store_pqr_status: Exception occurred - {e}")
        raise HTTPException(status_code=500, detail=str(e))


def extract_section_names(text, section_names):
    """
    Extracts unique section names from the input text based on provided list of section names.
    Handles variations and non-English characters.

    Args:
        text (str): Input text to search.
        section_names (list): List of section names to match.

    Returns:
        list: Unique matched section names found in the text.
    """
    logger.info("extract_section_names: Starting extraction of section names")
    try:
        # Compile regex patterns for each section name, with optional leading digits and trailing words
        section_patterns = [re.compile(r'(\d+\s*)?({})'.format(re.escape(name)), re.IGNORECASE) for name in section_names]
        section_patterns.extend([re.compile(r'(\d+\s*)?({}[\w\s]+)'.format(re.escape(name)), re.IGNORECASE) for name in section_names])

        sections = []
        for line in text.split('\n'):
            for pattern in section_patterns:
                match = pattern.search(line)
                if match:
                    section_name = match.group(2).strip()
                    if section_name not in sections:
                        sections.append(section_name)
                    break

        logger.info(f"extract_section_names: Extracted {len(sections)} unique section names")
        return sections

    except Exception as e:
        logger.error(f"extract_section_names: Error occurred - {e}")
        return []


########################### SIV Chunking ####################################

############################# Extract docx_s3_key, excel_s3_key template
# def sitev_extract_template(TEMPLATE_DB, template_fullname):
#     """
#     This function retrieves template information from a DynamoDB table based on the provided template_fullname.

#     Args:
#         TEMPLATE_DB (str): The name of the DynamoDB table where template information is stored.
#         template_fullname (str): The full name of the template to retrieve information for.

#     Returns:
#         tuple: A tuple containing the following template information:
#             - template_id (str): The unique identifier of the template.
#             - s3_path (str or None): The S3 path of the template file (None for ISPR templates).
#             - s3_excel_path (str or None): The S3 path of the template Excel file (None for ISPR templates).
#             - section_names (list or None): A list of section names in the template (None for ISPR templates).
#             - template_name (str): The name of the template.
#             - doc_types (list or None): A list of document types associated with the template (None for ISPR templates).
#             - doc_types_full (list or None): A list of full document type names associated with the template (None for ISPR templates).

#     Raises:
#         HTTPException: If an unexpected error occurs during the execution of the function.
#     """
#     # Create a DynamoDB resource
#     dynamodb = boto3.resource('dynamodb')
    
#     # Get the table from the DynamoDB resource
#     table = dynamodb.Table(TEMPLATE_DB)

#     try:
#         # Scan the table for items matching the provided template_fullname
#         response = table.scan(
#             FilterExpression=Attr('template_fullname').eq(template_fullname)
#         )

#         # Get the first (and only) item from the response
#         res = response['Items'][0]

#         # Check if the template is an ISPR template
#         if "ISPR" in template_fullname:
#             # Return template information for ISPR templates
#             return res["template_id"], None, None, None, res["template_name"], None, None
#         else:
#             # Return template information for non-ISPR templates
#             return res["template_id"], res["s3_path"], res["s3_excel_path"], res["section_names"], res["template_name"], res["doc_types"], res["doc_types_full"]

#     except Exception as e:
#         # Log the error and raise an HTTP exception
#         logger.error(f"An unexpected error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again later.")


# ######################### chunking INE Template
# def get_document_typ(document_name, source_file_names, source_file_types, table_template_master, template_fullname):
    
#     template_id, docx_s3_key, excel_s3_key, section_name_list, template_name_, doc_typ_list, doc_typ_full_list = sitev_extract_template(table_template_master, template_fullname)
    
#     try:
#         full_file_typ = source_file_types[source_file_names.index(document_name)]
#         return doc_typ_list[doc_typ_full_list.index(full_file_typ)]
    
#     except Exception as e:
#         logger.error(f'file name not present in the list:get_document_typ: {str(e)}')
    
#     return document_name.split("_")[0]


# def chunk_sv_report(bucket_name, chunk_output_folder, s3_file_name, product_name, reporting_period, template_fullname,  report_id):
    
#      #################### Created a chunk and save it in in S3
#         metadata = {
#             "metadataAttributes": {
#                  "report_id": report_id,
#                  "document_name": report_doc_name,
#                  "product_name": product_name,
#                  "template_name": template_fullname,
#                  "reporting_period": reporting_period,
#                  "section_name": ""
#            }
#         }
#         chunk_text = chunk_text  + str(metadata)
#         # chunk_bytes = io.BytesIO(chunk_text)
#         chunk_path = f"{output_folder}/{report_doc_name}_chunk_{page_num}.txt"
#         metadata_json_path = f"{chunk_path}.metadata.json"
#         # metadata_json_path = f"{output_folder}/{pqr_file_name}_chunk_{page_num}.metadata.json"

#         s3.put_object(
#                 Bucket=bucket_name, Key=chunk_path , Body=chunk_text 
#             )

#         #################### Built metadata associated to the chunk and save it in S3

#          # Convert the list of dictionaries to a JSON string
#         json_str = json.dumps(metadata, indent=4)
#         # Convert the JSON string to a bytes object
#         json_bytes = io.BytesIO(json_str.encode("utf-8"))
#         s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)
   

# import boto3
# import json
# import io
# from boto3.dynamodb.conditions import Attr
# from fastapi import HTTPException

# # Logger assumed to be defined somewhere in the module
# # logger = logging.getLogger(__name__)

# ISPR_KEYWORD = "ISPR"  # Constant used for ISPR template detection


def sitev_extract_template(template_db, template_fullname):
    """
    Retrieves template metadata from DynamoDB based on the template's full name.

    Args:
        template_db (str): The name of the DynamoDB table containing template data.
        template_fullname (str): The full name of the template to search for.

    Returns:
        tuple: Template metadata including:
            - template_id (str)
            - s3_path (str or None)
            - s3_excel_path (str or None)
            - section_names (list or None)
            - template_name (str)
            - doc_types (list or None)
            - doc_types_full (list or None)

    Raises:
        HTTPException: If an error occurs during the DynamoDB operation.
    """
    logger.info("sitev_extract_template: Starting template lookup")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(template_db)

    try:
        # Filter the DynamoDB table based on template_fullname
        response = table.scan(
            FilterExpression=Attr('template_fullname').eq(template_fullname)
        )

        res = response['Items'][0]
        logger.info("sitev_extract_template: Template found in DynamoDB")

        if ISPR_KEYWORD in template_fullname:
            logger.info("sitev_extract_template: Detected ISPR template")
            return res["template_id"], None, None, None, res["template_name"], None, None

        logger.info("sitev_extract_template: Returning standard template metadata")
        return (
            res["template_id"],
            res["s3_path"],
            res["s3_excel_path"],
            res["section_names"],
            res["template_name"],
            res["doc_types"],
            res["doc_types_full"]
        )

    except Exception as e:
        logger.error(f"sitev_extract_template: An unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again later.")

def get_document_typ(document_name, source_file_names, source_file_types, table_template_master, template_fullname):
    """
    Determines the document type from source file types using the template mapping.

    Args:
        document_name (str): The name of the document file.
        source_file_names (list): List of source file names.
        source_file_types (list): Corresponding list of file types.
        table_template_master (str): DynamoDB table name for templates.
        template_fullname (str): Full template name.

    Returns:
        str: Document type name (mapped or inferred).
    """
    logger.info("get_document_typ: Extracting template and resolving document type")

    template_id, docx_s3_key, excel_s3_key, section_names, template_name, doc_types, doc_types_full = sitev_extract_template(
        table_template_master, template_fullname
    )

    try:
        # Map the document type from source file type
        full_file_type = source_file_types[source_file_names.index(document_name)]
        logger.info("get_document_typ: Mapped document type from template")
        return doc_types[doc_types_full.index(full_file_type)]

    except Exception as e:
        logger.error(f"get_document_typ: Fallback to inferred document type due to error: {str(e)}")

    # Fallback: use prefix from filename
    return document_name.split("_")[0]


def chunk_sv_report(bucket_name, chunk_output_folder, s3_file_name, product_name, reporting_period, template_fullname, report_id):
    """
    Creates a text chunk and associated metadata, uploads both to S3.

    Args:
        bucket_name (str): S3 bucket name.
        chunk_output_folder (str): Output folder path for the chunk.
        s3_file_name (str): File name for the report in S3.
        product_name (str): Name of the product.
        reporting_period (str): Reporting period.
        template_fullname (str): Full name of the template used.
        report_id (str): ID of the report.

    Returns:
        None
    """
    logger.info("chunk_sv_report: Starting chunk creation and upload")

    # Placeholder variables (you may need to define them earlier in the real implementation)
    report_doc_name = s3_file_name
    output_folder = chunk_output_folder
    page_num = 1
    chunk_text = "..."  # Actual chunk content should be inserted here

    # Prepare metadata dictionary
    metadata = {
        "metadataAttributes": {
            "report_id": report_id,
            "document_name": report_doc_name,
            "product_name": product_name,
            "template_name": template_fullname,
            "reporting_period": reporting_period,
            "section_name": ""
        }
    }

    # Append metadata to the chunk
    chunk_text += str(metadata)
    chunk_path = f"{output_folder}/{report_doc_name}_chunk_{page_num}.txt"
    metadata_json_path = f"{chunk_path}.metadata.json"

    # Upload the chunk text to S3
    logger.info("chunk_sv_report: Uploading chunk to S3")
    s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

    # Convert metadata to JSON and upload
    json_str = json.dumps(metadata, indent=4)
    json_bytes = io.BytesIO(json_str.encode("utf-8"))

    logger.info("chunk_sv_report: Uploading metadata JSON to S3")
    s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)


# def chunk_report(bucket_name, report_doc_name, chunk_text, chunk_num, section_name, product_name, reporting_period, template_fullname, report_id, created_at, userid, chunk_report_folder ):
    

#         #################### Created a chunk and save it in in S3
#         metadata = {
#             "metadataAttributes": {
#                  "report_id": report_id,
#                  "document_name": report_doc_name,
#                  "created_by": userid,
#                  "created_at":  int((datetime.fromisoformat(created_at)).timestamp()*1000),
#                  "product_name": product_name.lower(),
#                  "template_name": template_fullname.lower(),
#                  "reporting_period": reporting_period,
#                  "section_name": section_name.lower()
#            }
#         }
#         chunk_text = chunk_text  + str(metadata)
#         # chunk_bytes = io.BytesIO(chunk_text)
#         chunk_path = f"{chunk_report_folder}/{report_doc_name}_chunk_{chunk_num}.txt"
#         metadata_json_path = f"{chunk_path}.metadata.json"
#         # metadata_json_path = f"{output_folder}/{pqr_file_name}_chunk_{page_num}.metadata.json"

#         s3.put_object(
#                 Bucket=bucket_name, Key=chunk_path , Body=chunk_text 
#             )

#         #################### Built metadata associated to the chunk and save it in S3

#          # Convert the list of dictionaries to a JSON string
#         json_str = json.dumps(metadata, indent=4)
#         # Convert the JSON string to a bytes object
#         json_bytes = io.BytesIO(json_str.encode("utf-8"))
#         s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)

# def chunk_site_validation(bucket_name, upload_folder, output_folder, source_file_names, source_file_types, table_template_master, template_fullname, report_id):
    
#     list_of_upload_files = get_list_of_files(bucket_name, upload_folder)
#     print(list_of_upload_files)

#     for i, pqr_file in enumerate(list_of_upload_files):  # Iterate over each file
#         # if i == 0: 
#         #     continue
#         pqr_file_key = pqr_file["Key"]
#         pqr_file_name = pqr_file_key.split("/")[-1]
    
#        # Open the PDF using pdfplumber
#         print("####################################")
#         print(pqr_file_key)
#         pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=pqr_file_key)
#         pqr_file_data = pqr_file_obj["Body"].read()
#         with pdfplumber.open(io.BytesIO(pqr_file_data)) as pdf:
#         # with pdfplumber.open(pdf_path) as pdf:
#             for page_num, page in enumerate(pdf.pages):
                
#                 #################### Created a chunk and save it in in S3
#                 metadata = {
#                     "metadataAttributes": {
#                          "report_id": report_id,
#                          "document_name": pqr_file_name,
#                          "document_typ": get_document_typ(pqr_file_name, source_file_names, source_file_types, table_template_master, template_fullname),
#                          "page_nr": page_num
#                    }
#                 }
#                 chunk_text = page.extract_text() + str(metadata)
#                 # chunk_bytes = io.BytesIO(chunk_text)
#                 chunk_path = f"{output_folder}/{pqr_file_name}_chunk_{page_num}.txt"
#                 metadata_json_path = f"{chunk_path}.metadata.json"
#                 # metadata_json_path = f"{output_folder}/{pqr_file_name}_chunk_{page_num}.metadata.json"
            
#                 s3.put_object(
#                         Bucket=bucket_name, Key=chunk_path , Body=chunk_text 
#                     )
                
#                 #################### Built metadata associated to the chunk and save it in S3
               
#                  # Convert the list of dictionaries to a JSON string
#                 json_str = json.dumps(metadata, indent=4)
#                 # Convert the JSON string to a bytes object
#                 json_bytes = io.BytesIO(json_str.encode("utf-8"))
#                 s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)


# import io
# import json
# from datetime import datetime
# import pdfplumber

# # Assumes `s3` and `logger` are already defined elsewhere in the module

# TIMESTAMP_MULTIPLIER = 1000  # Multiplier to convert seconds to milliseconds


def chunk_report(bucket_name, report_doc_name, chunk_text, chunk_num, section_name,
                 product_name, reporting_period, template_fullname,
                 report_id, created_at, userid, chunk_report_folder):
    """
    Saves a report chunk and its associated metadata to S3.

    Args:
        bucket_name (str): Name of the S3 bucket.
        report_doc_name (str): Name of the report document.
        chunk_text (str): Text content of the chunk.
        chunk_num (int): Chunk number.
        section_name (str): Section of the document the chunk belongs to.
        product_name (str): Name of the product.
        reporting_period (str): Time period the report covers.
        template_fullname (str): Full name of the template.
        report_id (str): Unique identifier of the report.
        created_at (str): Timestamp when the chunk was created (ISO format).
        userid (str): ID of the user creating the chunk.
        chunk_report_folder (str): Folder path in S3 where chunks are saved.

    Returns:
        None
    """
    logger.info("chunk_report: Preparing metadata for report chunk")

    metadata = {
        "metadataAttributes": {
            "report_id": report_id,
            "document_name": report_doc_name,
            "created_by": userid,
            "created_at": int(datetime.fromisoformat(created_at).timestamp() * TIMESTAMP_MULTIPLIER),
            "product_name": product_name.lower(),
            "template_name": template_fullname.lower(),
            "reporting_period": reporting_period,
            "section_name": section_name.lower()
        }
    }

    chunk_text += str(metadata)
    chunk_path = f"{chunk_report_folder}/{report_doc_name}_chunk_{chunk_num}.txt"
    metadata_json_path = f"{chunk_path}.metadata.json"

    logger.info("chunk_report: Uploading chunk to S3")
    s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

    logger.info("chunk_report: Uploading metadata to S3")
    json_str = json.dumps(metadata, indent=4)
    json_bytes = io.BytesIO(json_str.encode("utf-8"))
    s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)


def chunk_site_validation(bucket_name, upload_folder, output_folder,
                          source_file_names, source_file_types,
                          table_template_master, template_fullname, report_id):
    """
    Processes PDF files in an S3 folder, extracts page-wise chunks with metadata, and saves them to S3.

    Args:
        bucket_name (str): Name of the S3 bucket.
        upload_folder (str): Folder in S3 containing uploaded PDF files.
        output_folder (str): Destination folder for chunked output in S3.
        source_file_names (list): List of original file names.
        source_file_types (list): Corresponding list of file types.
        table_template_master (str): DynamoDB table name for template data.
        template_fullname (str): Full name of the template.
        report_id (str): Unique identifier of the report.

    Returns:
        None
    """
    logger.info("chunk_site_validation: Fetching list of files from S3 upload folder")
    list_of_upload_files = get_list_of_files(bucket_name, upload_folder)
    print(list_of_upload_files)

    for i, upload_file in enumerate(list_of_upload_files):
        file_key = upload_file["Key"]
        file_name = file_key.split("/")[-1]
        
        if file_name.endswith(".pdf"):
            
            logger.info(f"chunk_site_validation: Processing file {file_key}")

            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_data = file_obj["Body"].read()

            with pdfplumber.open(io.BytesIO(file_data)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()

                    logger.info(f"chunk_site_validation: Extracting text from page {page_num} of {file_name}")

                    metadata = {
                        "metadataAttributes": {
                            "report_id": report_id,
                            "document_name": file_name,
                            "document_typ": get_document_typ(
                                file_name, source_file_names, source_file_types,
                                table_template_master, template_fullname
                            ),
                            "page_nr": page_num
                        }
                    }

                    chunk_text = page_text + str(metadata)
                    chunk_path = f"{output_folder}/{file_name}_chunk_{page_num}.txt"
                    metadata_json_path = f"{chunk_path}.metadata.json"

                    logger.info(f"chunk_site_validation: Uploading chunk page {page_num} to S3")
                    s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

                    logger.info(f"chunk_site_validation: Uploading metadata for page {page_num}")
                    json_str = json.dumps(metadata, indent=4)
                    json_bytes = io.BytesIO(json_str.encode("utf-8"))
                    s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)

        if file_name.endswith("xlsx"):
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            excel_data = response['Body'].read()
            sheets = pd.read_excel(io.BytesIO(excel_data), sheet_name=None, keep_default_na=False, na_values=[""])

            for sheet_name, df in sheets.items():
                # # Convert rows to dicts
                # records = df.to_dict(orient="records")
                
                # # Wrap with metadata
                # for i, record in enumerate(records):
                metadata = {
                    "metadataAttributes": {
                        "report_id": report_id,
                        "document_name": file_name,
                        "document_typ": get_document_typ(
                            file_name, source_file_names, source_file_types,
                            table_template_master, template_fullname
                        ),
                        "sheet_name": sheet_name
                    }
                }
                chunk_text = json.dumps({"headers": df.columns.tolist(), "rows": df.values.tolist()}, ensure_ascii=False, default=str)
                chunk_path = f"{output_folder}/{file_name}_chunk_{sheet_name}.txt"
                metadata_json_path = f"{chunk_path}.metadata.json"

                logger.info(f"chunk_site_validation: Uploading Sheet name {sheet_name} to S3")
                s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

                logger.info(f"chunk_site_validation: Uploading metadata for Sheet name {sheet_name}")
                json_str = json.dumps(metadata, indent=4)
                json_bytes = io.BytesIO(json_str.encode("utf-8"))
                s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)

################################################################################################

def update_report_queue_status(report_id, file_version, newstatus, tablename):
    """
    Updates the PqrTrackingStatus record in the DynamoDB table.

    Args:
        table (boto3.resources.factory.dynamodb.Table): The DynamoDB table resource.
        pqr_tracking_status (PqrTrackingStatus): The PqrTrackingStatus object with updated values.

    Returns:
        dict: The response from the update_item operation.
    """
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    # Construct the update expression
    update_expression = "SET status_in_queue = :status_in_queue"
    expression_attribute_values = {":status_in_queue": newstatus}

    key = { 
            'report_id': report_id,
            'file_version': file_version
        }

    # Update the item in DynamoDB
    update_response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW"
    )

    return update_response

def Ingest_Report(knowledge_base_name, bucket_name, kbname_max_size, ds_name="knowledgebase_report_gen", chunk_report_folder = "chunk_report_gen"):
    try:
        # chunk_report(bucket_name, upload_folder, chunk_output_folder, source_file_names, source_file_types, table_template_master, template_fullname, report_id)
    # Get knowledge base ID
        kb_id = get_knowledge_base_id(knowledge_base_name, kbname_max_size)

    # Create a data source using the S3 file
        data_source_id = get_data_source_id(kb_id, ds_name)
        if not data_source_id:
            data_source_id = create_data_source(kb_id, bucket_name, chunk_report_folder, ds_name)

    # data_source_id=check_data_sources(kb_id, template_name)
    # # # check_data_source
    # # #check_data_source(kb_id,'kb-txt-source_12')


    # # Start sync job
        sync_job_id = sync_data_source(kb_id, data_source_id)
        
        status = 'INIT'
        while status not in  ['COMPLETE', 'FAILED','STOPPED']:
            status = check_sync_status(kb_id, data_source_id, sync_job_id)

        if status in 'COMPLETE':
            logger.info(f"\n Data source synced successfully")

            #   print("Data source synced successfully!")
        else:
                   logger.error(f"Sync failed with status: {status}")
                   raise Exception(f"Sync failed with status: {status}")
        
        # return  kb_id, data_source_id, sync_job_id

    # # Check sync job status until complete
#         status = 'INIT'
#         while status not in  ['COMPLETE', 'FAILED','STOPPED']:
#             status = check_sync_status(kb_id, data_source_id, sync_job_id)

#         if status in 'COMPLETE':
#             logger.info(f"\n Data source synced successfully")
             
#             #   print("Data source synced successfully!")
#         else:
#             raise Exception(f"Sync failed with status: {status}")
            
#         delete_s3_folder(bucket_name, upload_folder)
            #   break
    except Exception as e:
        logger.error(f'Unexpected error in utils:Ingest_SIV: {str(e)}')
        # delete_s3_folder(bucket_name, upload_folder)

def Ingest_SIV(knowledge_base_name, bucket_name, upload_folder, source_file_names, source_file_types, chunk_output_folder, table_template_master, template_fullname, template_name, kbname_max_size, report_id):
    try:
        chunk_site_validation(bucket_name, upload_folder, chunk_output_folder, source_file_names, source_file_types, table_template_master, template_fullname, report_id)
    # Get knowledge base ID
        kb_id = get_knowledge_base_id(knowledge_base_name, kbname_max_size)

    # Create a data source using the S3 file
        data_source_id = get_data_source_id(kb_id, template_name)
        if not data_source_id:
            data_source_id = create_data_source(kb_id, bucket_name, chunk_output_folder, template_name)

    # data_source_id=check_data_sources(kb_id, template_name)
    # # # check_data_source
    # # #check_data_source(kb_id,'kb-txt-source_12')


    # # Start sync job
        sync_job_id = sync_data_source(kb_id, data_source_id)
        
        status = 'INIT'
        while status not in  ['COMPLETE', 'FAILED','STOPPED']:
            status = check_sync_status(kb_id, data_source_id, sync_job_id)

        if status in 'COMPLETE':
            logger.info(f"\n Data source synced successfully")

            #   print("Data source synced successfully!")
        else:
                   logger.error(f"Sync failed with status: {status}")
                   raise Exception(f"Sync failed with status: {status}")
        
        # return  kb_id, data_source_id, sync_job_id

    # # Check sync job status until complete
#         status = 'INIT'
#         while status not in  ['COMPLETE', 'FAILED','STOPPED']:
#             status = check_sync_status(kb_id, data_source_id, sync_job_id)

#         if status in 'COMPLETE':
#             logger.info(f"\n Data source synced successfully")
             
#             #   print("Data source synced successfully!")
#         else:
#             raise Exception(f"Sync failed with status: {status}")
            
#         delete_s3_folder(bucket_name, upload_folder)
            #   break
    except Exception as e:
        logger.error(f'Unexpected error in utils:Ingest_SIV: {str(e)}')
        # delete_s3_folder(bucket_name, upload_folder)

def Ingest_PQR(bucket_name, upload_folder, input_folder, output_folder, date_pattern, header_pattern, footer_pattern, list_site_names, list_product_names, pqr_param_json_filename, sessionId, userId, timestamp, product_name_, reporting_period_, list_filesite_name, list_versions, list_pqr_file_name, report_id, file_version): 
    """
    Ingest and process PQR (Product Quality Review) files from an S3 bucket.

    This function retrieves a list of uploaded files from the specified S3 bucket and upload folder.
    It iterates through the list of files, extracts metadata (reporting period, product name, site name, version number)
    and sections from each file. The extracted sections are saved to JSON files in the specified output folder.
    If any file fails during the ingestion process, its status is updated in a DynamoDB table, and an IngestResult
    object is added to the result list.

    **External Function Calls:**
    1. get_mapping_list(bucket_name, excel_file_path, az_mapping_sheet_name): Retrieves a mapping dictionary from a specified Excel file.
    2. get_list_of_files(bucket_name, upload_folder): Retrieves a list of uploaded files in the specified S3 bucket and folder.
    3. Ingest_phase_1(bucket_name, upload_folder, input_folder, pqr_file_name, date_pattern, list_site_names, list_product_names, reporting_period_, product_name_): Extracts metadata (reporting period, product name, site name, version number) from the PQR file.
    4. Ingest_phase_2(bucket_name, input_folder, product_name_, reporting_period_, reporting_period_startdate, reporting_period_enddate, site_name, section_names, date_pattern, header_pattern, footer_pattern, pqr_file_name): Extracts sections from the PQR file.
    5. IngestResult(fileName, status, comment): Creates an IngestResult object representing the status of file ingestion.
    6. update_report_queue_status(report_id, file_version, status, REPORTSQUEUE_DYNAMOTABLE): Updates the status of the report queue in a DynamoDB table.
    7. delete_s3_folder(bucket_name, upload_folder): Deletes the specified S3 folder containing uploaded files.
    8. save_chunks_to_json(sections, bucket_name, output_folder, json_filename): Saves the extracted sections or section chunks to a JSON file in the specified S3 bucket and folder.
    9. save_pqr_paramter_to_json(bucket_name, output_folder, product_name_, reporting_period_, list_filesite_name, list_pqr_file_name, list_versions, pqr_param_json_filename)`: Saves the PQR parameters to a JSON file in the specified S3 bucket and folder.

    Args:
        bucket_name (str): Name of the S3 bucket containing the uploaded files.
        upload_folder (str): Path to the folder in the S3 bucket containing the uploaded files.
        input_folder (str): Path to the folder in the S3 bucket where the input files are located.
        output_folder (str): Path to the folder in the S3 bucket where the output JSON files will be saved.
        date_pattern (str): Pattern used to extract the date from the file.
        header_pattern (str): Pattern used to identify section headers in the file.
        footer_pattern (str): Pattern used to identify section footers in the file.
        list_site_names (list): List of site names used for validation.
        list_product_names (list): List of product names used for validation.
        pqr_param_json_filename (str): Filename for the PQR parameter JSON file.
        sessionId (str): Session ID associated with the ingestion process.
        userId (str): User ID associated with the ingestion process.
        timestamp (str): Timestamp associated with the ingestion process.
        product_name_ (str): Product name used for generating output filenames.
        reporting_period_ (str): Reporting period used for generating output filenames.
        list_filesite_name (list): List of file site names.
        list_versions (list): List of versions.
        list_pqr_file_name (list): List of PQR file names.
        report_id (str): ID of the report associated with the ingestion process.
        file_version (str): Version of the file being processed.

    Returns:
        list: A list of IngestResult objects representing the status of file ingestion for each failed file.
    """
    logger.info('Ingest_PQR: ----- Start ----')
    mapping_dic = get_mapping_list(bucket_name, excel_file_path=f"{MAPPING_FILE_PATH}",
                             az_mapping_sheet_name=f"{SHEET_NAME_MAPPING}")
    section_names = mapping_dic["section_names_keysearch"] 
            
    list_of_upload_files = get_list_of_files(bucket_name, upload_folder)
    sections = []
    section_chunks = []
    site_names = []
    pqr_file_names = []
    result = []
    # product_name = None
    site_name = None
    
     ######## Testing Check status table to track the progress of PQR file processing.
    # prTrackingRes = PqrTrackingStatus(UserId=userId, Timestamp=timestamp, SessionID=sessionId, ProductName=product_name_, ReportingPeriod=reporting_period_, SiteName=list_filesite_name, Versions=list_versions, PqrFilename=list_pqr_file_name, pqr_status="")
    

    # Check if there is an existing ISPR status record in the DynamoDB table
    # print(check_ispr_status(prTrackingRes, tablename))
#     ispr_status, ispr_user = check_ispr_status(prTrackingRes, tablename)
#     if ispr_status:
        
#         if (ispr_status.lower() == 'pending'):
#             IngestResp = IngestResult(fileName="ispr", status="ispr_running", comment=f"ISPR already in running by user {ispr_user}")
#             delete_s3_folder(bucket_name, upload_folder)
#             result.append(IngestResp)
#             return result
        
#         if (ispr_status.lower() == 'success'):
#             IngestResp = IngestResult(fileName="ispr", status="ispr_success", comment=f"ISPR has been generated already by user {ispr_user}")
#             delete_s3_folder(bucket_name, upload_folder)
#             result.append(IngestResp)
#             return result
        
#         if (ispr_status.lower() == 'failed'):
#             prTrackingRes.pqr_status = "pending"
#             store_pqr_status(prTrackingRes, tablename)
        
#          # if (ispr_status.lower() == 'failed'):
#          #    IngestResp = IngestResult(fileName="ispr", status="ispr_success", comment=f"ISPR has been generated already by {userid}")
#          #    result.append(IngestResp)
#          #    return result
        
        
            
#         # Aborting the process of ISPR when the earlier existing PQR status record was pending or failed.
#     else:
#         # Adding PQR status as 'pending' when an ISPR was not existing.
#         prTrackingRes.pqr_status = "pending"
#         store_pqr_status(prTrackingRes, tablename)

    # Iterate over the list of uploaded files # Start of loop
    if len(list_of_upload_files) > 0:
        logger.info('Ingest_PQR: ----- 1 ----')

        for pqr_file in list_of_upload_files:
            pqr_file_key = pqr_file["Key"]
            if pqr_file_key.endswith(".pdf") or pqr_file_key.endswith(".docx"):
                pqr_file_name = pqr_file_key.split("/")[-1]
                pqr_file_names.append(pqr_file_name)
                
                logger.info(f"Ingest_PQR: Extraction of Metadata from {pqr_file_name} into json")
                
                try:
                    # reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no = Ingest_phase_1(
                    reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no = ingest_phase_1(
                    bucket_name, upload_folder, input_folder, pqr_file_name, date_pattern, list_site_names, list_product_names, reporting_period_, product_name_)
                    # logger.info('Ingest_PQR: ---------- TEST -----------")
                    logger.info(f"Ingest_PQR: Output ------------ TEST ----------")
                    # logger.info(f"Ingest_PQR: Output of Ingest_phase_1 for file: {pqr_file_name}: Details: {res_ingest_phase1}")
                    if product_name and site_name:
                        logger.info(f"Ingest_PQR:Output of ingest_phase_1 for file: Details: {product_name}-{site_name}")
                        
                    else:
                        logger.info(f"Ingest_PQR:Output of ingest_phase_1 for file: Details: Missing.")
                        # delete_incoming_files(bucket_name, output_folder, pqr_param_json_filename)
                        # delete_s3_folder(bucket_name, "tmp/")
                        error_msg = f"Error in ingest_phase_1 for file: {pqr_file_name}. Details: Product or site name not present in first page"
                        IngestResp = IngestResult(fileName=pqr_file_name, status="failed", comment= "First Page not as expected: missing product or site name")
                        # update_pqr_tracking_status(prTrackingRes.UserId, prTrackingRes.Timestamp, "failed", tablename)
                        # update_report_queue_status(report_id, file_version, "failed", REPORTSQUEUE_DYNAMOTABLE)
                        result.append(IngestResp)
                        logger.info(f"Ingest_PQR: {error_msg}")
                        continue
                        
                except Exception as e:
                    # delete_incoming_files(bucket_name, output_folder, pqr_param_json_filename)
                    # delete_s3_folder(bucket_name, "tmp/")
                    error_msg = f'Error in ingest_phase_1 for file: {pqr_file_name}. Details: {str(e)}'
                    # update_pqr_tracking_status(prTrackingRes.UserId, prTrackingRes.Timestamp, "failed", tablename)
                    # update_report_queue_status(report_id, file_version, "failed", REPORTSQUEUE_DYNAMOTABLE)
                    IngestResp = IngestResult(fileName=pqr_file_name, status="failed", comment= "Unsupported file structure")
                    result.append(IngestResp)
                    logger.info(f"Ingest_PQR: {error_msg}")
                    continue
                    # raise HTTPException(status_code=400, detail=pqr_file_name)
                        
                site_names.append(site_name)
                
                try:
                    sec_doc = ingest_phase_2(
                           bucket_name, input_folder, product_name_, reporting_period_, reporting_period_startdate, reporting_period_enddate, site_name, section_names, date_pattern, header_pattern, footer_pattern, pqr_file_name
                )
                    
                    logger.info(f"Ingest_PQR: Ingest_phase_2 Compare extracted section vs fixed: {len(section_names)}")
                    if len(sec_doc["sections"]) < len(section_names) - 1:
                        len_sec_doc= len(sec_doc["sections"])
                        len_sec_name = len(section_names) - 1
                        # delete_incoming_files(bucket_name, output_folder, pqr_param_json_filename)
                        IngestResp = IngestResult(fileName=pqr_file_name, status="failed", comment= "Validation failed: Unsupported file structure")
                        # update_pqr_tracking_status(prTrackingRes.UserId, prTrackingRes.Timestamp, "failed", tablename)
                        # update_report_queue_status(report_id, file_version, "failed", REPORTSQUEUE_DYNAMOTABLE)
                        error_msg = f"Ingest_PQR: Error in Ingest_phase_2 for file: {pqr_file_name}. Details: Validation failed: Unsupported file structure"
                        logger.info(f"Ingest_PQR: {error_msg}")
                        logger.info(f"Ingest_PQR: Error in Ingest_phase_2 for file: {len_sec_doc} - {len_sec_name}")
                        result.append(IngestResp)
                        continue
                        
                except Exception as e:
                    # delete_incoming_files(bucket_name, output_folder, pqr_param_json_filename)
                    error_msg = f'Ingest_PQR: Error in Ingest_phase_2 for file: {pqr_file_name}. Details: {str(e)}'
                    logger.error(error_msg)
                    IngestResp = IngestResult(fileName=pqr_file_name, status="failed", comment= "Unsupported file structure")
                    # update_pqr_tracking_status(prTrackingRes.UserId, prTrackingRes.Timestamp, "failed", tablename)
                    update_report_queue_status(report_id, file_version, "failed", REPORTSQUEUE_DYNAMOTABLE)
                    result.append(IngestResp)
                    continue
                    #raise HTTPException(status_code=400, detail=pqr_file_name)

                sections = sections + sec_doc["sections"]
                section_chunks = section_chunks + sec_doc["chunk_sections"]
                # print(sec_doc["documents"])
                
                # pinecone_insert_docs(sec_doc["documents"], INDEX_NAME)
                logger.info(f"Ingest_PQR: Ingestion of  {pqr_file_name} to VectorDB")
#                 if opensearch_document_exists(opensearch_client, opensearch_index, product_name, reporting_period, site_name) == 0:
#                     opensearch_insert_docs(opensearch_vdb, sec_doc["documents"])
                    
#                     IngestResp = IngestResult(fileName=pqr_file_name, status="success", comment= "sucessfull sync up the VectorDB")
                    
#                 else:
#                     IngestResp = IngestResult(fileName=pqr_file_name, status="success", comment= "File already present in VectorDB")
                
                # result.append(IngestResp)
    
        # delete_s3_folder(bucket_name, upload_folder)
    
    logger.info('Ingest_PQR: ----- 2 ----')
    if product_name and site_name:
        logger.info(f"Ingest_PQR: ----- 3 ----{product_name} : {site_name}")
        ispr_json_filename = "ispr" + "-" + product_name_ + "-" + reporting_period_ + "-" + userId + ".json"
        ispr_chunk_json_filename = (
        "ispr_chunk" + "-" + product_name_ + "-" + reporting_period_ + "-" + userId + ".json"
    )

        logger.info(f"Ingest_PQR: ----- 4 ---len(sections)-{len(sections)}")
        logger.info(f"Ingest_PQR: ----- 5 ----ispr_json_filename : {ispr_json_filename}")
        
        save_chunks_to_json(sections, bucket_name, output_folder, ispr_json_filename)
        logger.info(f"Ingest_PQR: ----- 6 ----len(section_chunks) : {len(section_chunks)}")
        save_chunks_to_json(section_chunks, bucket_name, output_folder, ispr_chunk_json_filename)
        save_pqr_parameter_to_json(bucket_name, output_folder, product_name_, reporting_period_, list_filesite_name, list_pqr_file_name, list_versions, pqr_param_json_filename)
        logger.info(f"Ingest_PQR: ----- 7 ----ispr_chunk_json_filename : {ispr_chunk_json_filename}")
    return result

########################### GENAIOPSIT-56: formatting fix: START ###########################
def highlight_text(run, color=HIGHLIGHT_COLOR):
    """
    Highlights the given run with the specified color.
    """
    # Normalize color if it's an enum
    if isinstance(color, WD_COLOR_INDEX):
        color = color.name.lower()  # e.g., 'YELLOW' → 'yellow'
    elif not isinstance(color, str):
        color = str(color).lower()

    # Word only supports a limited set of highlight colors
    allowed_colors = {
        'yellow', 'green', 'cyan', 'magenta', 'blue', 'red',
        'gray', 'darkYellow', 'darkGreen', 'darkCyan', 'darkMagenta',
        'darkBlue', 'darkRed', 'darkGray', 'lightGray', 'black'
    }

    if color not in allowed_colors:
        color = 'yellow'  # fallback to a safe default

    highlight = OxmlElement('w:highlight')
    highlight.set(qn('w:val'), color)
    run._element.get_or_add_rPr().append(highlight)

def highlight_subtext_in_hyperlink(hlink, substring, color_hex="00B050"):
    """
    Highlights a specific substring inside a <w:hyperlink> XML element.

    This preserves the hyperlink while highlighting only the target text,
    without affecting the rest of the content.

    Args:
        hlink (OxmlElement): The hyperlink XML element containing the text.
        substring (str): The substring to highlight.
        color_hex (str): The highlight color (Word-compatible color name or hex).

    Note:
        Word highlight colors must be names like 'green', not hex.
        This function handles internal Word XML runs directly.
    """
    ns = hlink.nsmap

    for r in hlink.xpath('.//w:r'):
        t = r.find('.//w:t', namespaces=ns)

        if t is None or not t.text or substring not in t.text:
            continue

        before, match, after = t.text.partition(substring)

        # Replace current run with "before"
        t.text = before if before else match

        if before and match:
            # Create a new run for the highlighted match
            match_run = OxmlElement("w:r")
            rPr = OxmlElement("w:rPr")
            highlight = OxmlElement("w:highlight")
            highlight.set(qn("w:val"), color_hex)
            rPr.append(highlight)

            match_t = OxmlElement("w:t")
            match_t.text = match

            match_run.append(rPr)
            match_run.append(match_t)

            r.addnext(match_run)
            r = match_run  # Move reference to match run

        elif not before:  # Match is at the beginning — highlight this run
            # rPr = r.find('w:rPr')
            rPr = r.find('w:rPr', namespaces=ns)

            if rPr is None:
                rPr = OxmlElement('w:rPr')
                r.insert(0, rPr)

            highlight = OxmlElement('w:highlight')
            highlight.set(qn('w:val'), color_hex)
            rPr.append(highlight)

        if after:
            # Create a new run for the "after" part
            after_run = OxmlElement("w:r")
            after_t = OxmlElement("w:t")
            after_t.text = after
            after_run.append(after_t)

            r.addnext(after_run)

def build_segments(full_text, replacements):
    """
    Splits full_text into segments around placeholders.

    Each segment is a tuple:
        (text, is_highlighted, placeholder_key)

    Example:
        Input: "Hello <Name>!"
        Replacements: {"<Name>": "Alice"}

        Returns:
            [
                ("Hello ", False, None),
                ("Alice", True, "<Name>"),
                ("!", False, None)
            ]

    Args:
        full_text (str): The combined paragraph text.
        replacements (dict): Mapping of placeholders → replacement values.

    Returns:
        List[Tuple[str, bool, Optional[str]]]: A list of text segments.
    """

    if not full_text or not replacements:
        return [(full_text, False, None)]

    segments = []
    current_pos = 0

    # Match all placeholders using regex
    pattern = "(" + "|".join(map(re.escape, replacements.keys())) + ")"

    for match in re.finditer(pattern, full_text):
        start, end = match.start(), match.end()
        placeholder = match.group()

        # Text before match
        if current_pos < start:
            segments.append((full_text[current_pos:start], False, None))

        # Replacement or original placeholder
        replacement_value = replacements[placeholder]
        if should_replace(placeholder, replacement_value):
            segments.append((replacement_value, True, placeholder))
        else:
            segments.append((placeholder, False, None))

        current_pos = end

    # Remaining text
    if current_pos < len(full_text):
        segments.append((full_text[current_pos:], False, None))

    return segments


def add_segment_run(paragraph, text, is_highlighted, placeholder, segment_formatting, highlight_color):
    """
    Adds a text segment to the paragraph, preserving character formatting.

    Args:
        paragraph (docx.text.paragraph.Paragraph): Target paragraph.
        text (str): The text to insert.
        is_highlighted (bool): Whether to apply highlight to the segment.
        placeholder (str or None): Placeholder being replaced (for reference).
        segment_formatting (List[Dict]): Formatting info for each char.
        highlight_color (str): Word highlight color name (e.g., 'green', 'yellow').
    """
    for i, char in enumerate(text):
        run = paragraph.add_run(char)

        # Apply formatting
        fmt = segment_formatting[i] if i < len(segment_formatting) else None
        if fmt:
            run.bold = fmt["bold"]
            run.italic = fmt["italic"]

            # Safely apply underline (can be True, False, or style string)
            underline_value = fmt.get("underline")
            if underline_value is not None:
                run.underline = underline_value

            if run.font:
                run.font.name = fmt["font_name"]
                run.font.size = fmt["font_size"]
                if fmt["font_color"]:
                    run.font.color.rgb = fmt["font_color"]

        # Apply highlight
        if is_highlighted:
            highlight_text(run, highlight_color)
        elif fmt and fmt.get("highlight_color"):
            highlight_text(run, fmt["highlight_color"])


def replace_and_highlight_paragraph(paragraph, replacements, highlight_color=HIGHLIGHT_COLOR):
    """
    Replaces placeholders in a paragraph with replacement values while:
    - Preserving character-level formatting
    - Highlighting replacements
    - Handling placeholders inside hyperlinks

    Args:
        paragraph: The docx paragraph to modify.
        replacements: Dict of placeholder → replacement value.
        highlight_color: Color name (string) for highlighting replaced text.
    """
    if not paragraph or not hasattr(paragraph, '_p'):
        logger.info("Invalid paragraph or missing XML.")
        return

    if not replacements or not isinstance(replacements, dict):
        logger.info("No valid replacements dictionary provided.")
        return

    has_text = any(run.text for run in paragraph.runs) or any(
        el.tag.endswith('hyperlink') for el in paragraph._p
    )
    if not has_text:
        return

    # Step 1: Replace placeholders inside hyperlinks
    handle_hyperlink_placeholders(paragraph, replacements, highlight_color)

    # Step 2: Flatten normal text and extract formatting
    normal_runs, full_text, original_char_formatting = extract_and_flatten_runs(paragraph)

    if not full_text.strip():
        return

    # Step 3: Build replacement segments (e.g. with highlighting info)
    segments = build_segments(full_text, replacements)
    if not any(seg[1] for seg in segments):  # No replacements
        return

    # Step 4: Clear old runs
    for run in normal_runs:
        try:
            run.clear()
        except Exception as e:
            logger.warning(f"Failed to clear run: {e}")

    # Step 5: Rebuild paragraph with replacements and formatting
    rebuild_paragraph_runs(paragraph, segments, original_char_formatting, highlight_color)


def handle_hyperlink_placeholders(paragraph, replacements, highlight_color):
    """
    Scans and replaces placeholders inside hyperlink elements in a Word paragraph.

    This function ensures that placeholders embedded within hyperlinks (<w:hyperlink> tags)
    are correctly replaced with their corresponding values while preserving:
        - The original hyperlink structure and target (r:id or anchor)
        - Inline formatting and XML structure
        - Visual highlighting of the replaced value (for visibility or validation)

    ### How it works:

    1. **XPath and local-name() Usage**:
        - The function uses the XPath expression:
              './/*[local-name()="hyperlink"]'
        - This expression selects all `<w:hyperlink>` elements in the paragraph,
          regardless of their namespace (typically `w:` in WordprocessingML).
        - `local-name()` is a standard XPath function that extracts the tag name
          without the namespace prefix — making the query robust against variations
          in namespace declarations or parsing contexts.
        - This does **not** access the local file system — it's purely XML-based
          and operates in memory.

    2. **Text Replacement**:
        - For each `<w:hyperlink>` element, the function looks inside its child
          `<w:r>` (run) elements to find `<w:t>` (text) nodes.
        - If a placeholder (e.g., "<Document_Name>") exists in the text node,
          and its replacement value passes the `should_replace()` filter,
          the placeholder is replaced directly within the `<w:t>` element.

    3. **Highlighting Replaced Text**:
        - After replacing a placeholder with its value, the function calls
          `highlight_subtext_in_hyperlink()` to visually highlight the inserted
          text (e.g., using green or yellow background).
        - This highlighting works at the XML level and does not break the hyperlink.

    ### Parameters:
        paragraph (docx.text.paragraph.Paragraph):
            The Word paragraph object containing text and hyperlinks.

        replacements (dict):
            A dictionary mapping placeholder strings (e.g., "<Name>") to
            replacement values (e.g., "Alice").

        highlight_color (str):
            A Word-supported color name (e.g., "green", "yellow") used to
            highlight the replaced text inside the hyperlink.

    ### Notes:
        - If multiple placeholders exist inside the same hyperlink, only the
          first matching one is replaced due to the `break` logic.
        - The function only modifies hyperlinks containing placeholders;
          other hyperlinks are left untouched.
        - Hyperlinks without text nodes (`<w:t>`) or invalid structure are skipped silently.

    """

    for hyperlink_elem in paragraph._p.xpath('.//*[local-name()="hyperlink"]'):
        for run in hyperlink_elem.xpath('.//*[local-name()="r"]'):
            t_elems = run.xpath('.//*[local-name()="t"]')
            for t_elem in t_elems:
                if t_elem is None or not t_elem.text:
                    continue

                original_text = t_elem.text

                for placeholder, value in replacements.items():
                    if not should_replace(placeholder, value):
                        continue

                    if placeholder in original_text:
                        before, match, after = original_text.partition(placeholder)
                        t_elem.text = before + value + after

                        highlight_subtext_in_hyperlink(hyperlink_elem, value, highlight_color)
                        break

def extract_and_flatten_runs(paragraph):
    """
    Flattens all non-hyperlink runs into a single string and tracks
    the formatting of each character.

    Returns:
        - list of runs to clear later
        - full text string
        - list of (char, formatting_dict)
    """
    full_text = ''
    char_formatting = []

    normal_runs = [
        run for run in paragraph.runs
        if not run_in_hyperlink(run) and run._element.get(qn('w:dummy')) != 'true'
    ]

    for run in normal_runs:
        if not run.text:
            continue

        formatting = {
            "bold": run.bold,
            "italic": run.italic or (run.font.italic if run.font else False),
            "underline": run.underline,
            "font_name": run.font.name if run.font else None,
            "font_size": run.font.size if run.font else None,
            "font_color": run.font.color.rgb if run.font and run.font.color and run.font.color.rgb else None,
            "highlight_color": run.font.highlight_color if run.font and hasattr(run.font, 'highlight_color') else None
        }

        for char in run.text:
            full_text += char
            char_formatting.append((char, formatting))

    return normal_runs, full_text, char_formatting


def rebuild_paragraph_runs(paragraph, segments, original_char_formatting, highlight_color):
    """
    Adds new runs to the paragraph based on segments with formatting.

    Args:
        paragraph: docx paragraph to rebuild
        segments: list of (text, is_highlighted, placeholder)
        original_char_formatting: list of (char, formatting_dict)
        highlight_color: highlight color to apply to replacements
    """
    pos = 0
    for text, is_highlighted, placeholder in segments:
        if not text:
            continue

        segment_formatting = []

        if placeholder:
            # Find position of the original placeholder in the flattened text
            placeholder_pos = ''.join(c for c, _ in original_char_formatting).find(placeholder, pos)
            fmt_sample = original_char_formatting[placeholder_pos][1] if placeholder_pos != -1 else None
            segment_formatting = [fmt_sample] * len(text)
            pos += len(placeholder)
        else:
            for _ in text:
                if pos < len(original_char_formatting):
                    _, fmt = original_char_formatting[pos]
                    segment_formatting.append(fmt)
                    pos += 1
                else:
                    segment_formatting.append(None)

        add_segment_run(paragraph, text, is_highlighted, placeholder, segment_formatting, highlight_color)


########################### GENAIOPSIT-56: formatting fix: END ###########################

########################### Highlight text: START ###########################
# Commented for hyperlink
# def highlight_text(run, color=HIGHLIGHT_COLOR):
#     """
#     Highlights the given run with the specified color.
#     Valid colors: yellow, green, cyan, magenta, red, etc.

#     Args:
#         run (python-docx.text.Run): The run object to be highlighted.
#         color (str, optional): The color to use for highlighting. Defaults to HIGHLIGHT_COLOR.

#     Returns:
#         None
#     """
#     highlight = OxmlElement('w:highlight')
#     highlight.set(qn('w:val'), color)
#     run._element.get_or_add_rPr().append(highlight)

# Commented for hyperlink
# def should_replace(placeholder, value):
#     """
#     Determines whether a placeholder should be replaced.
    
#     Args:
#         placeholder (str): The placeholder string.
#         value (str): The value to potentially replace the placeholder with.
        
#     Returns:
#         bool: True if the placeholder should be replaced, False otherwise.
        
#     The function excludes empty or 'unable' values from being used as replacements.
#     """
    
#     # Check if both placeholder and value are strings
#     is_placeholder_str = isinstance(placeholder, str)
#     is_value_str = isinstance(value, str)
#     keywords = ["unable", "contains", "search"]
    
#     # Check if value is non-empty and doesn't contain 'unable' (case-insensitive)
#     value_valid = value.strip() and all(word.lower() not in value.lower() for word in keywords) 
    
#     # Replace placeholder only if all conditions are met
#     return is_placeholder_str and is_value_str and value_valid


# Commented for hyperlink.
# def get_full_text(paragraph):
#     """
#     Concatenates and returns all run texts from the paragraph.
#     """
#     # logger.info("Collecting full paragraph text from runs.")
#     return ''.join(run.text for run in paragraph.runs if run.text)


# Commented for hyperlink
# def build_segments(full_text, replacements):
#     """
#     Constructs a list of segments:
#     Each segment is a tuple (text, is_highlighted, original_placeholder).

#     This allows us to recreate the paragraph with highlights applied only
#     to replacement values.

#     Args:
#         full_text (str): The original text.
#         replacements (dict): A dictionary mapping placeholders to replacement values.

#     Returns:
#         list: A list of segments, where each segment is a tuple (text, is_highlighted, original_placeholder).
#     """
#     logger.info("Building segments for replacement and highlight.")
#     segments = []
#     idx = 0
#     while idx < len(full_text):
#         matched = False
#         # Try longer placeholders first to prevent partial matches
#         for placeholder, value in sorted(replacements.items(), key=lambda x: -len(x[0])):
#             if should_replace(placeholder, value) and full_text.startswith(placeholder, idx):
#                 # Replace the placeholder with the value and mark it as highlighted
#                 segments.append((value, True, placeholder))
#                 idx += len(placeholder)
#                 matched = True
#                 break
#         if not matched:
#             # No placeholder matched, add the character as is
#             segments.append((full_text[idx], False, None))
#             idx += 1
#     return segments


# Commented for hyperlink
# def clear_paragraph_runs(paragraph):
#     """
#     Safely clears all runs from the given paragraph.

#     Args:
#         paragraph (Paragraph): The paragraph object from which to clear runs.

#     Returns:
#         None

#     """
#     logger.info("Clearing original runs in paragraph.")
#     for run in paragraph.runs:
#         try:
#             # Clear the run
#             run.clear()
#         except Exception as e:
#             # Log any exceptions that occur during the clearing process
#             logger.warning(f"Failed to clear run: {e}")


# Commented for hyperlink
# def add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color):
#     """
#     Adds a segment of text to the paragraph, optionally with highlight
#     and formatting copied from the original placeholder run.

#     Args:
#         paragraph (docx.text.paragraph.Paragraph): The paragraph object to add the text to.
#         text (str): The text to be added to the paragraph.
#         is_highlighted (bool): Whether the text should be highlighted or not.
#         placeholder (str, optional): The placeholder text in the original document.
#         original_runs (list): A list of tuples containing the original text and run objects.
#         highlight_color (str): The color to highlight the text with (e.g., 'yellow', 'green').

#     Returns:
#         None
#     """
#     try:
#         # Add the text to the paragraph as a new run
#         run = paragraph.add_run(text)
#         # logger.info(f"Added run: '{text}' (highlight={is_highlighted})")
#     except Exception as e:
#         logger.error(f"Failed to add run: {e}")
#         return

#     # Clone formatting from original placeholder run, if available
#     if placeholder:
#         for orig_text, orig_run in original_runs:
#             if placeholder in orig_text:
#                 try:
#                     # Clone the formatting from the original run to the new run
#                     clone_run_format(orig_run, run)
#                     # logger.info(f"Formatting cloned from placeholder '{placeholder}'")
#                 except Exception as e:
#                     logger.warning(f"Error cloning format: {e}")
#                 break

#     # Highlight if needed
#     if is_highlighted:
#         try:
#             # Apply highlighting to the run with the specified color
#             highlight_text(run, highlight_color)
#             # logger.info(f"Applied highlight to '{text}'")
#         except Exception as e:
#             logger.warning(f"Highlighting failed for text: '{text}': {e}")

# Commented for hyperlink
# def replace_and_highlight_paragraph(paragraph, replacements, highlight_color=HIGHLIGHT_COLOR):
#     """
#     Replaces placeholders in the paragraph with their values and highlights them.
#     Handles multiple placeholders and preserves formatting.

#     Args:
#         paragraph (Paragraph): The paragraph object to modify.
#         replacements (dict): A dictionary containing placeholders as keys and their values as values.
#         highlight_color (str): The color to use for highlighting the replaced values (default: HIGHLIGHT_COLOR).

#     Returns:
#         None
#     """
#     # logger.info("replace_and_highlight_paragraph START")

#     # Check if the paragraph is valid and has runs
#     if not paragraph or not hasattr(paragraph, 'runs') or not paragraph.runs:
#         logger.info("No valid runs found in paragraph.")
#         return

#     # Check if the replacements dictionary is valid
#     if not replacements or not isinstance(replacements, dict):
#         logger.info("No valid replacements dictionary provided.")
#         return

#     # Save original run texts and objects to copy formatting later
#     original_runs = [(run.text, run) for run in paragraph.runs if run.text]
#     full_text = get_full_text(paragraph)

#     # Check if the paragraph text is empty
#     if not full_text.strip():
#         logger.info("Paragraph text is empty.")
#         return

#     # Build segments with placeholders and their values
#     segments = build_segments(full_text, replacements)

#     # Check if there are any valid matches
#     if not any(seg[1] for seg in segments):  # No valid matches
#         # logger.info("No replacements matched in this paragraph.")
#         return

#     # Clear the existing runs in the paragraph
#     clear_paragraph_runs(paragraph)

#     # Add the segments to the paragraph with or without highlighting
#     for text, is_highlighted, placeholder in segments:
#         add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color)

#     # logger.info("replace_and_highlight_paragraph END")


###################### Hyperlink implementation START ####################

# GENAIOPSIT-56: Commented out for formatting fix.
# def add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color):
#     """
#     Adds a segment of text to the paragraph, optionally with highlight
#     and formatting copied from the original placeholder run.
#     Preserves hyperlink if the placeholder run was inside one.

#     Args:
#         paragraph (docx.text.paragraph.Paragraph): The paragraph object to add the text to.
#         text (str): The text to be added to the paragraph.
#         is_highlighted (bool): Whether the text should be highlighted or not.
#         placeholder (str, optional): The placeholder text in the original document.
#         original_runs (list): A list of tuples containing the original text and run objects.
#         highlight_color (str): The color to highlight the text with (e.g., 'yellow', 'green').

#     Returns:
#         None
#     """
#     run_to_clone = None
#     r_id = None

#     if placeholder:
#         for orig_text, orig_run in original_runs:
#             if placeholder in orig_text:
#                 run_to_clone = orig_run
#                 try:
#                     if run_in_hyperlink(orig_run):
#                         r_id = get_hyperlink_rid(orig_run)
#                 except Exception as e:
#                     logger.warning(f"Failed to detect hyperlink on run: {e}")
#                 break

#     try:
#         if r_id:
#             # Add new run inside hyperlink
#             run = add_hyperlink_run(paragraph, text, r_id)
#         else:
#             run = paragraph.add_run(text)
#         # logger.info(f"Added run: '{text}' (highlight={is_highlighted})")
#     except Exception as e:
#         logger.error(f"Failed to add run: {e}")
#         return

#     # Clone formatting
#     if run_to_clone:
#         try:
#             clone_run_format(run_to_clone, run)
#             # logger.info(f"Cloned formatting from run for placeholder: {placeholder}")
#         except Exception as e:
#             logger.warning(f"Error cloning format: {e}")

#     # Highlight if needed
#     if is_highlighted:
#         try:
#             highlight_text(run, highlight_color)
#             # logger.info(f"Applied highlight to '{text}'")
#         except Exception as e:
#             logger.warning(f"Highlighting failed for text: '{text}': {e}")


# GENAIOPSIT-56: TODO: Remove
# def clone_run_format(source_run: Run, new_run: Run):
#     """
#     Copies basic formatting from source_run to new_run.

#     Args:
#         source_run (Run): The Run object from which formatting will be copied.
#         new_run (Run): The Run object to which formatting will be applied.

#     """
#     # logger.info("Cloning formatting from source run to new run.")
#     try:
#         # Check if both Run objects are valid
#         if not source_run or not new_run:
#             return

#         # Copy basic formatting properties
#         new_run.bold = source_run.bold
#         new_run.italic = source_run.italic
#         new_run.underline = source_run.underline

#         # Check if font properties exist for both Run objects
#         if hasattr(source_run, "font") and hasattr(new_run, "font"):
#             # Copy font name if present in source_run
#             if source_run.font.name:
#                 new_run.font.name = source_run.font.name

#             # Copy font size if present in source_run
#             if source_run.font.size:
#                 new_run.font.size = source_run.font.size

#             # Copy font color if present in source_run
#             if source_run.font.color and source_run.font.color.rgb:
#                 new_run.font.color.rgb = source_run.font.color.rgb

#     except Exception as e:
#         # Log any exceptions that occur during the formatting process
#         logger.warning(f"Error in clone_run_format: {e}")


# GENAIOPSIT-56: Commented out for formatting fix.
# def highlight_text(run, color=HIGHLIGHT_COLOR):
#     """
#     Highlights the given run with the specified color.
#     Valid colors: yellow, green, cyan, magenta, red, etc.

#     Args:
#         run (python-docx.text.Run): The run object to be highlighted.
#         color (str, optional): The color to use for highlighting. Defaults to HIGHLIGHT_COLOR.

#     Returns:
#         None
#     """
#     highlight = OxmlElement('w:highlight')
#     highlight.set(qn('w:val'), color)
#     run._element.get_or_add_rPr().append(highlight)

def should_replace(placeholder, value):
    """
    Determines whether a placeholder should be replaced.
    
    Args:
        placeholder (str): The placeholder string.
        value (str): The value to potentially replace the placeholder with.
        
    Returns:
        bool: True if the placeholder should be replaced, False otherwise.
        
    The function excludes empty or 'unable' values from being used as replacements.
    """
    
    # Check if both placeholder and value are strings
    is_placeholder_str = isinstance(placeholder, str)
    is_value_str = isinstance(value, str)
    keywords = ["unable", "contains", "search"]
    
    # Check if value is non-empty and doesn't contain 'unable' (case-insensitive)
    value_valid = value.strip() and all(word.lower() not in value.lower() for word in keywords) 
    
    # Replace placeholder only if all conditions are met
    return is_placeholder_str and is_value_str and value_valid


# GENAIOPSIT-56: TODO: Remove
# def get_full_text(paragraph):
#     """
#     Concatenates and returns all run texts from the paragraph.
#     """
#     # logger.info("Collecting full paragraph text from runs.")
#     return ''.join(run.text for run in paragraph.runs if run.text)


# GENAIOPSIT-56: Commented out for formatting fix.
# def build_segments(full_text, replacements):
#     """
#     Constructs a list of segments:
#     Each segment is a tuple (text, is_highlighted, original_placeholder).

#     This allows us to recreate the paragraph with highlights applied only
#     to replacement values.

#     Args:
#         full_text (str): The original text.
#         replacements (dict): A dictionary mapping placeholders to replacement values.

#     Returns:
#         list: A list of segments, where each segment is a tuple (text, is_highlighted, original_placeholder).
#     """
#     logger.info("Building segments for replacement and highlight.")
#     segments = []
#     idx = 0
#     while idx < len(full_text):
#         matched = False
#         # Try longer placeholders first to prevent partial matches
#         for placeholder, value in sorted(replacements.items(), key=lambda x: -len(x[0])):
#             if should_replace(placeholder, value) and full_text.startswith(placeholder, idx):
#                 # Replace the placeholder with the value and mark it as highlighted
#                 segments.append((value, True, placeholder))
#                 idx += len(placeholder)
#                 matched = True
#                 break
#         if not matched:
#             # No placeholder matched, add the character as is
#             segments.append((full_text[idx], False, None))
#             idx += 1
#     return segments


def clear_paragraph_runs(paragraph):
    """
    Safely clears all runs from the given paragraph.

    Args:
        paragraph (Paragraph): The paragraph object from which to clear runs.

    Returns:
        None

    """
    logger.info("Clearing original runs in paragraph.")
    for run in paragraph.runs:
        try:
            # Clear the run
            run.clear()
        except Exception as e:
            # Log any exceptions that occur during the clearing process
            logger.warning(f"Failed to clear run: {e}")

#####################################################    

def run_in_hyperlink(run):
    """
    Checks if the given run is inside a hyperlink tag.

    Args:
        run (docx.text.run.Run): The run to check.

    Returns:
        bool: True if inside a hyperlink, False otherwise.
    """
    parent = run._element.getparent()
    return parent.tag.endswith('hyperlink')


def get_hyperlink_rid(run):
    """
    Retrieves the relationship ID (r:id) from the hyperlink containing this run.

    Args:
        run (docx.text.run.Run): The run inside a hyperlink.

    Returns:
        str: The relationship ID if found, else None.
    """
    hyperlink_elem = run._element.getparent()
    return hyperlink_elem.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')


def add_hyperlink_run(paragraph, text, r_id):
    """
    Adds a hyperlink run (w:hyperlink) with given r:id and text to the paragraph XML.
    Returns the hyperlink XML element for further formatting.
    """
    if "toc" in r_id.lower():
        hlink_ = paragraph._p.xpath('.//w:hyperlink')
        print(hlink_)
        if len(hlink_) > 0:
            hlink = OxmlElement('w:hyperlink')
            hlink = hlink_[0]
            print(hlink)
            t_nodes = hlink.xpath('.//w:t')
            t_nodes[0].text = text
            # Clear remaining t_nodes if split across multiple runs
            for t in t_nodes[1:]:
                # print(t.text)
                t.text = ""

                
    else:
        hlink = OxmlElement('w:hyperlink')
        hlink.set(qn('r:id'), r_id)
    
        new_run = OxmlElement('w:r')
        rPr = OxmlElement('w:rPr')  # Run properties container
        new_run.append(rPr)
    
        t = OxmlElement('w:t')
        t.text = text
        new_run.append(t)
    
        hlink.append(new_run)
        paragraph._p.append(hlink)

    return hlink  # return hyperlink XML element, not a Run object

# New one
def highlight_hyperlink_run(hyperlink_elem, highlight_color, text_to_highlight):
    """
    Adds highlight to the <w:rPr> of the first <w:r> inside the hyperlink XML element.
    """
    ns = hyperlink_elem.nsmap
    r_element = hyperlink_elem.find('.//w:r', namespaces=ns)
    if r_element is not None:
        for r in r_element:
            r_wt = r_element.find('.//w:t', namespaces=ns)
            if text_to_highlight in r_wt.text:
                rPr = r.find('w:rPr', namespaces=ns)
                if rPr is None:
                    rPr = OxmlElement('w:rPr')
                    r.insert(0, rPr)
        
                highlight = OxmlElement('w:highlight')
                highlight.set(qn('w:val'), highlight_color)
                rPr.append(highlight)


# GENAIOPSIT-56: Commented out for formatting fix.
# def highlight_subtext_in_hyperlink(hlink, substring, color_hex="00B050"):
#     """
#     Highlight a substring inside a hyperlink in green without losing the link.
#     """
#     ns = hlink.nsmap
#     print(hlink)
#     for r in hlink.xpath('.//w:r'):
#         t = r.find('.//w:t', namespaces=ns)
#         if t is not None and t.text and substring in t.text:
#             before, match, after = t.text.partition(substring)

#             # Update current run to keep "before"
#             t.text = before if before else match

#             # If there is a match and we had a "before" part, create a new run for the match
#             if before and match:
#                 match_run = OxmlElement("w:r")
#                 match_t = OxmlElement("w:t")
#                 match_t.text = match
#                 match_run.append(match_t)

#                 # add formatting <w:rPr><w:color/>
#                 rPr = OxmlElement("w:rPr")
#                 highlight = OxmlElement("w:highlight")
#                 highlight.set(qn("w:val"), color_hex)
#                 rPr.append(highlight)                
#                 match_run.insert(0, rPr)

#                 # insert after the current run
#                 r.addnext(match_run)
#                 r = match_run  # move pointer to new run

#             elif not before:  # substring starts at beginning, format current run
#                 rPr = r.find(qn("w:rPr"))
#                 if rPr is None:
#                     rPr = OxmlElement("w:rPr")
#                     r.insert(0, rPr)
#                 color = rPr.find(qn("w:color"))
#                 if color is None:
#                     color = OxmlElement("w:color")
#                     rPr.append(color)
#                 color.set(qn("w:val"), color_hex)

#             # If there’s text after, create a new run for it
#             if after:
#                 after_run = OxmlElement("w:r")
#                 after_t = OxmlElement("w:t")
#                 after_t.text = after
#                 after_run.append(after_t)
#                 r.addnext(after_run)


# GENAIOPSIT-56: Commented out for formatting fix.
# def replace_and_highlight_paragraph(paragraph, replacements, highlight_color=HIGHLIGHT_COLOR):
#     """
#     Replaces placeholders in a paragraph with values, preserving formatting and hyperlinks.
#     - If a placeholder is inside a hyperlink, it replaces it and retains the hyperlink.
#     - Highlights the replacement text.
#     """
#     if not paragraph or not hasattr(paragraph, '_p'):
#         logger.info("Invalid paragraph or missing XML.")
#         return

#     has_text = any(run.text for run in paragraph.runs) or any(
#         el.tag.endswith('hyperlink') for el in paragraph._p
#     )
#     if not has_text:
#         logger.info("Paragraph has no runs or hyperlinks with text.")
#         return

#     if not replacements or not isinstance(replacements, dict):
#         logger.info("No valid replacements dictionary provided.")
#         return

#     # Step 1: Handle placeholders inside hyperlinks
#     for child in list(paragraph._p):  # Direct access to paragraph XML children
#             # hyperlink_rel_id = hyperlink.get(qn("r:id"))
#         if child.tag.endswith('hyperlink'):
#             hyperlink_text = paragraph.text
#             # for r in child.findall('.//w:r', namespaces=child.nsmap):
#             #     text_elem = r.find('.//w:t', namespaces=child.nsmap)
#             #     if text_elem is not None and text_elem.text:
#             #         hyperlink_text += text_elem.text

#             # Check if any placeholder is inside the full hyperlink text
#             for placeholder, value in replacements.items():
#                 if should_replace(placeholder, value) and placeholder in hyperlink_text:
#                     print(hyperlink_text)
#                     hlink = paragraph._p.xpath('.//w:hyperlink')
#                     if len(hlink) > 0:
#                         hlink = hlink[0]
#                         # r_id = child.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
#                         r_id_ext = hlink.get(qn("r:id"))
#                         r_id_in = hlink.get(qn("w:anchor"))
#                         r_id = r_id_ext if r_id_ext is not None else  r_id_in
#                         # print(r_id)
#                         if not r_id:
#                             logger.info("Hyperlink has no external and internal r:id; skipping.")
#                             break
                        
#                         # Remove the old hyperlink element        
#                         # Insert the new hyperlink with replacement text
#                         new_text = hyperlink_text.replace(placeholder, value)
#                         print(new_text)
#                         new_hyperlink = add_hyperlink_run(paragraph, new_text, r_id)
#                         # highlight_hyperlink_run(new_hyperlink, highlight_color, value)
#                         highlight_subtext_in_hyperlink(new_hyperlink, value, highlight_color)
        
#                         break  # Only handle one placeholder per hyperlink

#     # Step 2: Handle normal (non-hyperlink) runs
#     normal_runs = [
#         run for run in paragraph.runs
#         if not run_in_hyperlink(run) and run._element.get(qn('w:dummy')) != 'true'
#     ]

#     if not normal_runs:
#         return

#     original_runs = [(run.text, run) for run in normal_runs if run.text]
#     full_text = ''.join(run.text for run in normal_runs)

#     if not full_text.strip():
#         return

#     segments = build_segments(full_text, replacements)

#     if not any(seg[1] for seg in segments):  # No replacements found
#         return

#     # Clear normal runs
#     for run in normal_runs:
#         try:
#             run.clear()
#         except Exception as e:
#             logger.warning(f"Failed to clear run: {e}")

#     # Add new segments with formatting/highlighting
#     for text, is_highlighted, placeholder in segments:
#         add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color)

###################### Hyperlink implementation END ######################

# Commented for hyperlink.
# def clone_run_format(source_run: Run, new_run: Run):
#     """
#     Copies basic formatting from source_run to new_run.

#     Args:
#         source_run (Run): The Run object from which formatting will be copied.
#         new_run (Run): The Run object to which formatting will be applied.

#     """
#     # logger.info("Cloning formatting from source run to new run.")
#     try:
#         # Check if both Run objects are valid
#         if not source_run or not new_run:
#             return

#         # Copy basic formatting properties
#         new_run.bold = source_run.bold
#         new_run.italic = source_run.italic
#         new_run.underline = source_run.underline

#         # Check if font properties exist for both Run objects
#         if hasattr(source_run, "font") and hasattr(new_run, "font"):
#             # Copy font name if present in source_run
#             if source_run.font.name:
#                 new_run.font.name = source_run.font.name

#             # Copy font size if present in source_run
#             if source_run.font.size:
#                 new_run.font.size = source_run.font.size

#             # Copy font color if present in source_run
#             if source_run.font.color and source_run.font.color.rgb:
#                 new_run.font.color.rgb = source_run.font.color.rgb

#     except Exception as e:
#         # Log any exceptions that occur during the formatting process
#         logger.warning(f"Error in clone_run_format: {e}")
########################### Highlight text: END ###########################

########################### Site Validation ###########################
def sitev_generate_report_from_template(s3_bucket, template_s3_key, output_s3_key, data=None, dframe_list=[], table_loc_list= []):
    """
    Generates a report from a Word document template stored in S3.

    Args:
        s3_bucket (str): The name of the S3 bucket.
        template_s3_key (str): The S3 key of the Word document template.
        output_s3_key (str): The S3 key where the generated report will be saved.
        data (dict): A dictionary containing the data to be inserted into the template.
    """
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=s3_bucket, Key=template_s3_key)
        template_bytes = response['Body'].read()
        template_file = BytesIO(template_bytes)
        doc = Document_docx(template_file)
    except Exception as e:
        logger.error(f"sitev_generate_report_from_template : Error downloading or opening template: {str(e)}")
    
    tables = doc.tables
    for key, value in data.items():
        # logger.info(f"\nsitev_generate_report_from_template ------ 1 ------placeholder / key: {key}")
        # logger.info(f"sitev_generate_report_from_template ------ 2 ------ key: {value}")
        placeholder = f"{key}"

        ########################## Header and Footer Replacement
        for section in doc.sections:
            header = section.header
            footer = section.footer
            logger.info("=== HEADER Replacement  ===")
            for paragraph in header.paragraphs:
                if any(k in paragraph.text for k in data.keys()):
                    replace_and_highlight_paragraph(paragraph, data)
            for table in header.tables:
                for row in table.rows:
                   for cell in row.cells:
                       for paragraph in cell.paragraphs:
                           if any(k in paragraph.text for k in data.keys()):
                            # logger.info(f"sitev_generate_report_from_template: Processing table cell paragraph: '{paragraph.text[:50]}'")
                               replace_and_highlight_paragraph(paragraph, data)

            logger.info("=== FOOTER Replacement  ===")    
            for paragraph in footer.paragraphs:
                if any(k in paragraph.text for k in data.keys()):
                    replace_and_highlight_paragraph(paragraph, data)
                    
            for table in footer.tables:
                for row in table.rows:
                   for cell in row.cells:
                       for paragraph in cell.paragraphs:
                           if any(k in paragraph.text for k in data.keys()):
                            # logger.info(f"sitev_generate_report_from_template: Processing table cell paragraph: '{paragraph.text[:50]}'")
                               replace_and_highlight_paragraph(paragraph, data)

        ############### Normal Paragraph and Table Replacement
        for paragraph in doc.paragraphs:
            if any(k in paragraph.text for k in data.keys()):
                # logger.info(f"sitev_generate_report_from_template:Processing paragraph: '{paragraph.text[:50]}'")
                replace_and_highlight_paragraph(paragraph, data)

        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    for paragraph in cell.paragraphs:
                        if any(k in paragraph.text for k in data.keys()):
                            # logger.info(f"sitev_generate_report_from_template: Processing table cell paragraph: '{paragraph.text[:50]}'")
                            replace_and_highlight_paragraph(paragraph, data)

        # for paragraph in doc.paragraphs:

        # # Replacing the text
        # # for key, value in data.items():
        #         # Earlier existing code
        #         # if "unable" in value.lower():
        #         #     continue
        #         # else:
        #         #     paragraph.text = paragraph.text.replace(placeholder, value)

        # for table in tables:
        #     logger.info(f"sitev_generate_report_from_template : Iterating tables: ----- 3 -----")
        #     for row in table.rows:
        #       for cell in row.cells:

        #          if placeholder in cell.text:
        #             # Earlier existing code
        #             # if "unable" in value.lower():
        #             #     continue
        #             # else:
        #             #     cell.text = value

    output_file = BytesIO()
    doc.save(output_file)
    output_file.seek(0)

    try:
        s3.put_object(Bucket=s3_bucket, Key=output_s3_key, Body=output_file.read())
        report_s3_uri = f"s3://{s3_bucket}/{output_s3_key}" #create s3 uri
        # logger.info(f"sitev_generate_report_from_template : Report generated and uploaded to \n{report_s3_uri}")
    except Exception as e:
        logger.error(f"sitev_generate_report_from_template : Error Saving the update template: {str(e)}")


def convert_stringtable_to_df(text_content, bucket_name, table_name = "table_4"):
    """
    Convert a text-based table extracted from a PDF into CSV format by first
    converting to a pandas DataFrame for easier manipulation.
    
    Args:
        text_content (str): The raw text extracted from a PDF containing table data
        
    Returns:
        str: CSV formatted string
    """
  
    # Identify the "Key: Value" pattern
    pattern = re.compile(r"([^:]+):\s*(.*)")

    lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        
    dict_entry = {}
    for line in lines:
        match = pattern.match(line)
        if match:
            key = match.group(1).strip()
            value = match.group(2).strip()
            if key  in dict_entry:
                dict_entry[key].append(value)
            else:
                dict_entry[key] = [value]


    # Keys to include in the subset
    if table_name == "table_4":
       keys = ["Parameter Classification", "Parameter Description", "Validation Criteria"]
       subset_dict = {k: dict_entry[k] for k in keys if k in dict_entry}        
       df = pd.DataFrame.from_dict(subset_dict)
    else:
       df = pd.DataFrame.from_dict(dict_entry, orient='columns')
    
    return df


################### report generation for template name Inoculum_Expansion
def sitev_generation(report_id, file_version, user_id, session_id, timestamp, table_template_master, knowledge_base_name, model_id, template_fullname, bucket_name,  output_folder, source_file_names, source_file_types, kbname_max_size, name, upload_folder):
    logger.info(f"\nsitev_generation      START : source_file_names: \n{source_file_names}")
    template_id, docx_s3_key, excel_s3_key, section_name_list, template_name, doc_typ_list, doc_typ_full_list = sitev_extract_template(table_template_master, template_fullname)
    # data = get_mapping_list_sv(bucket_name, excel_s3_key, "Site_Validation", knowledge_base_name, model_id, kbname_max_size, report_id)
    data, figures, tables = get_mapping_list_sv(bucket_name, excel_s3_key, "Site_Validation", knowledge_base_name, model_id, kbname_max_size, report_id, source_file_names, source_file_types, upload_folder, table_template_master, template_fullname)

    try:
            # docx_s3_key, excel_s3_key, section_name_list = sitev_extract_template(TEMPLATE_DB, template_name)
            ####################### Placeholder to insert the code to fill the docx template #######################
            unique_str = ''.join(random.choice(string.ascii_lowercase) for i in range(6)) 
            logger.info(f"sitev_generation      unique_str : {unique_str}")
            ispr_filename = "sitev" + "-"  + template_name + "-"  + user_id + "-"  + unique_str + ".docx"
            target_filename = f"{output_folder}{ispr_filename}"
            # sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data)
            deviation_change_values = None
            table_ins = False
            figure_ins = False
            if tables and (SOURCE_TYPE_TABLE_NAME in source_file_types):
                xls_table_filename = source_file_names[source_file_types.index(SOURCE_TYPE_TABLE_NAME)]
                xls_table_s3_key = f"{upload_folder}{xls_table_filename}"
                deviation_change_values = sitev_insert_table_in_report(bucket_name, tables, target_filename, xls_table_s3_key, docx_s3_key)
                table_ins = True
                # if deviation_change_values:
                #     ispr_filename_out = "sitev" + "-"  + template_name + "-"  + user_id + "-"  + unique_str + "dc" + ".docx"
                #     target_filename_out = f"{output_folder}{ispr_filename_out}"
                #     sitev_generate_report_from_template(bucket_name, target_filename,  target_filename_out, deviation_change_values)

            if figures:
                if table_ins:
                    sitev_insert_figure_in_report(bucket_name, figures, target_filename, target_filename)
                else:
                    sitev_insert_figure_in_report(bucket_name, figures, target_filename, docx_s3_key)
                figure_ins = True
                    
                    
            if data:
                if figure_ins or table_ins:
                    input_filename = target_filename
                    output_filename = target_filename
                else:
                    input_filename = docx_s3_key
                    output_filename = target_filename
                    
                if deviation_change_values:
                    data.update(deviation_change_values)                
                sitev_generate_report_from_template(bucket_name, input_filename, output_filename, data=data)
                # elif tables:
                #     sitev_generate_report_from_template(bucket_name, input_filename, output_filename, data=data)
               
                
                    
                
                # Use the new S3-native populate_tables function here
                # logger.info("Invoking populate_tables ----------------->")
                # populate_tables(
                #     bucket_name=bucket_name,
                #     rules_s3_key=excel_s3_key,
                #     source_xls_s3_key=xls_table_s3_key,
                #     target_docx_s3_key=target_filename,
                #     tables_mapping=tables
                # )
            ##########################################################################
            response = s3.get_object(Bucket=bucket_name, Key=target_filename)
            template_bytes = response['Body'].read()
            chunk_text = extract_text_from_word(template_bytes)
            chunk_report_folder = "chunk_report_gen"
            chunk_report(bucket_name, ispr_filename,  chunk_text, 0, "all_sections", data["<Molecule_Name>"], "2024_2025", template_fullname,  report_id, timestamp, user_id, chunk_report_folder)
            #######################################################################################################
            DetailCompletionStatus = []
            logger.info(f"sitev_generation:Successfully saved INEX document to S3: {target_filename}")
    except Exception as e:
            # delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
            logger.error(f"sitev_generation: Error saving INEX document to S3: {str(e)}")
            raise
    
    for section_index, section_name in enumerate(section_name_list):
        DetailCompletionStatus.append(ReportTrackingSection(section_name=section_name_list[section_index], completion=0))
    
    ReportTrackingRes = ReportTrackingCompletion(
             report_id = report_id,
             created_by = user_id,
             name = name,
             session_id = session_id,
             created_at = timestamp,
             file_version = file_version,
             report_file_path = target_filename,
             template_name = template_name,
             template_fullname = template_fullname,
             product_name = data["<Molecule_Name>"],  ############ Or Molecule Name 
             pqr_param_json_filename = "",
             reporting_period = "",
             edit_status = "Ready For review",
             completion_detail_section=DetailCompletionStatus,
             completion = 0,
             updated_at = "",
             edited_by = "",
             locked = False,
             source_file_names = source_file_names,
             site_names = source_file_types,
             report_title = template_name + "_" + data["<Molecule_Name>"] + "_" + "v" + str(file_version),
             html_content = "<html></html>"
        )
        
    store_reporttrackingcompletion(ReportTrackingRes, REPORTS_DYNAMOTABLE)
    logger.info(f"\nsitev_generation      END")

def ispr_generation(user_id, session_id, timestamp, model_id, bucket_name, mapping_file_s3key, mapping_file_sheet_name, upload_folder, output_folder,  model_max_tokens, pqr_param_json_filename, report_id, file_version, template_name, template_fullname, name):
    """
    This function generates an ISPR (Integrated Site Periodic Report) document based on the provided input data.

    Functionality Steps:
    1. Retrieve the product name, reporting period, site names, PQR file names, and list of versions from the context.
    2. Set up the LLM (Large Language Model) with the specified model ID and model parameters.
    3. Generate a unique filename for the ISPR document.
    4. Retrieve the mapping dictionary containing section names, flags, and prompts from an Excel file in S3.
    5. Load the JSON data containing the text, images, and tables for each section and site.
    6. Iterate through each section:
        a. Collect the text, images, and tables for the current section and site.
        b. If the section requires summarization, generate a summary using the LLM and the specified prompts.
        c. Add the summarized text to the ISPR document, or add the individual site-specific text, images, and tables.
    7. Save the ISPR document as a Word file (.docx) in the specified S3 output folder.
    8. Create a ReportTrackingCompletion object with the necessary details and store it in the DynamoDB table.

    The generated JSON file contains the following information:
    - report_id: Unique identifier for the report.
    - created_by: User ID of the user who created the report.
    - name: Name associated with the report.
    - session_id: Session ID associated with the report generation.
    - created_at: Timestamp when the report was created.
    - file_version: Version of the report file.
    - report_file_path: S3 path of the generated ISPR document.
    - template_fullname: Full name of the template used for the report.
    - template_name: Name of the template used for the report.
    - product_name: Name of the product or molecule.
    - pqr_param_json_filename: Filename of the PQR parameter JSON file.
    - reporting_period: Reporting period for the ISPR.
    - edit_status: Status of the report (e.g., "Ready For review").
    - completion_detail_section: Details about the completion status of each section.
    - completion: Overall completion status of the report generation.
    - updated_at: Timestamp when the report was last updated (initially empty).
    - edited_by: User ID of the user who last edited the report (initially empty).
    - locked: Flag indicating whether the report is locked for editing (initially False).
    - source_file_names: List of source file names used for generating the report.
    - site_names: List of site names included in the report.
    - html_content: HTML content of the report (initially an empty HTML tag).
    """    
    product_name, reporting_period, site_names, pqr_file_names, list_versions = retrieve_context(bucket_name, output_folder, pqr_param_json_filename)
    
    ############################ Setting the llm model ############################################
    model_kwargs = {
            "temperature": 0,
            "top_k": 250,
            "top_p": 0,
            "stop_sequences": ["\n\nHuman"],
            # "max_tokens_to_sample": 2048,
            # "prompt": enclosed_prompt
        }
    bedrock_runtime = boto3.client(service_name="bedrock-runtime")
    llm = ChatBedrock(
            client=bedrock_runtime,
            model_id=model_id,
            # temperature=0,
            model_kwargs=model_kwargs,
            # temperature=0,
            # streaming=True,
            # api_key=open_ai_key
        )
    ###############################################################################################
    
     # ispr_filename = "ispr" + "_" + product_name  + "_" + reporting_period + ".docx"
    try:
        
        unique_str = ''.join(random.choice(string.ascii_lowercase) for i in range(6)) 
        ispr_filename = "ispr" + "-" + product_name + "-" + reporting_period + "-" + user_id + "-" + unique_str + ".docx"
    
        logger.info(f"ispr_generation: 1 Starting ISPR generation for product: {product_name}, period: {reporting_period}")
        
        try:
            mapping_dic = get_mapping_list(bucket_name, excel_file_path=f"{mapping_file_s3key}",
                                           az_mapping_sheet_name=f"{mapping_file_sheet_name}")
            logger.info("ispr_generation: 2 Successfully retrieved mapping dictionary")
        except Exception as e:
            delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
            logger.error(f"ispr_generation: 3 Error retrieving mapping dictionary: {str(e)}")
            raise

        # Extract necessary data from mapping_dic
        section_names_keysearch = mapping_dic["section_names_keysearch"]
        section_names_ispr = mapping_dic["section_names_ispr"]
        ispr_summary_flag = mapping_dic["ispr_summary_flag"]
        ispr_map_prompt_templates = mapping_dic["ispr_map_prompt_ls"]
        ispr_combine_prompt_templates = mapping_dic["ispr_combine_prompt_ls"]

        try:
            ispr_json_filename = "ispr" + "-" + product_name + "-" + reporting_period + "-" + user_id + ".json"
            output_path_json = f"{output_folder}{ispr_json_filename}"
            json_obj = s3.get_object(Bucket=bucket_name, Key=output_path_json)
            json_data = json_obj['Body'].read().decode('utf-8')
            df = pd.DataFrame(json.loads(json_data))
            logger.info(f"ispr_generation: 4 Successfully loaded JSON data from S3: {output_path_json}")
        except Exception as e:
            delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
            logger.error(f"ispr_generation: 5 Error loading JSON data from S3: {str(e)}")
            raise

        document = Document_docx()
        DetailCompletionStatus = []

        for section_index, section_name in enumerate(section_names_keysearch):
            logger.info(f"ispr_generation: 6 Iterating section names: {section_name}")
            
            Subsection = []
            i = 1
            document.add_heading(section_names_ispr[section_index], i)
            i = i + 1

            #### Collecting the text, images and tables present in a specific section
            docs = []
            images_section_sites = []
            tables_section_sites = []

            try:
                logger.info(f"ispr_generation: 7 Processing section: {section_name}")

                for site_name in site_names:
                    logger.info(f"ispr_generation: 8 section_name: Processing site: {section_name}:{site_name}")
                    logger.info(f"ispr_generation: 8.01 shape: {df.shape}")
                    logger.info(f"ispr_generation: 8.02 columns: {df.columns}")
                    logger.info(f"ispr_generation: 8.03 head: {df.head()}")

                    # Subsetting specific rows and columns by labels
                    subset = df.loc[
                        (df["section_name"] == section_name.lower())
                        & (df["site_name"] == site_name.lower())
                        & (df["product_name"] == product_name),
                        ["page_num", "images", "site_name", "tables", "section_name", "file_name", "text"],
                    ]
                    logger.info("ispr_generation: 8.1")

                    if not subset.empty:
                        logger.info("ispr_generation: 8.2")
                        max_row = subset.loc[subset["page_num"].idxmax()]

                        logger.info("ispr_generation: 8.3")
                        if ispr_summary_flag[section_index] == 1:
                            text = max_row["text"]
                            for i in range(0, len(text), model_max_tokens):
                                chunk = text[i:i + model_max_tokens]
                                doc = Document(
                                    page_content=chunk,
                                    # metadata=result["matches"][j]["metadata"],
                                )
                                docs.append(doc)
                            logger.info("ispr_generation: 8.31")
                            images_section_sites.append(max_row["images"])
                            logger.info("ispr_generation: 8.32")
                            tables_section_sites.append(max_row["tables"])

                        else:
                            logger.info("ispr_generation: 8.4")
                            heading_name = section_name + "_" + site_name
                            # logger.info(f"Processing heading_name: {heading_name}")
                            document.add_heading(heading_name, i)
                            document.add_paragraph(max_row["text"])
                            logger.info("ispr_generation: 8.5")

                        # Add images
                        #for image_path in max_row["images"]:
                        #    if len(image_path) > 0:
                        #        img_stream = io.BytesIO()
                        #        s3.download_fileobj(bucket_name, image_path, img_stream)
                        #        img_stream.seek(0)
                        #        document.add_picture(
                        #            img_stream, width=Inches(4)
                        #        )  # Add image with a fixed width

                        # Add tables
                        #for table_path in max_row["tables"]:
                        #    if len(table_path) > 0:
                        #        csv_stream = io.BytesIO()
                        #        s3.download_fileobj(bucket_name, table_path, csv_stream)
                        #        csv_stream.seek(0)
                        #        # Read the CSV content
                        #        csv_data = list(csv.reader(io.StringIO(csv_stream.read().decode('utf-8'))))
                        #        # Create a new table in Word
                        #        rows = len(csv_data)
                        #        cols = len(csv_data[0])
                        #        word_table = document.add_table(rows=rows, cols=cols)
                        #        # Populate the Word table
                        #        for row_index, row in enumerate(csv_data):
                        #            for col_index, cell in enumerate(row):
                        #                word_table.cell(row_index, col_index).text = str(cell)
                       
                            Subsection.append(IsprTrackingSubSection(SectionName=section_names_ispr[section_index] + "_" + site_name, SectionText=max_row["text"], 
                                                               SectionTables=max_row["tables"], SectionImages= max_row["images"]))
                        logger.info("ispr_generation: 8.6")
                    else:
                        logger.info("ispr_generation: 8.7")
                        continue
                            
                if ispr_summary_flag[section_index] == 1:
                    logger.info(f"ispr_generation: 9 Generating summary for section: {section_name} - site name: {site_name}")
                    summary_text = get_abs_summarize(llm, docs, ispr_map_prompt_templates[section_index], ispr_combine_prompt_templates[section_index], site_name)
                    document.add_paragraph(summary_text['output_text'])
                    chunk_report_folder = "chunk_report_gen"
                    chunk_report(bucket_name, ispr_filename, summary_text['output_text'], section_index, section_name, product_name, reporting_period, template_fullname, report_id, timestamp, user_id, chunk_report_folder)
                    DetailCompletionStatus.append(ReportTrackingSection(section_name=section_names_ispr[section_index], section_text=summary_text['output_text'], is_feedback_positive=None, completion=0))
                else:
                    logger.info(f"ispr_generation: 10 ELSE Condition: Generating summary for section: {section_name}")
                    DetailCompletionStatus.append(ReportTrackingSection(section_name=section_names_ispr[section_index], subsection=Subsection, is_feedback_positive=None, completionStatus=0))
                    
            except Exception as e:
                delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
                logger.error(f"ispr_generation: 11 Error processing section {section_name}: \nDetailed: {str(e)}")
                # Decide whether to continue with other sections or raise the exception

        try:
            target_filename = f"{output_folder}{ispr_filename}"
            doc_stream = io.BytesIO()
            document.save(doc_stream)
            doc_stream.seek(0)
            s3.upload_fileobj(doc_stream, bucket_name, target_filename)
            logger.info(f": ispr_generation: 12 Successfully saved ISPR document to S3: {target_filename}")
        except Exception as e:
            delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
            logger.error(f"ispr_generation: 13 Error saving ISPR document to S3: {str(e)}")
            raise


        ReportTrackingRes = ReportTrackingCompletion(
             report_id = report_id,
             created_by = user_id,
             name = name,
             session_id = session_id,
             created_at = timestamp,
             file_version = file_version,
             report_file_path = target_filename,
             template_fullname = template_fullname,
             template_name = template_name,
             product_name = product_name,  ############ Or Molecule Name 
             pqr_param_json_filename = pqr_param_json_filename,
             reporting_period=reporting_period, 
             edit_status = "Ready For review",
             completion_detail_section=DetailCompletionStatus,
             completion = 0,
             updated_at = "",
             edited_by  = "",
             locked = False,
             source_file_names = pqr_file_names,
             site_names=site_names,
             report_title = template_name + "_" + product_name + "_" + "v" + str(file_version),
             html_content = "<html></html>"
            
        )
             
    # store_isprtrackingcompletion(IsprTrackingRes, ISPR_DYNAMOTABLE)
        store_reporttrackingcompletion(ReportTrackingRes, REPORTS_DYNAMOTABLE)
    
        logger.info(f"ispr_generation: 14 ISPR generation completed successfully for site names: {ReportTrackingRes.site_names}")
        return ReportTrackingRes

    except Exception as e:
        delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
        logger.error(f"ispr_generation: 15 Unexpected error in ISPR generation: {str(e)}")
        raise


# def ispr_generation(user_id, session_id, timestamp, model_id, bucket_name, mapping_file_s3key,
#                     mapping_file_sheet_name, upload_folder, output_folder, model_max_tokens,
#                     pqr_param_json_filename, report_id, file_version, template_name,
#                     template_fullname, name):
#     """
#     Generates the ISPR (Integrated Site Performance Report) document for a given context.
    
#     Parameters:
#         user_id (str): ID of the user generating the report.
#         session_id (str): ID of the current session.
#         timestamp (str): Timestamp of the report generation request.
#         model_id (str): LLM model identifier for summarization.
#         bucket_name (str): S3 bucket name where input/output files reside.
#         mapping_file_s3key (str): S3 key of the Excel mapping file.
#         mapping_file_sheet_name (str): Sheet name in the mapping Excel file.
#         upload_folder (str): Folder path for incoming uploads in S3.
#         output_folder (str): Folder path for output reports in S3.
#         model_max_tokens (int): Max token length per chunk for the LLM.
#         pqr_param_json_filename (str): JSON file used for context input.
#         report_id (str): Unique ID for the report.
#         file_version (int): Version of the report file.
#         template_name (str): Name of the report template.
#         template_fullname (str): Full name of the report template.
#         name (str): Display name of the report creator.
        
#     Returns:
#         ReportTrackingCompletion: Object containing the full report tracking info.
#     """

#     product_name, reporting_period, site_names, pqr_file_names, list_versions = retrieve_context(
#         bucket_name, output_folder, pqr_param_json_filename)

#     logger.info("ispr_generation: 1 Retrieved context from JSON.")

#     # Set up LLM model
#     model_kwargs = {
#         "temperature": 0,
#         "top_k": 250,
#         "top_p": 0,
#         "stop_sequences": ["\n\nHuman"]
#     }

#     bedrock_runtime = boto3.client(service_name="bedrock-runtime")
#     llm = ChatBedrock(client=bedrock_runtime, model_id=model_id, model_kwargs=model_kwargs)

#     try:
#         unique_str = ''.join(random.choice(string.ascii_lowercase) for _ in range(6))
#         ispr_filename = f"ispr-{product_name}-{reporting_period}-{user_id}-{unique_str}.docx"
#         logger.info(f"ispr_generation: 1 Starting ISPR generation for product: {product_name}, period: {reporting_period}")

#         try:
#             mapping_dic = get_mapping_list(
#                 bucket_name,
#                 excel_file_path=mapping_file_s3key,
#                 az_mapping_sheet_name=mapping_file_sheet_name
#             )
#             logger.info("ispr_generation: 2 Successfully retrieved mapping dictionary")
#         except Exception as e:
#             delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
#             logger.error(f"ispr_generation: 3 Error retrieving mapping dictionary: {str(e)}")
#             raise

#         # Extracting mapping components
#         section_names_keysearch = mapping_dic["section_names_keysearch"]
#         section_names_ispr = mapping_dic["section_names_ispr"]
#         ispr_summary_flag = mapping_dic["ispr_summary_flag"]
#         ispr_map_prompt_templates = mapping_dic["ispr_map_prompt_ls"]
#         ispr_combine_prompt_templates = mapping_dic["ispr_combine_prompt_ls"]

#         try:
#             ispr_json_filename = f"ispr-{product_name}-{reporting_period}-{user_id}.json"
#             output_path_json = f"{output_folder}{ispr_json_filename}"
#             json_obj = s3.get_object(Bucket=bucket_name, Key=output_path_json)
#             json_data = json_obj['Body'].read().decode('utf-8')
#             df = pd.DataFrame(json.loads(json_data))
#             logger.info(f"ispr_generation: 4 Successfully loaded JSON data from S3: {output_path_json}")
#         except Exception as e:
#             delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
#             logger.error(f"ispr_generation: 5 Error loading JSON data from S3: {str(e)}")
#             raise

#         document = Document_docx()
#         detail_completion_status = []

#         for section_index, section_name in enumerate(section_names_keysearch):
#             logger.info(f"ispr_generation: 6 Iterating section name: {section_name}")

#             subsection = []
#             heading_level = 1
#             document.add_heading(section_names_ispr[section_index], heading_level)
#             heading_level += 1

#             docs = []
#             images_section_sites = []
#             tables_section_sites = []

#             try:
#                 logger.info(f"ispr_generation: 7 Processing section: {section_name}")

#                 for site_name in site_names:
#                     logger.info(f"ispr_generation: 8 section_name: Processing site: {section_name}:{site_name}")
#                     logger.info(f"ispr_generation: 8.01 shape: {df.shape}")
#                     logger.info(f"ispr_generation: 8.02 columns: {df.columns}")
#                     logger.info(f"ispr_generation: 8.03 head: {df.head()}")

#                     subset = df.loc[
#                         (df["section_name"] == section_name.lower()) &
#                         (df["site_name"] == site_name.lower()) &
#                         (df["product_name"] == product_name),
#                         ["page_num", "images", "site_name", "tables", "section_name", "file_name", "text"]
#                     ]

#                     logger.info("ispr_generation: 8.1")

#                     if not subset.empty:
#                         logger.info("ispr_generation: 8.2")
#                         max_row = subset.loc[subset["page_num"].idxmax()]
#                         logger.info("ispr_generation: 8.3")

#                         if ispr_summary_flag[section_index] == SUMMARY_FLAG:
#                             text = max_row["text"]
#                             for i in range(0, len(text), model_max_tokens):
#                                 chunk = text[i:i + model_max_tokens]
#                                 docs.append(Document(page_content=chunk))
#                             logger.info("ispr_generation: 8.31")
#                             images_section_sites.append(max_row["images"])
#                             tables_section_sites.append(max_row["tables"])
#                             logger.info("ispr_generation: 8.32")
#                         else:
#                             logger.info("ispr_generation: 8.4")
#                             heading_name = f"{section_name}_{site_name}"
#                             document.add_heading(heading_name, heading_level)
#                             document.add_paragraph(max_row["text"])
#                             logger.info("ispr_generation: 8.5")
#                             subsection.append(IsprTrackingSubSection(
#                                 SectionName=f"{section_names_ispr[section_index]}_{site_name}",
#                                 SectionText=max_row["text"],
#                                 SectionTables=max_row["tables"],
#                                 SectionImages=max_row["images"]
#                             ))
#                         logger.info("ispr_generation: 8.6")
#                     else:
#                         logger.info("ispr_generation: 8.7")
#                         continue

#                 if ispr_summary_flag[section_index] == SUMMARY_FLAG:
#                     logger.info(f"ispr_generation: 9 Generating summary for section: {section_name} - site name: {site_name}")
#                     summary_text = get_abs_summarize(
#                         llm, docs,
#                         ispr_map_prompt_templates[section_index],
#                         ispr_combine_prompt_templates[section_index],
#                         site_name
#                     )
#                     document.add_paragraph(summary_text['output_text'])
#                     chunk_report(
#                         bucket_name, ispr_filename, summary_text['output_text'], section_index,
#                         section_name, product_name, reporting_period, template_fullname,
#                         report_id, timestamp, user_id, CHUNK_REPORT_FOLDER
#                     )
#                     detail_completion_status.append(ReportTrackingSection(
#                         section_name=section_names_ispr[section_index],
#                         section_text=summary_text['output_text'],
#                         is_feedback_positive=None,
#                         completion=0
#                     ))
#                 else:
#                     logger.info(f"ispr_generation: 10 ELSE Condition: Generating summary for section: {section_name}")
#                     detail_completion_status.append(ReportTrackingSection(
#                         section_name=section_names_ispr[section_index],
#                         subsection=subsection,
#                         is_feedback_positive=None,
#                         completionStatus=0
#                     ))

#             except Exception as e:
#                 delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
#                 logger.error(f"ispr_generation: 11 Error processing section {section_name}: \nDetailed: {str(e)}")

#         try:
#             target_filename = f"{output_folder}{ispr_filename}"
#             doc_stream = io.BytesIO()
#             document.save(doc_stream)
#             doc_stream.seek(0)
#             s3.upload_fileobj(doc_stream, bucket_name, target_filename)
#             logger.info(f"ispr_generation: 12 Successfully saved ISPR document to S3: {target_filename}")
#         except Exception as e:
#             delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
#             logger.error(f"ispr_generation: 13 Error saving ISPR document to S3: {str(e)}")
#             raise

#         report_tracking_res = ReportTrackingCompletion(
#             report_id=report_id,
#             created_by=user_id,
#             name=name,
#             session_id=session_id,
#             created_at=timestamp,
#             file_version=file_version,
#             report_file_path=target_filename,
#             template_fullname=template_fullname,
#             template_name=template_name,
#             product_name=product_name,
#             pqr_param_json_filename=pqr_param_json_filename,
#             reporting_period=reporting_period,
#             edit_status="Ready For review",
#             completion_detail_section=detail_completion_status,
#             completion=0,
#             updated_at="",
#             edited_by="",
#             locked=False,
#             source_file_names=pqr_file_names,
#             site_names=site_names,
#             report_title=f"{template_name}_{product_name}_v{file_version}",
#             html_content="<html></html>"
#         )

#         store_reporttrackingcompletion(report_tracking_res, REPORTS_DYNAMOTABLE)
#         logger.info(f"ispr_generation: 14 ISPR generation completed successfully for site names: {report_tracking_res.site_names}")
#         return report_tracking_res

#     except Exception as e:
#         delete_incoming_files(bucket_name, upload_folder, output_folder, pqr_param_json_filename)
#         logger.error(f"ispr_generation: 15 Unexpected error in ISPR generation: {str(e)}")
#         raise

########################### Change in ISPR, Saving in S3 and track the completion Status of each Section
# def save_ispr(ispr_actual_status: IsprTrackingCompletion , bucket_name, target_filename):
    
#     document = Document_docx()
    
#     for section_index, ispr_section_status in enumerate(ispr_actual_status.DetailCompletionStatus):
#         i = 1
#         document.add_heading(ispr_section_status.SectionName, i)
#         if ispr_section_status.SectionText:
#             document.add_paragraph(ispr_section_status.SectionText)
#         i = i + 1
        
#         if ispr_section_status.Subsection:
#             for ispr_subsection in ispr_section_status.Subsection:
#                 document.add_heading(ispr_subsection.SectionName, i)
#                 document.add_paragraph(ispr_subsection.SectionText)
#                 # Add images
#                 if ispr_subsection.SectionImages:
#                     for image_path in ispr_subsection.SectionImages:
#                     #print(image_path)
#                         if len(image_path) > 0:
#                             img_stream = io.BytesIO()
#                             s3.download_fileobj(bucket_name, image_path, img_stream)
#                             img_stream.seek(0)

#                             document.add_picture(
#                             img_stream, width=Inches(4)
#                         )  # Add image with a fixed width

#                 # Add tables
#                 if ispr_subsection.SectionTables:
#                     for table_path in ispr_subsection.SectionTables:
#                         if len(table_path) > 0:
#                             csv_stream = io.BytesIO()
#                             s3.download_fileobj(bucket_name, table_path, csv_stream)
#                             csv_stream.seek(0)

#                             # Read the CSV content
#                             csv_data = list(csv.reader(io.StringIO(csv_stream.read().decode('utf-8'))))

#                             # Create a new table in Word
#                             rows = len(csv_data)
#                             cols = len(csv_data[0])
#                             word_table = document.add_table(rows=rows, cols=cols)

#                             # Populate the Word table   
#                             for row_index, row in enumerate(csv_data):
#                                 for col_index, cell in enumerate(row):
#                                     word_table.cell(row_index, col_index).text = str(cell)


#   # Save the document to a BytesIO object (in-memory file)
#     doc_stream = io.BytesIO()
#     document.save(doc_stream)
#     doc_stream.seek(0)
#     s3.upload_fileobj(doc_stream, bucket_name, target_filename)
    
    
# # def send_email(mail_body, email_to, subject, email_function_name, attachment_s3_path=None):
# #     client = boto3.client('lambda')
# #     payload = {
# #         'email_to': email_to,
# #         'email_subject': subject,
# #         'mail_body': mail_body,
# #         'attachment_s3_path': attachment_s3_path
# #     }
# #     response = client.invoke(FunctionName=email_function_name,
# #                             InvocationType='RequestResponse',  # 'Event' for async invocation
# #                             Payload=json.dumps(payload))
# #     response_payload = json.loads(response['Payload'].read())
# #     print("Email trigger response: " + str(response_payload))


# def send_email(mail_body, email_to, subject, email_function_name, attachment_s3_path=None):
#     # logger.info(f"send_email invocation subject : {subject}")
#     # initialize counter
#     if not hasattr(send_email, "_call_count"):
#         send_email._call_count = 0

#     # log every invocation, and highlight if >1
#     logger.info(f"[send_email] invocation #{send_email._call_count} → to={email_to!r}, subject={subject!r}")
#     if send_email._call_count > 1:
#         logger.warning(f"[send_email] called more than once in this run (#{send_email._call_count})")

#     send_email._call_count += 1

#     client = boto3.client('lambda')
#     payload = {
#         'email_to': email_to,
#         'email_subject': subject,
#         'mail_body': mail_body,
#         'attachment_s3_path': attachment_s3_path
#     }

#     response = client.invoke(
#         FunctionName=email_function_name,
#         InvocationType='RequestResponse',
#         Payload=json.dumps(payload)
#     )
#     response_payload = json.loads(response['Payload'].read())
#     logger.info(f"[send_email] trigger response: {response_payload}")
#     return response_payload


def save_ispr(ispr_actual_status: IsprTrackingCompletion, bucket_name, target_filename):
    """
    Generate a Word document from ISPR completion data and upload it to S3.

    Args:
        ispr_actual_status (IsprTrackingCompletion): Object containing ISPR detail and subsection status.
        bucket_name (str): The name of the S3 bucket to upload the file to.
        target_filename (str): The name of the file to be saved in S3.

    Returns:
        None
    """
    logger.info("save_ispr: Starting document creation.")
    document = Document_docx()

    for section_index, ispr_section_status in enumerate(ispr_actual_status.DetailCompletionStatus):
        heading_level = 1
        document.add_heading(ispr_section_status.SectionName, heading_level)

        # Add main section text if it exists
        if ispr_section_status.SectionText:
            document.add_paragraph(ispr_section_status.SectionText)

        heading_level += 1

        if ispr_section_status.Subsection:
            for ispr_subsection in ispr_section_status.Subsection:
                document.add_heading(ispr_subsection.SectionName, heading_level)
                document.add_paragraph(ispr_subsection.SectionText)

                # Add images if present
                if ispr_subsection.SectionImages:
                    for image_path in ispr_subsection.SectionImages:
                        if len(image_path) > 0:
                            img_stream = io.BytesIO()
                            s3.download_fileobj(bucket_name, image_path, img_stream)
                            img_stream.seek(0)

                            # Insert image into document
                            document.add_picture(img_stream, width=Inches(IMAGE_WIDTH_INCHES))

                # Add tables if present
                if ispr_subsection.SectionTables:
                    for table_path in ispr_subsection.SectionTables:
                        if len(table_path) > 0:
                            csv_stream = io.BytesIO()
                            s3.download_fileobj(bucket_name, table_path, csv_stream)
                            csv_stream.seek(0)

                            # Read and parse CSV content
                            csv_data = list(csv.reader(io.StringIO(csv_stream.read().decode('utf-8'))))

                            if csv_data:
                                rows = len(csv_data)
                                cols = len(csv_data[0])
                                word_table = document.add_table(rows=rows, cols=cols)

                                # Populate Word table
                                for row_index, row in enumerate(csv_data):
                                    for col_index, cell in enumerate(row):
                                        word_table.cell(row_index, col_index).text = str(cell)

    # Save document to in-memory stream
    doc_stream = io.BytesIO()
    document.save(doc_stream)
    doc_stream.seek(0)

    # Upload document to S3
    s3.upload_fileobj(doc_stream, bucket_name, target_filename)
    logger.info(f"save_ispr: Document uploaded to s3://{bucket_name}/{target_filename}")


def send_email(mail_body, email_to, subject, email_function_name, attachment_s3_path=None):
    """
    Send an email using AWS Lambda and log the process.

    Args:
        mail_body (str): The body of the email.
        email_to (str): Recipient email address.
        subject (str): Subject line for the email.
        email_function_name (str): Name of the Lambda function to invoke.
        attachment_s3_path (str, optional): S3 path to an attachment file.

    Returns:
        dict: The response payload from the Lambda invocation.
    """
    # Initialize invocation counter
    if not hasattr(send_email, "_call_count"):
        send_email._call_count = 0

    logger.info(f"[send_email] invocation #{send_email._call_count} → to={email_to!r}, subject={subject!r}")
    
    # Warn if the function is invoked more than once during the same run
    if send_email._call_count > 1:
        logger.warning(f"[send_email] called more than once in this run (#{send_email._call_count})")

    send_email._call_count += 1

    # Construct payload for Lambda invocation
    client = boto3.client('lambda')
    payload = {
        'email_to': email_to,
        'email_subject': subject,
        'mail_body': mail_body,
        'attachment_s3_path': attachment_s3_path
    }

    response = client.invoke(
        FunctionName=email_function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    response_payload = json.loads(response['Payload'].read())
    logger.info(f"[send_email] trigger response: {response_payload}")
    return response_payload

def scan_and_update_ispr_status(default_n_mins: int, table_name: str):
    """
    Scans the DynamoDB table for records where Ispr_Editor_Status is True, and updates the status
    to False for records that were edited more than default_n_mins minutes ago.

    This function uses the table.scan method to retrieve all records that match the filter condition
    and updates the Ispr_Editor_Status column for qualifying records using the update_ispr_edit_status
    function.

    If there are many records in the table, the scan operation is performed in batches using pagination.

    Raises:
        ClientError: If an error occurs while scanning or updating the DynamoDB table.
    
    logger.info(f"\nscan_and_update_ispr_status Function executed START with default_n_mins = {default_n_mins}, table_name = {table_name}")
    """
    
    logger.info("scan_and_update_ispr_status -------START----->")
    logger.info(f"scan_and_update_ispr_status executed with default_n_mins = {default_n_mins}, table_name = {table_name}")
    
    try:
        scan_kwargs = {}
        done = False
        start_key = None
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        while not done:
            # If start_key is present, it means there are more records to be scanned
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key

            # Define a placeholder for the 'Timestamp' attribute.
            # placeholder is needed when one column name equals a keyword.
            timestamp_placeholder = '#TS'

            # Scan the table for records where Ispr_Editor_Status is True
            response = table.scan(
                FilterExpression=Attr('locked').eq(True),
                ProjectionExpression=f'report_id, created_at, locked, updated_at'
                # ExpressionAttributeNames={timestamp_placeholder: 'Timestamp'}
            )

            # If LastEvaluatedKey is present, it means there are more records to be scanned
            start_key = response.get('LastEvaluatedKey', None)
            done = 'LastEvaluatedKey' not in response

            # Check if there are any items in the response
            items = response.get('Items', [])
            if not items:
                logger.info("No records found.")
                continue

            # Iterate over the scan results
            for item in items:
                logger.info("scan_and_update_ispr_status: Iterating records")
                
                # Check if the 'DateofEdition' attribute exists
                if 'updated_at' in item:
                    date_of_edit = parser.isoparse(item['updated_at'])
                
                    # Check if the record was edited more than default_n_mins minutes ago
                    if datetime.now() - date_of_edit > timedelta(minutes=default_n_mins):
                        logger.info(f"scan_and_update_ispr_status: Updating records which are {default_n_mins} min old.")
                
                        # Update the Ispr_Editor_Status to False for the qualifying record
                        update_report_locked_status(item['report_id'], item['created_at'], False, table_name, True)
                        logger.info("scan_and_update_ispr_status: Updated ----------->")

                else:
                    # Handle the case where 'DateofEdition' is null or missing
                    logger.warning(f"scan_and_update_ispr_status: 'updated_at' is missing for item {item}")
                    continue

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
    except Exception as e:
        logger.error(f"startup error: {str(e)}")

################### HTML to DOCX ###################
# def get_image_size(image_bytes):
#     """Get the width of the image in inches, maintaining aspect ratio."""
#     with Image.open(BytesIO(image_bytes)) as img:
#         width, height = img.size  # Get size in pixels
#         dpi = 96  # Default DPI for Word
#         return width / dpi, height / dpi  # Convert to inches

# def set_table_borders(table):
#     """Apply visible borders to a table in Word."""
#     tbl = table._element  # Get XML element of table
#     tbl_pr = tbl.find(qn("w:tblPr"))

#     # Ensure table properties exist
#     if tbl_pr is None:
#         tbl_pr = OxmlElement("w:tblPr")
#         tbl.insert(0, tbl_pr)

#     # Remove any existing border settings
#     tbl_borders = tbl_pr.find(qn("w:tblBorders"))
#     if tbl_borders is not None:
#         tbl_pr.remove(tbl_borders)

#     # Add new borders
#     tbl_borders = OxmlElement("w:tblBorders")

#     for border_name in ["top", "left", "bottom", "right", "insideH", "insideV"]:
#         border = OxmlElement(f"w:{border_name}")
#         border.set(qn("w:val"), "single")  # Border type
#         border.set(qn("w:sz"), "6")  # Border thickness
#         border.set(qn("w:space"), "0")
#         border.set(qn("w:color"), "000000")  # Black color
#         tbl_borders.append(border)

#     tbl_pr.append(tbl_borders)  # Attach new borders to table properties


def get_image_size(image_bytes):
    """
    Calculate the width and height of an image in inches, preserving aspect ratio.

    Args:
        image_bytes (bytes): The image content in bytes.

    Returns:
        tuple: Width and height in inches.
    """
    logger.info("get_image_size: Opening image to calculate dimensions.")
    with Image.open(BytesIO(image_bytes)) as img:
        width, height = img.size  # Dimensions in pixels
        return width / DEFAULT_DPI, height / DEFAULT_DPI  # Convert to inches

def set_table_borders(table):
    """
    Apply black single-line borders to all sides and internal lines of a Word table.

    Args:
        table: A python-docx table object.
    """
    logger.info("set_table_borders: Applying custom borders to table.")

    tbl = table._element  # Access the underlying XML element
    tbl_pr = tbl.find(qn("w:tblPr"))  # Find table properties

    # Create table properties if not present
    if tbl_pr is None:
        logger.info("set_table_borders: Creating new table properties element.")
        tbl_pr = OxmlElement("w:tblPr")
        tbl.insert(0, tbl_pr)

    # Remove existing borders if present
    tbl_borders = tbl_pr.find(qn("w:tblBorders"))
    if tbl_borders is not None:
        logger.info("set_table_borders: Removing existing borders.")
        tbl_pr.remove(tbl_borders)

    # Create and append new borders
    tbl_borders = OxmlElement("w:tblBorders")
    for border_name in BORDER_NAMES:
        border = OxmlElement(f"w:{border_name}")
        border.set(qn("w:val"), BORDER_TYPE)
        border.set(qn("w:sz"), BORDER_SIZE)
        border.set(qn("w:space"), BORDER_SPACE)
        border.set(qn("w:color"), BORDER_COLOR)
        tbl_borders.append(border)

    tbl_pr.append(tbl_borders)
    logger.info("set_table_borders: Borders applied successfully.")

def save_ispr_report_to_s3(files: UploadFile, bucket_name: str, target_filename: str):
    """
    Upload a file object (e.g., .docx file) directly to an S3 bucket.
    Supports the following API:
    /update_ispr_doc_file

    Args:
        files (UploadFile): The file object to be uploaded.
        bucket_name (str): The name of the S3 bucket.
        target_filename (str): The desired filename for the uploaded file.

    Returns:
        None
    """
    # Initialize S3 client
    s3 = boto3.client("s3")
    logger.info("\nsave_ispr_report_to_s3:------------ 1 -------------")

    try:
        # Upload file to S3
        s3.upload_fileobj(
            files.file,
            bucket_name,
            target_filename,
            ExtraArgs={"ContentType": files.content_type}
        )

        logger.info(f"\nsave_ispr_report_to_s3: File '{target_filename}' successfully uploaded to s3://{bucket_name}/{target_filename}")
    except Exception as e:
        logger.error(f"\n\nsave_ispr_report_to_s3:Error uploading file to S3: {str(e)}")
        # raise HTTPException(status_code=500, detail=str(e))

#####################################################################
def scan_and_ingestion_and_generation(default_n_mins, table_queue_name, table_template_master, knowledge_base_name, model_id, bucket_name, input_folder, output_folder, list_site_names, list_product_names, date_pattern, header_pattern, footer_pattern, email_sender, kbname_max_size, knowledge_base_name_report):
    
    """
    Scans the DynamoDB table for records where Ispr_Editor_Status is True, and updates the status
    to False for records that were edited more than default_n_mins minutes ago.

    This function uses the table.scan method to retrieve all records that match the filter condition
    and updates the Ispr_Editor_Status column for qualifying records using the update_ispr_edit_status
    function.

    If there are many records in the table, the scan operation is performed in batches using pagination.

    Raises:
        ClientError: If an error occurs while scanning or updating the DynamoDB table.
    """
    logger.info(f"scan_and_ingestion_and_generation executed with default_n_mins = {default_n_mins}, table_name = {table_queue_name}")
    
    try:
        scan_kwargs = {}
        done = False
        start_key = None
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_queue_name)

        while not done:
            logger.info("scan_and_ingestion_and_generation -------1------->")
            # If start_key is present, it means there are more records to be scanned
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key

            # Define a placeholder for the 'Timestamp' attribute.
            # placeholder is needed when one column name equals a keyword.
            timestamp_placeholder = '#TS'

            # Scan the table for records where status is "queued"
            response = table.scan(FilterExpression=(Attr('status_in_queue').eq("queued")))
            # logger.info(f"\nscan_and_ingestion_and_generation ----2---table scan response------\n{response}->")
            
            ################## Get the oldest "template_fullname" in Queues with status "queued"         
            response['Items'].sort(
                key=lambda x: datetime.fromisoformat(x["created_at"]),
                reverse=False
            )
            
            # If LastEvaluatedKey is present, it means there are more records to be scanned
            start_key = response.get('LastEvaluatedKey', None)
            done = 'LastEvaluatedKey' not in response
            # logger.info("scan_and_ingestion_and_generation -------3------->")

            # Check if there are any items in the response
            items = response.get('Items', [])
            # logger.info("scan_and_ingestion_and_generation -------4------->")
            if not items:
                logger.info("No records found.")
                continue
                                  
              # Iterate over the scan results
            for item in items:
                logger.info("scan_and_ingestion: Iterating records")
                
                formatted_timestamp = datetime.now().isoformat()
                report_id = item["report_id"]
                created_at =  parser.isoparse(item["created_at"])
                file_version = item["file_version"]
                template_fullname = item["template_fullname"]
                template_name = item["template_name"]
                user_id = item["created_by"]
                name = item["name"]
                emailId = item["emailId"]
                session_id = item["session_id"]
                timestamp = datetime.now().isoformat()
                upload_folder = item["upload_folder"]
                pqr_param_json_filename = item["pqr_param_json_filename"]
                product_name = item["product_name"]
                reporting_period = item["reporting_period"]
                source_file_names = item["source_file_names"]
                source_file_types = item["source_file_types"]
                list_versions = item["list_versions"]
                chunk_output_folder = template_name + "_chunk_output"
                logger.info(f"scan_and_ingestion_and_generation -------5.1------->\n{source_file_names}")

                # Check if the record was edited more than default_n_mins minutes ago
                logger.info("scan_and_ingestion_and_generation -------6------->")

                # Update the Ispr_Editor_Status to False for the qualifying record
                update_report_queue_status(item["report_id"], item["file_version"], "processing", REPORTSQUEUE_DYNAMOTABLE)
                logger.info("scan_and_ingestion_and_generation -------7------->")
                email_body_succeed = f"""
                            Report Generation Completed Successfully

                            Report ID: {report_id}
                            File Version: {file_version}
                            Template: {template_fullname}
                            Status: Completed
                            Completion Time: {datetime.now().isoformat()}
                            """
                email_body_failed = f"""
                            Report Generation Failed

                            Report ID: {report_id}
                            File Version: {file_version}
                            Template: {template_fullname}
                           Status: Completed
                            Completion Time: {datetime.now().isoformat()}
                            """
                subject_succeed = f"""Report Generation Succeed"""
                subject_failed = f"""Report Generation Failed"""

                if "ISPR" in template_fullname:

                    try: 

                        Ingest_PQR(bucket_name, upload_folder, input_folder, output_folder, date_pattern, header_pattern, footer_pattern, list_site_names, list_product_names, pqr_param_json_filename, session_id, user_id, formatted_timestamp, product_name, reporting_period, source_file_types, list_versions, source_file_names, report_id, file_version) 

                        ispr_generation(user_id, session_id, formatted_timestamp, model_id, bucket_name, MAPPING_FILE_PATH, SHEET_NAME_MAPPING, upload_folder, output_folder, model_max_tokens, pqr_param_json_filename, report_id, file_version, template_name, template_fullname, name)
                        update_report_queue_status(item["report_id"], item["file_version"], "completed", REPORTSQUEUE_DYNAMOTABLE)
                        send_email(email_body_succeed, emailId, subject_succeed, email_sender)
                        logger.info(f"scan_and_ingestion_and_generation -------ISPR-----> Ingest_Report Start")
                        Ingest_Report(knowledge_base_name_report, bucket_name, kbname_max_size)
                        logger.info(f"scan_and_ingestion_and_generation -------ISPR-----> Ingest_Report Ende")
                        logger.info(f"scan_and_ingestion_and_generation -------ISPR-----> Success")
                        ############## Send Email to user_id

                    except Exception as e:
                        update_report_queue_status(item["report_id"], item["file_version"], "failed", REPORTSQUEUE_DYNAMOTABLE)
                        send_email(email_body_failed, emailId, subject_failed, email_sender)
                        logger.info(f"scan_and_ingestion_and_generation -------ISPR-----> Exception")
                        delete_s3_folder(bucket_name, upload_folder)
                        logger.error(f"Generation Failed error: {str(e)}")

                else:

                    try: 
                        ####################### Ingestion in KnowledgeBase and Waiting for Sync Completion
                        Ingest_SIV(knowledge_base_name, bucket_name, upload_folder, source_file_names, source_file_types, chunk_output_folder, table_template_master, template_fullname, template_name, kbname_max_size, report_id) 
                        # logger.info("scan_and_ingestion_and_generation -------Site-----> 1")
                        # delete_s3_folder(bucket_name, chunk_output_folder)
                        # logger.info("scan_and_ingestion_and_generation -------Site-----> 2")
                        time.sleep(120)

                        ################################## Generation
                        logger.info("scan_and_ingestion_and_generation CALLING sitev_generation -------------")
                        sitev_generation(report_id, file_version, user_id, session_id, timestamp, table_template_master, knowledge_base_name, model_id, template_fullname, bucket_name,  output_folder, source_file_names, source_file_types, kbname_max_size, name, upload_folder)
                        # logger.info("scan_and_ingestion_and_generation -------Site-----> 3")
                        update_report_queue_status(item["report_id"], item["file_version"], "completed", REPORTSQUEUE_DYNAMOTABLE)
                        # delete_s3_folder(bucket_name, upload_folder)
                        ############## Send Email to user_id
                        logger.info("scan_and_ingestion_and_generation CALLING send_email -------------")
                        send_email(email_body_succeed, emailId, subject_succeed, email_sender)
                        logger.info(f"scan_and_ingestion_and_generation -------SVR-----> Ingest_Report Start")
                        Ingest_Report(knowledge_base_name_report, bucket_name, kbname_max_size)
                        logger.info(f"scan_and_ingestion_and_generation -------SVR-----> Ingest_Report End")
                        delete_data_source(knowledge_base_name, template_name, kbname_max_size)
                        logger.info(f"scan_and_ingestion_and_generation -------Site-----> Success --- 5")

                    except Exception as e:
                        update_report_queue_status(item["report_id"], item["file_version"], "failed", REPORTSQUEUE_DYNAMOTABLE)
                        logger.info("scan_and_ingestion_and_generation -------8------->")
                        send_email(email_body_failed, emailId, subject_failed, email_sender)
                        logger.error(f"scan_and_ingestion_and_generation -------Site-----> Exception")
                        delete_s3_folder(bucket_name, upload_folder)
                        logger.error(f"Generation Failed error: {str(e)}")

                logger.info("scan_and_ingestion: Updated ----------->")

            # else:
            #     # Handle the case where 'DateofEdition' is null or missing
            #     logger.warning(f"scan_and_update_ispr_status: 'DateofEdition' is missing for item {item}")
            #     continue

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
    except Exception as e:
        logger.error(f"startup error: {str(e)}")
        
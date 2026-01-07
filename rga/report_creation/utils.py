import re
import os
import csv
import boto3
from datetime import datetime, timedelta
import pdfplumber
import io
from docx import Document as Document_docx
from docx.shared import Inches
from docx.image.exceptions import UnrecognizedImageError
import pandas as pd
import json
import mammoth
import html2docx

from typing import Optional, List, Dict
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch, OpenSearchException, NotFoundError, RequestError
from langchain_community.embeddings import BedrockEmbeddings
from langchain.docstore.document import Document
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth
from langchain.chains.summarize import load_summarize_chain
from langchain.prompts import PromptTemplate
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from loguru import logger
from datetime import datetime

import PyPDF2
from io import BytesIO
from pathlib import Path
from PyPDF2 import PdfReader
from dateutil import parser
from boto3.dynamodb.conditions import Attr

import base64
from bs4 import BeautifulSoup
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from PIL import Image

###############with app
# from data import (IngestResult, IsprTrackingSection, IsprTrackingSubSection, IsprTrackingCompletion, QueryRequest, QnaAnswer, AnswerRequest, User, Query, RequestQuery, Citation, 
#     QuickReply, Result, QueryResponse, FeedbackDisplayOptions, Feedback, ChatInteraction, ChatMetadata, ChatHistorySearchRequest, FeedbackRequest, PqrTrackingStatus
# )

# from data import  ReportTrackingCompletion, ReportTrackingSection, ReportTrackingWelcomePageAllProduct

from knowbase_utils import (get_knowledge_base_id, create_data_source, sync_data_source, check_sync_status, retrieve_and_generate)
from data import ReportQueue

from config import *
###############

from fastapi import FastAPI, HTTPException, Response, UploadFile, status, File, Form
from fastapi.responses import JSONResponse
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
#from chat_app_aws import ReportGeneration

load_dotenv()
s3 = boto3.client("s3")
bucket_name = f"{BUCKET_NAME}"

ERROR_MESSAGE = "Oops! It seems there’s a network issue. Please check your connection and try again in a moment."
    
############# Defined Presigned URL for Download ##########################
import re

def delete_reportRecords(report_id, created_at, tablename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    try:
        response = table.scan(
           FilterExpression=( Attr("report_id").eq(report_id)&
                              Attr("created_at").eq(created_at)
                            )
         )
    
        if response['Items']:
            for item in response['Items']:
                table.delete_item(
                      Key={'report_id':item['report_id'], 
                  'created_at':item['created_at']
                 }
            )
            return {f"message: Records of Report has been successfully deleted in Queue"}
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            

def store_report_queue(request_object: ReportQueue, REPORTSQUEUE_DYNAMOTABLE: str):
    try:
        # message_id = str(uuid.uuid4())  #Generate unique message ID
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(REPORTSQUEUE_DYNAMOTABLE)
        item = request_object.dict()
        table.put_item(Item=item)
        return {"message": "New Request has beend queued successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        
##################################

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

def update_report_queue_uploadfolder(report_id, file_version, bucket_name, upload_folder, tablename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    try:
        response = table.scan(
           FilterExpression=(Attr("report_id").eq(report_id) &
                             Attr("file_version").eq(file_version)
                             # Attr("status").in(["queued", "processing", "completed"]
                           
         ) 
        )
            
        if response['Items']: ########## conflict detected
            delete_s3_folder(bucket_name, response['Items'][0]["upload_folder"])
    except Exception as e:
           raise HTTPException(status_code=500, detail=str(e))
            
        # Construct the update expression
    update_expression = "SET upload_folder = :upload_folder"
    expression_attribute_values = {":upload_folder": upload_folder}

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


def update_report_queue_overwrite(report_id, file_version, bucket_name, upload_folder, created_by, emailid, name, sessionid, created_at, tablename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    try:
        response = table.scan(
           FilterExpression=(Attr("report_id").eq(report_id) &
                             Attr("file_version").eq(file_version)
                             # Attr("status").in(["queued", "processing", "completed"]
                           
         ) 
        )
            
        if response['Items']: ########## conflict detected
            delete_s3_folder(bucket_name, response['Items'][0]["upload_folder"])
    except Exception as e:
           raise HTTPException(status_code=500, detail=str(e))
            
    # Construct the update expression
    update_expression = "SET upload_folder = :upload_folder, created_by = :created_by, emailId =:emailid, #username =:name, session_id =:sessionid, created_at =:created_at"
    expression_attribute_values = {
                                    ":upload_folder": upload_folder,
                                    ":created_by": created_by,
                                    ":emailid": emailid,
                                    ":name": name,
                                    ":sessionid": sessionid,
                                    ":created_at": created_at
                               }
    key = { 
            'report_id': report_id,
            'file_version': file_version
        }

    # Update the item in DynamoDB
    update_response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeNames={
        '#username': 'name'  
        },
        ExpressionAttributeValues=expression_attribute_values,     
        ReturnValues="UPDATED_NEW"
    )  
    return update_response


#############
def report_id_file_version_extracted(template_fullname, source_file_names, table_name):  
                                     
    dynamodb = boto3.resource('dynamodb')
    table=dynamodb.Table(table_name)
    
    try:
        response = table.scan(
           FilterExpression=(Attr("template_fullname").eq(template_fullname) &
                             Attr("source_file_names").eq(source_file_names
                             # Attr("status").in(["queued", "processing", "completed"]
                            )
         ) )
            
        
            
        if response['Items']: ########## conflict detected
            response['Items'].sort(
            key=lambda x: datetime.fromisoformat(x['created_at']),
            reverse=True
        )
            
        if table_name == REPORTSQUEUE_DYNAMOTABLE:            
            return response['Items'][0]["report_id"],  response['Items'][0]["file_version"], response['Items'][0]["created_at"], response['Items'][0]["status_in_queue"]
        
        if table_name == REPORTS_DYNAMOTABLE:            
            return response['Items'][0]["report_id"],  response['Items'][0]["file_version"], response['Items'][0]["created_at"]
            
    except Exception as e:
           raise HTTPException(status_code=500, detail=str(e))
        
    return None, None

def handle_validation_failure(error_message):
    """
    Handles validation failure by deleting incoming files and logging an error message.
    
    Args:
        error_message (str): Error message to be logged and returned.
        bucket_name (str): Name of the storage bucket.
        upload_folder (str): Folder where files were uploaded.
        output_folder (str): Folder where output files are stored.
        pqr_param_json_filename (str): JSON file name containing PQR parameters.
    
    Returns:
        JSONResponse: Response containing the error message with status code 400.
    """
    logger.error(f"Unexpected error in pqr_ingestion: {error_message}")
    print(f"Unexpected error in pqr_ingestion: {error_message}")
    return JSONResponse(content=error_message, status_code=400)
            
def validate_pqr_files(
    list_pqr_file_name, product_list, reporting_period_list, list_filesite_name, list_size_exceeded_flag,
    bucket_name, upload_folder, section_names):
    """
    Validates PQR file metadata against predefined criteria.
    Implementing new validations
    
    Args:
        list_pqr_file_name (list): List of PQR file names.
        product_list (list): List of product names corresponding to PQR files.
        reporting_period_list (list): List of reporting periods corresponding to PQR files.
        list_filesite_name (list): List of site names corresponding to PQR files.
        list_size_exceeded_flag (list): List of boolean flags indicating whether file size is exceeded.
        bucket_name (str): Name of the storage bucket.
        upload_folder (str): Folder where files were uploaded.
        output_folder (str): Folder where output files are stored.
        pqr_param_json_filename (str): JSON file name containing PQR parameters.
    
    Returns:
        JSONResponse: Response indicating validation success or failure.
    """
    
    num_pqr_files = len(list_pqr_file_name)
    logger.info(f"\nvalidate_pqr_files: product_list: \t{product_list}")
    logger.info(f"\nvalidate_pqr_files: reporting_period_list: \t{reporting_period_list}")
    logger.info(f"\nvalidate_pqr_files: len(reporting_period_list): \t{len(reporting_period_list)}")
    logger.info(f"\nvalidate_pqr_files: (num_pqr_files): \t{num_pqr_files} \t{list_pqr_file_name}")
    logger.info(f"\nvalidate_pqr_files: list_filesite_name: \t{list_filesite_name}")
        
    # Validation a: Check if the number of size flags matches the number of PQR files
    if len(list_size_exceeded_flag) != num_pqr_files:
        return handle_validation_failure("Validation failed: Number of size exceeded flags does not match the number of PQR files.")

    # Validation b: Check if any file exceeded the allowed size
    for flag in list_size_exceeded_flag:
        if flag:
            return handle_validation_failure(f"Validation failed: One or more files exceed the allowed size {input_size_limit}")

    # Validation c: Ensure product names are the same
    if len(product_list) != num_pqr_files:
        return handle_validation_failure("Validation failed: Number of product names does not match the number of PQR files.")
    
    if len(set(product_list)) != 1:
        return handle_validation_failure("Validation failed: Product names are inconsistent across PQR files.")
    
    # Validation d: Ensure reporting periods are the same
    if len(reporting_period_list) != num_pqr_files:
        return handle_validation_failure("Validation failed: Number of reporting periods does not match the number of PQR files.")
    
    if len(set(reporting_period_list)) != 1:
        return handle_validation_failure("Validation failed: Reporting periods are inconsistent across PQR files.")
    
    # Validation e: Ensure site names are unique
    if len(list_filesite_name) != num_pqr_files:
        return handle_validation_failure("Validation failed: Number of site names does not match the number of PQR files.")
    
    if len(set(list_filesite_name)) != num_pqr_files:
        return handle_validation_failure("Validation failed: Site names are not unique across PQR files.")

    # Validation e: Validate template structure
#     try:
#         for pqr_file_name in list_pqr_file_name:
#             # pdf_path = f"{upload_folder}{product_name}/{reporting_period}/{pqr_file_name}"
#             pdf_path = f"{upload_folder}{pqr_file_name}"
#             logger.info(f"\n6.validate_pqr_files: pdf_path ----->{pdf_path}")
            
#             template_validation_result = validate_template_struct(bucket_name, pdf_path, section_names)
#             logger.info(f"\n7.validate_pqr_files: template_validation_result ----->{template_validation_result}")
            
#             if template_validation_result['validation_failed']:
#                 return handle_validation_failure(f"Validation failed: Template structure mismatch for file {pqr_file_name}.")
#     except Exception as e:
#         return handle_validation_failure(f"Validation failed: An error occurred during template structure validation: {str(e)}")
    
    return JSONResponse(content="Validation successful.", status_code=200)
            
def delete_s3_folder(bucket_name: str, folder_name: str = "temp/"):
    try:
        s3_res = boto3.resource('s3')
        bucket = s3_res.Bucket(bucket_name)
        objects_to_delete = bucket.objects.filter(Prefix=folder_name)
        delete_requests = [{'Key': obj.key} for obj in objects_to_delete]
        
        if delete_requests:
            response = bucket.delete_objects(
                Delete={
                    'Objects': delete_requests,
                    'Quiet': True
                }
            )
            return response
        else:
            return {"Deleted": [], "Errors": []}
    
    except ClientError as err:
        logger.info(str(err))
        return {"Deleted": [], "Errors": [str(err)]}
 
def sort_versions(pqr_files: list, site_names: list, versions: list) -> tuple:
    """
    Sorts the versions in ascending order and rearranges the corresponding file and site lists accordingly.

    Args:
        pqr_files (list): List of file names.
        site_names (list): List of site names.
        versions (list): List of version strings.

    Returns:
        tuple: Three sorted lists (sorted_pqr_files, sorted_site_names, sorted_versions).
    """
    # Check if all lists have the same length.
    if not (len(pqr_files) == len(site_names) == len(versions)):
        raise ValueError("All input lists must have the same length.")
        
    # Extract numeric values from version strings
    def extract_version_number(version):
        match = re.match(r"v(\d+)\.(\d+)", version)
        return (int(match.group(1)), int(match.group(2))) if match else (0, 0)

    # Create a list of tuples and sort by extracted version numbers
    sorted_data = sorted(zip(versions, pqr_files, site_names), key=lambda x: extract_version_number(x[0]))

    # Unzip the sorted tuples back into separate lists
    sorted_versions, sorted_pqr_files, sorted_site_names = zip(*sorted_data)

    return list(sorted_pqr_files), list(sorted_site_names), list(sorted_versions)
            
def extract_version(text):
    """
    Extract the version number from a given text.

    Args:
        text (str): The input text from which the version number needs to be extracted.

    Returns:
        str or None: The extracted version number as a string if found, otherwise None.

    The function uses a regular expression to find all occurrences of version numbers
    in the given text. The pattern `r"\d+\.\d+"` matches any sequence of one or more
    digits, followed by a period, and then one or more digits again. The first match
    is returned as the version number. If no matches are found, the function returns
    None.
    """
    # Regular expression pattern to match version numbers
    version_patterns = [r"\sv\d+\.\d+", r"\d+\.\d+"]
    # Combine patterns into a single regex
    combined_pattern = "|".join(version_patterns)
    # Find all occurrences of version numbers in the text
    matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)

    
    # If at least one match is found, return the first match
    if matches:
        if "v" in matches[0]:            
            return  matches[0].strip()
        else: return  "v" + matches[0].strip()
    # If no matches are found, return None
    else:
        return "v0.0"

def extract_dates(text):    
    
    date_patterns = [
        r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b',  # DD-MM-YYYY or DD/MM/YYYY
        r'\b(?:\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',    # YYYY-MM-DD or YYYY/MM/DD
        r'\b(?:\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{4})\b',  # DD Mon YYYY
        r'\b(?:\d{1,2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\d{4})\b',  # DDMonYYYY
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s(?:\d{1,2},\s\d{4})\b',  # Mon DD, YYYY
        r'\b(?:\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})\b',  # DD Month YYYY
        r'\b(?:\w+\s\d{1,2},\s\d{4})\b',  # Month DD, YYYY
        r'\b(?:\d{2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{2})\b',  # DDMonYY
        r'\b(?:\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2})\b'  # ISO 8601 with time (YYYY-MM-DDThh:mm:ss)
    ]
    # Combine patterns into a single regex
    combined_pattern = "|".join(date_patterns)
    matches = re.findall(combined_pattern, text, flags=re.IGNORECASE)
    extracted_dates = []
    for match in matches:
        try:
            for fmt in (
                "%Y-%m-%d",       # Example: 2024-07-24
                "%d-%m-%Y",       # Example: 24-07-2024
                "%m/%d/%Y",       # Example: 07/24/2024
                "%d %B %Y",       # Example: 24 July 2024
                "%d %b %Y",       # Example: 24 Jul 2024
                "%d%b%Y",         # Example: 24Jul2024
                "%B %d, %Y",      # Example: July 24, 2024
                "%b %d, %Y",      # Example: Jul 24, 2024
                "%d%b%y",         # Example: 01May23 (DDMonYY)
                "%Y-%m-%dT%H:%M:%S"  # Example: 2024-07-24T14:21:54 (ISO 8601 with time)
            ):
                try:                    
                    normalized_date = datetime.strptime(match, fmt).date().strftime("%d%b%Y")
                    extracted_dates.append(normalized_date)
                    break
                except ValueError as ve:
                    continue
        except ValueError:
            continue  # Skip invalid formats
            
    extracted_dates.sort(key=lambda x: datetime.strptime(x, "%d%b%Y"))    
    return extracted_dates
            
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
            
def ingest_pqr_file(tmp_pqr_pdf_path, date_pattern, list_site_names, list_product_names):
    with pdfplumber.open(io.BytesIO(tmp_pqr_pdf_path)) as pdf:
        # Get the first page
        first_page = pdf.pages[0]
        # Extract text from the first page
        first_page_text = first_page.extract_text()
        return extract_reporting_period_product_name_site_name(
            first_page_text, date_pattern, list_site_names, list_product_names
        )

def get_list_of_files(bucket_name, folder_prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)["Contents"]
    return response

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
    
    output = []
    pattern = r"(\d{2}\w{3})(\d{4})_(\d{2}\w{3})(\d{4})"  # Regular expression pattern to match date ranges
    for date_range in date_ranges:
        date_range = date_range.strip()  # Trim leading/trailing whitespace
        match = re.match(pattern, date_range)
        if match:
            start_year = match.group(2)
            end_year = match.group(4)
            output.append(f"{start_year}_{end_year}")
    return output
            
def extract_product_report_version_for_all(bucket_name, upload_folder, date_pattern, list_site_names, list_product_names, input_size_limit, list_size_exceeded_flag):
    """
    This function extracts product report versions from a given source (S3 bucket) for all files matching the specified criteria.

    Args:
        bucket_name (str): The name of the S3 bucket containing the files.
        upload_folder (str): The folder path within the S3 bucket where the files are located.
        date_pattern (str): The date pattern to match in the file names.
        list_site_names (list): A list of site names to match against.
        list_product_names (list): A list of product names to match against.
        input_size_limit (int): The maximum file size limit in MB.
        list_size_exceeded_flag (list): A list to store flags indicating if the file size exceeds the limit.

    Returns:
        tuple: A tuple containing the following values:
            - pqr_file_names (list): A list of file names that were processed.
            - product_list (list): A list of extracted product names.
            - reporting_period_list (list): A list of extracted reporting periods.
            - list_filesite_name (list): A list of extracted site names.
            - list_versions (list): A list of extracted version numbers.
            - list_size_exceeded_flag (list): A list of flags indicating if the file size exceeded the limit.
            - extraction_successful (bool): A boolean indicating whether the extraction was successful or not.

    External Function Calls:
        - get_list_of_files(bucket_name, upload_folder): Retrieves a list of files from the specified S3 bucket and folder.
        - ingest_pqr_file(pqr_file_data, date_pattern, list_site_names, list_product_names): Extracts metadata from the file data.

    External Resource Interactions:
        - Reading files from an S3 bucket: The function retrieves file objects from the specified S3 bucket and folder using `s3.get_object`.

    """
    # Get the list of files from the S3 bucket
    list_of_upload_files = get_list_of_files(bucket_name, upload_folder)

    # Initialize lists to store extracted data
    product_list = []
    reporting_period_list = []
    list_filesite_name = []
    list_versions = []
    pqr_file_names = []

    # Calculate the maximum file size in bytes
    one_mb = 1024 * 1024
    max_size_bytes = input_size_limit * one_mb

    # Initialize the extraction status flag
    extraction_successful = True
    caught_exception = None

    # Check if there are any files to process
    if len(list_of_upload_files) > 0:
        logger.info(f"\n\nextract_product_report_version_for_all ---------START----2---->{list_size_exceeded_flag}")
        for pqr_file in list_of_upload_files:  # Iterate over each file
            pqr_file_key = pqr_file["Key"]
            logger.info(f"\n\nextract_product_report_version_for_all ---------START----2.1---->{list_size_exceeded_flag}")

            # Check if the file is a PDF or DOCX
            if pqr_file_key.endswith(".pdf") or pqr_file_key.endswith(".docx"):
                pqr_file_name = pqr_file_key.split("/")[-1]
                pqr_file_names.append(pqr_file_name)

                logger.info(f'extract_product_report_version_for_all: Extraction of First Page of {pqr_file_name}')

                try:
                    # Construct the file path for the S3 object
                    tmp_pqr_pdf_path = f"{upload_folder}{pqr_file_name}"

                    # Retrieve the file object from S3
                    pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=tmp_pqr_pdf_path)

                    # Read the file data
                    pqr_file_data = pqr_file_obj["Body"].read()

                    # Calculate the file size in MB
                    file_size = round((pqr_file_obj.get('ContentLength', 0) / one_mb), 2)
                    logger.info(f"\nextract_product_report_version_for_all:file_size:-------->\t{file_size} MB")

                    # Check if the file size exceeds the limit
                    if pqr_file_obj.get('ContentLength', 0) > max_size_bytes:
                        list_size_exceeded_flag.append(True)
                        logger.info(f"\nextract_product_report_version_for_all:Error: File '{tmp_pqr_pdf_path}' exceeds the maximum size of {input_size_limit} MB.")
                    else:
                        list_size_exceeded_flag.append(False)
                        logger.info(f"\nextract_product_report_version_for_all:File '{tmp_pqr_pdf_path}' is within the size limit of {input_size_limit} MB.")

                    # Extract metadata from the file data
                    reporting_period, reporting_period_startdate, reporting_period_enddate, product_name, site_name, version_no = ingest_pqr_file(pqr_file_data, date_pattern, list_site_names, list_product_names)
                    variables = [reporting_period, product_name, site_name, version_no]

                    # Validation for missing values.
                    if any(var is None for var in variables):
                        logger.error("Error: One or more output variables are missing or null during extraction process.")
                        extraction_successful = False
                        break  # Exit the loop if an values are missing 
                    else:
                        logger.info(f"\n\nextract_product_report_version_for_all:reporting_period: {reporting_period}")
                        logger.info(f"extract_product_report_version_for_all:product_name: {product_name}")
                        logger.info(f"extract_product_report_version_for_all:site_name: {site_name}")
                        logger.info(f"extract_product_report_version_for_all:version_no: {version_no}")

                    # Store the extracted data in the respective lists
                    if product_name and site_name:
                        product_list.append(product_name)
                        reporting_period_list.append(reporting_period)
                        list_filesite_name.append(site_name)
                        list_versions.append(version_no)
                    
                except Exception as e:
                    logger.error(f'extract_product_report_version_for_all: Unsupported file structure": {str(e)}')
                    extraction_successful = False
                    caught_exception = e
                    break  # Exit the loop if an exception occurs
                    
    # Validation procedure for reporting period list.
    logger.info(f"---------BEFORE----reporting_period_list---->{reporting_period_list}")
    output_reporting_period_list = convert_date_ranges(reporting_period_list)
    logger.info(f"---------AFTER----output_reporting_period_list---->{output_reporting_period_list}")
    
    logger.info(f"extract_product_report_version_for_all ---------END-------->{list_size_exceeded_flag}")
    return pqr_file_names, product_list, output_reporting_period_list, list_filesite_name, list_versions, list_size_exceeded_flag, extraction_successful, caught_exception

#### Read the Mapping column wise
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

def sitev_extract_template(TEMPLATE_DB, template_fullname):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TEMPLATE_DB)

    try:
        response = table.scan(
                    FilterExpression=Attr('template_fullname').eq(template_fullname)
                )

        res = response['Items'][0]
        
        if "ISPR" in template_fullname:
            return res["template_id"], None, None, None, res["template_name"],  None, None
        else:
            return res["template_id"], res["s3_path"], res["s3_excel_path"], res["section_names"], res["template_name"],  res["doc_types"], res["doc_types_full"]

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again later.")


# Standard Library
import base64
import csv
import io
import os
import posixpath
import re
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Third-party Libraries
import boto3
import json
import pandas as pd
import pdfplumber
import PyPDF2
import mammoth
import html2docx
from PIL import Image
from bs4 import BeautifulSoup
from dateutil import parser
from docx import Document as Document_docx
from docx.image.exceptions import UnrecognizedImageError
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile, status
from fastapi.responses import JSONResponse
from loguru import logger
from opensearchpy import (
    NotFoundError,
    OpenSearch,
    OpenSearchException,
    RequestError,
    RequestsHttpConnection
)
from PyPDF2 import PdfReader
from requests_aws4auth import AWS4Auth

# AWS-specific
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config

# Langchain and Related Libraries
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document
from langchain.prompts import PromptTemplate
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch

# Local Application Imports
from config import *
from data import ReportFile, ReportTrackingCompletion
# from models import ReportTrackingCompletion  # Adjust import as needed
from knowbase_utils import (
    check_sync_status,
    create_data_source,
    get_knowledge_base_id,
    retrieve_and_generate,
    sync_data_source
)

load_dotenv()
s3 = boto3.client("s3")
bucket_name = f"{BUCKET_NAME}"

s3_res = boto3.resource('s3')

boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)

bucket_name = f"{BUCKET_NAME}"
pqr_param_json_filename = "pqr_param.json"
input_folder = (
    f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
)
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save

bucket = s3_res.Bucket(bucket_name)

ERROR_MESSAGE = "Oops! It seems there’s a network issue. Please check your connection and try again in a moment."
    
############# Defined Presigned URL for Download ##########################
async def s3_upload(contents: bytes, key: str):
    logger.info(f'Uploading {key} to s3 bucket: {bucket}')
    bucket.put_object(Key=key, Body=contents)

def generate_presigned_urldownload(s3_url: str,  expiration=3600):
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    try:
        expiration = int(expiration)  # Expiration time in seconds
    except ValueError:
        print("Invalid expiration time: must be an integer.")
        return None       
    # Parse the S3 URL to extract the bucket and key
    pattern = r's3://([^/]+)/(.+)'
    match = re.match(pattern, s3_url)
    if not match:
        print("Invalid S3 URL format. Must start with 's3://'.")
        return None
    bucket_name = str(match.group(1)).strip()
    object_key = str(match.group(2)).strip()
    # Generate presigned URL
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 
                    'Key': object_key,
                    'ResponseContentDisposition': 'attachment'},
            ExpiresIn=expiration  # Expiration in seconds
        )
        return presigned_url
    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return None

###############################

def delete_reportRecords(report_file_path, tablename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    try:
        response = table.scan(
           FilterExpression=Attr("report_file_path").eq(report_file_path)
         )
    
        if response['Items']:
            for item in response['Items']:
                table.delete_item(
                      Key={'report_id':item['report_id'], 
                  'created_at':item['created_at']
                 }
            )
            return {f"message: Records of ISPR Report {report_file_path} has been successfully deleted"}
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
def delete_reportqueueRecords(report_file: ReportFile, tablename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    
    try:
        response = table.scan(
           FilterExpression=( Attr("report_id").eq(report_file.report_id)&
                              Attr("file_version").eq(report_file.file_version)
                            )
         )
    
        if response['Items']:
            for item in response['Items']:
                table.delete_item(
                      Key={'report_id':item['report_id'], 
                  'file_version':item['file_version']
                 }
            )
            return {f"message: Records of ISPR Report {report_file.report_file_path} has been successfully deleted in Queue"}
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
##############################
def update_report_locked_status(report_id, timestamp, is_locked, table_name, edit_dates_enabled = False, date_of_edit=None):
    """
    Updates the record status and DateofEdition in a DynamoDB table.

    Args:
        session_id (str): The unique identifier of the session.
        timestamp (str): The timestamp associated with the record.
        is_locked (bool): True if the record should be locked, False otherwise.
        table_name (str): The name of the DynamoDB table.
        date_of_edit (str, optional): The date of edition for the record. 
        If not provided, the DateofEdition column will not be updated.

    Returns:
        dict: The response from the update_item operation.
    """
    logger.info(f"\nupdate_report_locked_status -------START-------is_locked: {is_locked}")
    logger.info(f"\nupdate_report_locked_status -------edit_dates_enabled: {edit_dates_enabled}")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Construct the update expression
    update_expression = "SET locked = :locked_status"
    expression_attribute_values = {":locked_status": is_locked}

    # If date_of_edit is provided, add it to the update expression and expression attribute values
    if edit_dates_enabled:
        logger.info("update_report_locked_status -------edit_dates_enabled")
        update_expression += ", updated_at = :DateofEdition"
        expression_attribute_values[":DateofEdition"] = date_of_edit

    key = {
        'report_id': report_id,
        'created_at': timestamp
    }

    # Update the item in DynamoDB
    response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW"
    )
    logger.info(f"\nupdate_report_locked_status -------END-------is_locked: {is_locked}")
    return response

#######################
# def convert_docx_to_html_mammoth(bucket_name, s3_key):
#     logger.info("\n\nconvert_docx_to_html_mammoth --------------1-------------->")
#     # Create an S3 client
#     s3 = boto3.client('s3')

#     # Download the file from S3
#     obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
#     logger.info("convert_docx_to_html_mammoth --------------2-------------->")
#     docx_bytes = obj['Body'].read()
#     logger.info("convert_docx_to_html_mammoth --------------3-------------->")

#     # Create a file-like object from the bytes data
#     docx_file_like = io.BytesIO(docx_bytes)

#     # Convert docx to HTML using mammoth
#     result = mammoth.convert_to_html(docx_file_like)
#     logger.info("convert_docx_to_html_mammoth --------------4-------------->")
#     # html_content = result.value  # Extract HTML content
#     logger.info(f"convert_docx_to_html_mammoth --------------5-------------->")

#     # return html_content
#     return result

def convert_docx_to_html_mammoth(bucket_name, s3_key):
    logger.info("\n\nconvert_docx_to_html_mammoth --------------1-------------->")
    # Create an S3 client
    s3 = boto3.client('s3')

    # Download the file from S3
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    logger.info("convert_docx_to_html_mammoth --------------2-------------->")
    docx_bytes = obj['Body'].read()
    logger.info("convert_docx_to_html_mammoth --------------3-------------->")

    # Create a file-like object from the bytes data
    docx_file_like = io.BytesIO(docx_bytes)

    # Convert docx to HTML using mammoth
    try:
        result = mammoth.convert_to_html(docx_file_like)
        logger.info("convert_docx_to_html_mammoth --------------4-------------->")
        html_content = result.value  # Extract HTML content
        logger.info(f"convert_docx_to_html_mammoth --------------5-------------->")
    except Exception as e:
        print("Caught IndexError:", e)
        html_content = ""
    return html_content

##############

def save_ispr_html_html2docx(html_content, bucket_name, s3_file_path):
    try:
        logger.info(f"\nsave_ispr_html_html2docx ----------1-----len-----{len(html_content)}")
        # Convert HTML to DOCX in-memory
        docx_stream = io.BytesIO()

        logger.info("\nsave_ispr_html_html2docx ----------2----------")
        # Convert the HTML content to DOCX and save it directly to the byte stream
        # html2docx.html2docx(html_content, title="ISPR Report").save(docx_stream)

        # Call html2docx and get the document object
        # document = html2docx.html2docx(html_content, title="ISPR Report")
        html2docx.html2docx(html_content, docx_stream)
        
        # Save the document to the BytesIO stream
        # document.save(docx_stream)
        
        logger.info("\nsave_ispr_html_html2docx ----------3----------")
        # Rewind the stream to the start for S3 upload
        docx_stream.seek(0)

        logger.info("\nsave_ispr_html_html2docx ----------4----------")
        # Initialize the S3 client
        s3 = boto3.client('s3')

        logger.info("\nsave_ispr_html_html2docx ----------5----------")
        # Upload the in-memory DOCX file to S3
        # s3.upload_fileobj(docx_stream.getvalue(), bucket_name, s3_file_path)

        s3.put_object(
            Body=docx_stream.read(),  # Read the content of the in-memory DOCX stream
            # Body=docx_stream.getvalue(),
            # Body=docx_stream,
            Bucket=bucket_name,
            Key=s3_file_path,
            ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
        logger.info("\nsave_ispr_html_html2docx ----------6----------")
        logger.info(f"\nsave_ispr_html_html2docx ------File successfully uploaded to {bucket_name}/{s3_file_path}")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        
###################
def store_reporttrackingcompletion(interaction: ReportTrackingCompletion, REPORTS_DYNAMOTABLE:str):
    """
    Store the ReportTrackingCompletion interaction in the REPORTS_DYNAMOTABLE DynamoDB table.

    This function performs the following steps:
    1. Delete any existing records with the same ProductName_User combination.
    2. Insert the new IsprTrackingCompletion interaction into the table.
    3. Invoked from update ispr and Generate ispr APIs.
    
    **Steps Carried Out by the Function:**

    1. Delete any existing records with the same `ProductName_User` combination from the DynamoDB table.
       - Query the table using the `ProductName_User-Timestamp-index` to retrieve existing records with the same `ProductName_User`.
       - Delete each retrieved record using the correct partition key (`SessionID`) and sort key (`Timestamp`).
    2. Insert the new `IsprTrackingCompletion` interaction into the DynamoDB table.
       - Convert the `IsprTrackingCompletion` object to a dictionary using the `dict()` method.
       - Insert the dictionary as a new item in the DynamoDB table using the `put_item()` method.

    **External Function Calls:**

    - `boto3.resource('dynamodb')`: Used to create a DynamoDB resource.
    - `table.query()`: Used to query the DynamoDB table with a specific index and condition expression.
    - `table.delete_item()`: Used to delete an item from the DynamoDB table based on the provided primary key.
    - `table.put_item()`: Used to insert a new item into the DynamoDB table.

    Args:
        interaction (IsprTrackingCompletion): The IsprTrackingCompletion interaction to be stored.
        tablename (str): The name of the DynamoDB table.

    Returns:
        dict: A success message if the interaction is stored successfully.

    Raises:
        HTTPException: If an exception occurs during the storage process.

    External Function Calls:
        - boto3.resource('dynamodb')
        - table.query()
        - table.delete_item()
        - table.put_item()
    """
    logger.info("\nstore_isprtrackingcompletion ---------- START --------")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    ############## First delete all previous record from the same ProductName&UserID
    response = table.query(
                # IndexName = "report_id-file_version-index",
                KeyConditionExpression=Key('report_id').eq(interaction.report_id),
                FilterExpression=(Attr('file_version').eq(interaction.file_version)),
              )

    # Step 2: Delete existing records using correct keys
    for item in response.get('Items', []):
        table.delete_item(Key={
            "report_id": item["report_id"],  # Correct partition key
            "created_at": item["created_at"]   # Correct sort key
        })
    logger.info("store_reportcompletion ---------- Deleted --------")
    #################### Insert New Records
    try:
        item = interaction.dict()
        ############ delete all previous records about the interaction.ProductName_User
        table.put_item(Item=item)
        logger.info("store_reportcompletion ---------- END --------Report Edition interaction stored successfully")
        return {"message": "Report-Edition interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


################### HTML to DOCX ###################
def get_image_size(image_bytes):
    """Get the width of the image in inches, maintaining aspect ratio."""
    with Image.open(BytesIO(image_bytes)) as img:
        width, height = img.size  # Get size in pixels
        dpi = 96  # Default DPI for Word
        return width / dpi, height / dpi  # Convert to inches
    
def set_table_borders(table):
    """Apply visible borders to a table in Word."""
    tbl = table._element  # Get XML element of table
    tbl_pr = tbl.find(qn("w:tblPr"))

    # Ensure table properties exist
    if tbl_pr is None:
        tbl_pr = OxmlElement("w:tblPr")
        tbl.insert(0, tbl_pr)

    # Remove any existing border settings
    tbl_borders = tbl_pr.find(qn("w:tblBorders"))
    if tbl_borders is not None:
        tbl_pr.remove(tbl_borders)

    # Add new borders
    tbl_borders = OxmlElement("w:tblBorders")

    for border_name in ["top", "left", "bottom", "right", "insideH", "insideV"]:
        border = OxmlElement(f"w:{border_name}")
        border.set(qn("w:val"), "single")  # Border type
        border.set(qn("w:sz"), "6")  # Border thickness
        border.set(qn("w:space"), "0")
        border.set(qn("w:color"), "000000")  # Black color
        tbl_borders.append(border)

    tbl_pr.append(tbl_borders)  # Attach new borders to table properties


def save_ispr_html(bucket_name, html_content, target_filename):
    """
    Convert an HTML string to a DOCX file and upload it to an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket to upload the DOCX file to.
        html_content (str): The HTML content to be converted to DOCX format.
        target_filename (str): The filename for the DOCX file to be saved in the S3 bucket.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the conversion or upload process.

    This function uses BeautifulSoup to parse the HTML content and convert it to a DOCX document
    using the python-docx library. It handles various HTML elements such as headings, paragraphs,
    images (including base64-encoded images), tables, line breaks, and bold text. The resulting
    DOCX file is then uploaded to the specified S3 bucket with the given target filename.
    """
    try:
        # Convert an HTML string to a DOCX file and upload it to S3.
        logger.info(f"\nsave_ispr_html ----------- 1 ------{len(html_content)}-----")

        # Initialize S3 client
        s3 = boto3.client("s3")

        # Parse HTML using BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")
        # logger.info(f"\nsave_ispr_html ----------- 2 -----------")
        doc = Document_docx()
        # logger.info(f"\nsave_ispr_html ----------- 3 -----------")

        # Iterate over HTML elements and convert them
        for element in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "img", "table", "br", "b", "strong"]):
            try:
                # logger.info(f"\nsave_ispr_html ----------- 3.1 -----------")

                if element.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                    logger.info(f"\nsave_ispr_html ----------- 3.2 -----------")
                    doc.add_heading(element.get_text(strip=True), level=int(element.name[1]))  # Add headings
                elif element.name == "p":
                    logger.info(f"\nsave_ispr_html ----------- 3.3 -----------")
                    paragraph = doc.add_paragraph()
                    for child in element.contents:
                        if child.name == "br":
                            run = paragraph.add_run()
                            run.add_break()
                        elif child.name in ["b", "strong"]:
                            run = paragraph.add_run(child.get_text(strip=True))
                            run.bold = True
                        else:
                            paragraph.add_run(child.string)
                elif element.name == "img":
                    logger.info(f"\nsave_ispr_html ----------- 3.4 -----------")
                    img_src = element["src"]

                    if img_src.startswith("data:image"):  # Handle base64 encoded images
                        logger.info(f"\nsave_ispr_html ----------- 3.5 -----------")
                        format, encoded = img_src.split(";base64,")
                        img_ext = format.split("/")[-1]  # Extract image format
                        img_bytes = base64.b64decode(encoded)  # Decode base64 image data

                        # Get original image size
                        width, height = get_image_size(img_bytes)

                        # Add picture to DOCX with the original size
                        doc.add_picture(BytesIO(img_bytes), width=Inches(width))

                elif element.name == "table":
                    logger.info(f"\nsave_ispr_html ----------- 3.6 -----------")
                    rows = element.find_all("tr")
                    if rows:
                        logger.info(f"\nsave_ispr_html ----------- 3.7 -----------")
                        cols = rows[0].find_all(["th", "td"])  # Get column count from first row
                        table = doc.add_table(rows=len(rows), cols=len(cols))  # Create table with correct number of rows and columns
                        set_table_borders(table)  # Apply borders
                        logger.info(f"\nsave_ispr_html ----------- 3.8 -----------")

                        for r_idx, row in enumerate(rows):
                            logger.info(f"\nsave_ispr_html ----------- 3.9 -----r_idx: {r_idx}------")
                            cells = row.find_all(["th", "td"])

                            # Check if the number of cells matches the number of columns
                            if len(cells) != len(cols):
                                logger.warning(f"save_ispr_html: Number of cells in row {r_idx} does not match the number of columns. Skipping row.")
                                continue

                            for c_idx, cell in enumerate(cells):
                                # Check if the current cell index is within the number of columns
                                if c_idx < len(cols):
                                    logger.info(f"\nsave_ispr_html ----------- 3.10 ----c_idx: {c_idx} \tr_idx: {r_idx}-------")
                                    table.cell(r_idx, c_idx).text = cell.get_text(strip=True)  # Add text to each cell
                                    logger.info(f"\nsave_ispr_html ----------- 3.11 ----c_idx: {c_idx} \tr_idx: {r_idx} -------")
                                else:
                                    logger.warning(f"save_ispr_html: ----------- 3.12 ----Too many cells in row {r_idx}. Skipping additional cells.")
                                    break

                elif element.name == "br":
                    logger.info(f"\nsave_ispr_html ----------- 3.13 -----------")
                    doc.add_paragraph().add_run().add_break()  # Add line break

            # Handle the UnrecognizedImageError exception
            except UnrecognizedImageError as uie:
                error_message = f"\nsave_ispr_html ----------- 3.14 -----------\nError processing image: {uie}"
                logger.error(error_message)
                # You can log additional information or raise a custom exception
                continue

            except Exception as ex:
                error_message = f"\nsave_ispr_html ----------- 3.15 ----------- \nError processing image: {ex}"
                logger.error(error_message)
                continue

        logger.info("\nsave_ispr_html ----------- 4 -----------")
        # Save DOCX to memory
        docx_buffer = BytesIO()
        doc.save(docx_buffer)
        docx_buffer.seek(0)  # Reset buffer pointer
        logger.info("\nsave_ispr_html ----------- 5 -----------")

        # Upload DOCX file to S3
        s3.put_object(Body=docx_buffer.getvalue(), Bucket=bucket_name, Key=target_filename, ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        logger.info("\nsave_ispr_html ----------- 6 -----------")
        logger.info(f"DOCX file successfully saved to s3://{bucket_name}/{target_filename}")
    except Exception as e:
        logger.error(f"Error occurred while saving: {str(e)}", exc_info=True)
        logger.exception(e)


def get_created_at_for_report(report_id: str, table_name: str = REPORTS_DYNAMOTABLE) -> str:
    """
    Retrieves the 'created_at' (sort key) for the latest version of the report from DynamoDB.

    Args:
        report_id (str): The primary key of the report.
        table_name (str): Name of the DynamoDB table (default: REPORTS_DYNAMOTABLE).

    Returns:
        str: The 'created_at' timestamp of the latest report version.

    Raises:
        HTTPException: If no matching report is found.
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        response = table.query(
            KeyConditionExpression=Key('report_id').eq(report_id),
            ScanIndexForward=False,
            Limit=1
        )

        items = response.get('Items', [])
        if not items:
            logger.info(f"No item found for the given report_id: {report_id}")
            raise HTTPException(status_code=404, detail=f"No report found with report_id: {report_id}")

        created_at = items[0]['created_at']
        logger.info(f"Fetched created_at for report_id={report_id}: {created_at}")
        return created_at

    except Exception as e:
        logger.error(f"Error retrieving created_at for report_id={report_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving report data: {str(e)}")


def get_report_tracking_completion(
    report_id: str,
    table_name: str = REPORTS_DYNAMOTABLE
) -> ReportTrackingCompletion:
    """
    Retrieves the latest report record from DynamoDB and returns it as a ReportTrackingCompletion object.

    Args:
        report_id (str): The primary key of the report.
        table_name (str): Name of the DynamoDB table (default: REPORTS_DYNAMOTABLE).

    Returns:
        ReportTrackingCompletion: Pydantic model of the latest report.

    Raises:
        HTTPException: If no matching report is found or on error.
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        response = table.query(
            KeyConditionExpression=Key('report_id').eq(report_id),
            ScanIndexForward=False,  # Get latest item
            Limit=1
        )

        items = response.get('Items', [])
        if not items:
            logger.info(f"No item found for the given report_id: {report_id}")
            raise HTTPException(status_code=404, detail=f"No report found with report_id: {report_id}")

        item = items[0]

        # Ensure 'completion_detail_section' is set to None for now
        item['completion_detail_section'] = None

        logger.info(f"Fetched and preparing model for report_id={report_id}")

        report_model = ReportTrackingCompletion(**item)
        return report_model

    except Exception as e:
        logger.error(f"Error retrieving report for report_id={report_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving report data: {str(e)}")


# --- Upload Utility Functions START ---
def generate_timestamp() -> str:
    """
    Generate current timestamp string for versioning filenames.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_s3_key(uploaded_filename: str, report_file_path: Optional[str], report_id: str, created_at: str, REPORTS_DYNAMOTABLE:str) -> str:
    """
    Determine the final S3 key (path) for uploading the file.

    If report_file_path is provided, validates filename consistency.
    """
    if not uploaded_filename:
        raise HTTPException(status_code=400, detail="Uploaded filename is empty or missing")

    if report_file_path:
        expected_filename = os.path.basename(report_file_path)
        if uploaded_filename != expected_filename:
            update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE, True, None)
            raise HTTPException(
                status_code=400,
                detail=f"Uploaded file name '{uploaded_filename}' does not match expected file name '{expected_filename}' from report_file_path"
            )
        return report_file_path
    else:
        logger.info(f"get_s3_key: No report_file_path provided. Defaulting upload to {OUTPUTS_PATH} folder.")
        return f"{OUTPUTS_PATH}{uploaded_filename}"


def backup_existing_file(s3_res, bucket_name: str, final_s3_key: str, extension_name: str = None) -> None:
    """
    Backup the existing file by copying it to a timestamped or custom-named key in S3.
    The new uploaded file will still overwrite the original key after backup.

    Args:
        s3_res: Boto3 S3 resource object.
        bucket_name: S3 bucket name.
        final_s3_key: Existing S3 object key to be backed up.
        extension_name: Optional custom extension name to use instead of timestamp.
    """
    timestamp_or_extension = extension_name if extension_name else generate_timestamp()

    original_file_name = os.path.basename(final_s3_key)
    name_part, ext_part = os.path.splitext(original_file_name)
    backup_file_name = f"{name_part}_{timestamp_or_extension}{ext_part}"

    # Use posixpath.join for S3 key safety
    backup_file_key = posixpath.join(os.path.dirname(final_s3_key), backup_file_name)

    try:
        s3_res.Object(bucket_name, backup_file_key).copy_from(
            CopySource={'Bucket': bucket_name, 'Key': final_s3_key}
        )
        logger.info(f"Backup created successfully: {backup_file_key}")
    except Exception as e:
        logger.error(f"Failed to create backup for {final_s3_key}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to backup existing file on S3.")


def handle_s3_file_versioning(s3_res, s3_client, bucket_name, final_s3_key):
    """
    Handle versioning: if file exists, create backup; otherwise do nothing.

    Returns:
        The same final_s3_key to upload into.
    """
    logger.info("handle_s3_file_versioning: ----- START -----")

    try:
        s3_client.head_object(Bucket=bucket_name, Key=final_s3_key)
        logger.info(f"Existing file found at {final_s3_key}. Creating backup...")
        backup_existing_file(s3_res, bucket_name, final_s3_key, extension_name="backup")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info("No existing file found. No backup required.")
        else:
            logger.error(f"S3 head_object error: {str(e)}")
            raise HTTPException(status_code=500, detail="S3 error during file existence check.")
    except Exception as e:
        logger.error(f"Unexpected versioning error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error in versioning.")

    logger.info("handle_s3_file_versioning: ----- END -----")
    return final_s3_key  # Always return original key


async def upload_file_to_s3(file: UploadFile, key: str):
    """
    Upload file contents to S3.

    Args:
        file: Uploaded file object.
        key: Target S3 object key.
    """
    try:
        logger.info(f"Starting S3 upload for: {file.filename}")
        contents = await file.read()

        if not contents:
            logger.warning("Uploaded file content is empty.")
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        await s3_upload(contents=contents, key=key)
        logger.info("File successfully uploaded to S3.")

    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail="S3 upload failed.")
    except Exception as e:
        logger.error(f"Unexpected upload error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during upload.")

# --- Upload Utility Functions END ---
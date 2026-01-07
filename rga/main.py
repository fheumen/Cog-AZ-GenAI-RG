###############with app
from config import *
###############
from uuid import uuid4
import uuid
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import magic
import uvicorn
import re
import os
import csv
import boto3
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from typing import List
import json
from pydantic import BaseModel, ValidationError

from datetime import datetime
import time
#from const_aws import *
import fitz  # PyMuPDF
import pdfplumber
import io
from docx import Document as Document_docx
from docx.shared import Inches
import pandas as pd
import tiktoken
import json
import ast
from collections.abc import Iterable
#from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from typing import Optional, List, Dict
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError, BotoCoreError
from collections import defaultdict
from langchain_aws import ChatBedrock
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain.memory import ConversationBufferMemory
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_community.chat_message_histories.dynamodb import DynamoDBChatMessageHistory
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch
from langchain_community.embeddings import BedrockEmbeddings
from langchain.docstore.document import Document
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth
from langchain.chains.summarize import load_summarize_chain
from langchain.prompts import PromptTemplate
from fastapi import FastAPI, HTTPException, Response, UploadFile, status, File, Form, Depends
from fastapi.responses import FileResponse, JSONResponse
import io
from loguru import logger
import requests
from fastapi.middleware.cors import CORSMiddleware
from tempfile import NamedTemporaryFile
from PIL import Image
# from opensearch_client import get_oss_client, get_vector_db_client

#########################################################################################
################ Change to be uncomment for making api token worked ###################
from auth.auth import auth_router
from auth.utils import verify_token
#########################################################################################


###############with app
from chathistory import store_interaction
                          
from utils import (generate_technical_error_message,  save_ispr,  store_isprtrackingcompletion, retrieve_context, 
                           get_list_of_files, check_create_dir, get_mapping_list, get_abs_summarize,
                          extract_reporting_period_product_name_site_name, extract_text_tables_images_by_sections, ingest_pqr_file, ingest_phase_1,
                          opensearch_document_exists, opensearch_insert_docs, save_chunks_to_json, save_pqr_parameter_to_json, Ingest_PQR,
                          ispr_generation,  extract_pdf_contents, extract_text_from_word,get_file_type, generate_prompt, delete_s3_folder, 
                          delete_incoming_files, extract_version, sort_versions, extract_latest_timestamp_ispr_status, 
                          check_ispr_status, store_pqr_status, update_ispr_edition_status, update_ispr_edit_status, save_ispr_report_to_s3, 
                          Ingest_SIV, sitev_generation, sitev_extract_template, sitev_generate_report_from_template, get_mapping_list_sv, get_document_typ, get_document_fulltyp,
                          convert_stringtable_to_df, load_excel_from_s3)


from chat_model_ini import ReportGeneration

from data import (
    QueryRequest, QnaAnswer, AnswerRequest, User, UserTemplate, Query, RequestQuery, Citation, 
    QuickReply, Result, QueryResponse, FeedbackDisplayOptions, Feedback, ChatInteraction, ChatMetadata, ChatHistorySearchRequest, FeedbackRequest,
    IngestResult, IngestResponse, IsprTrackingSection, IsprTrackingCompletion, IsprTrackingWelcomePageAllProduct, IsprUpdatingPage, IsprFile, IsprSelectForEditionOutput, IsprTrackingProduct, User_context, ProductVersion
)

from chat_message_history import ChatHistory
from scheduler import start_job, scheduler

from config import config_router
from chathistory import chat_history_router
from qna.qna import qna_router
from dashboard.dashboard import dashboard_router
from report_creation.create_report import create_report_router
from report_edition.report_edition import report_edition_router
from template.template import template_router 
from users.users import users_router
from template_edit.template_edit import template_edit_router 

###############
app = FastAPI()
###############with app
app.include_router(config_router, prefix="/load", tags=["Config"]) 
# app.include_router(chat_history_router, prefix="/chat", tags=["Chat history"])
#########################################################################################
################# Change to be uncomment for making api token worked ###################
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
# app.include_router(config_router, prefix="/load", tags=["Config"]) 
app.include_router(chat_history_router, prefix="/chat", tags=["Chat history"],dependencies=[Depends(verify_token)])
app.include_router(qna_router, prefix="/qna", tags=["Qna"],dependencies=[Depends(verify_token)])
app.include_router(dashboard_router, prefix="/dashboard", tags=["Dashboard"],dependencies=[Depends(verify_token)])
app.include_router(create_report_router, prefix="/create", tags=["Report Creation"],dependencies=[Depends(verify_token)])
app.include_router(report_edition_router, prefix="/edit", tags=["Report Edition"],dependencies=[Depends(verify_token)])
app.include_router(template_router, prefix="/template", tags=["Template Master"], dependencies=[Depends(verify_token)])
app.include_router(users_router, prefix="/users", tags=["Users Master"], dependencies=[Depends(verify_token)])
app.include_router(template_edit_router, prefix="/template", tags=["Template Edit"], dependencies=[Depends(verify_token)])
#########################################################################################

# Get the absolute path to the static folder
static_folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
# Mount the static folder
app.mount("/static", StaticFiles(directory=static_folder_path), name="static")

s3 = boto3.client("s3")
textract_client = boto3.client('textract', region_name='us-east-1')
bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

session = boto3.session.Session()
client = session.client(service_name="secretsmanager", region_name="us-east-1")

# Get environment variables      
secret_name = os.getenv("secret_name")

# Retrieve secret value
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
secret = json.loads(get_secret_value_response["SecretString"])

DEFAULT_N_MINS = int(secret.get("DEFAULT_N_MINS", 0))
DEFAULT_N_QUEUE_MINS = int(secret.get("DEFAULT_N_QUEUE_MINS", 0))
INPUT_SIZE_LIMIT =  int(secret.get("INPUT_SIZE_LIMIT", 0))
KBNAME_MAX_SIZE = int(secret.get("KBNAME_MAX_SIZE", 0))
# logger.info(f"\n\nmain.py \nDEFAULT_N_MINS: \t{DEFAULT_N_MINS}")
# logger.info(f"\n\nmain.py \nDEFAULT_N_QUEUE_MINS: \t{DEFAULT_N_MINS}")
# logger.info(f"INPUT_SIZE_LIMIT: \t{INPUT_SIZE_LIMIT}")

list_site_names = ast.literal_eval(secret.get("LIST_SITE_NAMES", "[]"))
list_product_names = ast.literal_eval(secret.get("LIST_PRODUCT_NAMES", "[]"))
# logger.info(f"\nlist_site_names: \t{list_site_names}")
# logger.info(f"\nlist_product_names: \t{list_product_names}")

input_folder = (
    f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
)
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save

KB = 1024
MB = 1024 * KB

SUPPORTED_FILE_TYPES = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'application/pdf': 'pdf'
}


# Regular expression to match the date range (e.g., "14 Nov 2022 – 13 Nov 2023" or "14 Nov 2022 to 13 Nov 2023")
date_pattern = r"""
    (\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})  # First date
    .+?                                                            # Any text in between
    (\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})  # Second date
"""
# Regular expression to match header and the footer, in order to avoid extracting them
header_pattern = r"REP-\d{7}\sv\d{1}.\d{1}\sStatus: Approved Approved Date:\s\d{1,2}\s\w+\s\d{4}\sPage\s\d{1,3}\sof\s\d{1,3}"
footer_pattern = r"Check this is the latest version of the document before use."

app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"],
   allow_credentials=True,
   allow_methods=["*"],
   allow_headers=["*"],
) 

@app.get("/")
async def health_check():
    return {"status": "Healthy"}

# Start scheduler when FastAPI application starts
@app.on_event("startup")
def startup():
    try:
        logger.info(f"\n\nStarting scheduler...\t{DEFAULT_N_MINS} \t{REPORTS_DYNAMOTABLE} \t{REPORTSQUEUE_DYNAMOTABLE} \t{TEMPLATEMASTER_DYNAMOTABLE}------------->")
        start_job(DEFAULT_N_MINS, DEFAULT_N_QUEUE_MINS, REPORTS_DYNAMOTABLE, REPORTSQUEUE_DYNAMOTABLE, TEMPLATEMASTER_DYNAMOTABLE, KNOWLEDGEBASE_NAME, MODEL_ID, BUCKET_NAME, output_folder, input_folder, list_site_names, list_product_names, date_pattern, header_pattern, footer_pattern, EMAIL_SENDER, KBNAME_MAX_SIZE, KNOWLEDGEBASE_NAME_QnA) 
    except Exception as e:
        logger.error(f'startup error": {str(e)}')

@app.on_event("shutdown")
def shutdown():
    try:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
    except Exception as e:
        logger.error(f'shutdown error": {str(e)}')
        
        
# ----------------------------------------------------
# Move the below functions to utility, 
# Use secrets manager for the model id and other constants. 
# ----------------------------------------------------

# Bedrock model ID
BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

def start_textract_analysis(s3_bucket, s3_object_key):
    response = textract_client.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': s3_bucket, 'Name': s3_object_key}},
        FeatureTypes=['LAYOUT']
    )
    return response['JobId']


def get_textract_output(job_id):
    job_status = 'IN_PROGRESS'
    while job_status == 'IN_PROGRESS':
        time.sleep(5)
        response = textract_client.get_document_analysis(JobId=job_id)
        job_status = response['JobStatus']
        print(f"Textract job status: {job_status}")

    if job_status != 'SUCCEEDED':
        print(f"Textract job {job_id} failed.")
        return None

    blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract_client.get_document_analysis(JobId=job_id)
        blocks.extend(response['Blocks'])
        next_token = response.get('NextToken')
        if not next_token:
            break
    return {'Blocks': blocks}


def group_blocks_by_page(textract_data):
    """Groups Textract blocks by their page number."""
    page_map = {}
    for block in textract_data:
        if 'Page' in block:
            page = block['Page']
            if page not in page_map:
                page_map[page] = []
            page_map[page].append(block)
    return page_map


def get_bedrock_coordinates(textract_data, user_prompt):
    prompt = f"""
You are an expert at parsing Amazon Textract layout data.

Task:
- Extract bounding boxes and page numbers for content that matches the user request.
- Only consider BLOCK_TYPE 'FIGURE', 'TABLE', 'PARAGRAPH'.
- If nothing matches, return an empty list [].

User request: "{user_prompt}"
Textract Data: {json.dumps(textract_data)}
Expected JSON output: [{{"type":"TABLE","page":2,"bounding_box":{{"Left":0.1,"Top":0.2,"Width":0.3,"Height":0.4}}}}]
"""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": prompt}]
    })

    try:
        response = bedrock_client.invoke_model(
            body=body,
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_body = json.loads(response.get('body').read())
        llm_response_text = response_body.get('content')[0].get('text')

        start_index = llm_response_text.find('[')
        end_index = llm_response_text.rfind(']') + 1
        if start_index == -1 or end_index == -1:
            return None
        return json.loads(llm_response_text[start_index:end_index])
    except Exception as e:
        print(f"Error invoking Bedrock: {e}")
        return None


def combine_bounding_boxes(elements):
    page_boxes = {}
    for element in elements:
        page = element['page']
        box = element['bounding_box']
        if page not in page_boxes:
            page_boxes[page] = dict(box)
        else:
            existing = page_boxes[page]
            left = min(existing['Left'], box['Left'])
            top = min(existing['Top'], box['Top'])
            right = max(existing['Left'] + existing['Width'], box['Left'] + box['Width'])
            bottom = max(existing['Top'] + existing['Height'], box['Top'] + box['Height'])
            page_boxes[page] = {
                'Left': left,
                'Top': top,
                'Width': right - left,
                'Height': bottom - top
            }
    return page_boxes


def crop_and_save_image_locally(pdf_path, page_number, coordinates, output_path):
    try:
        doc = fitz.open(pdf_path)
        page = doc.load_page(page_number - 1)

        zoom = 300 / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=matrix)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        width, height = img.size
        left = coordinates['Left'] * width
        upper = coordinates['Top'] * height
        right = (coordinates['Left'] + coordinates['Width']) * width
        lower = (coordinates['Top'] + coordinates['Height']) * height

        cropped_img = img.crop((left, upper, right, lower))
        cropped_img.save(output_path, 'PNG')
        print(f"Image saved locally: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error cropping image: {e}")
        return None
    finally:
        if 'doc' in locals():
            doc.close()


def upload_to_s3(local_file_path, s3_bucket, s3_key):
    try:
        s3.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"Uploaded {local_file_path} → s3://{s3_bucket}/{s3_key}")
        return f"s3://{s3_bucket}/{s3_key}"
    except ClientError as e:
        print(f"S3 upload failed: {e}")
        return None


# ----------------------------------------------------
#  Remove the boundaries after the integration. 
# ----------------------------------------------------
def extract_figure_from_s3_pdf(
    s3_file_location: str,
    search_text: str,
    left_margin: float | None = None,
    above_title: float | None = None,
    right_margin: float | None = None,
    bottom_margin: float | None = None,
    output_s3_location: str | None = None,
) -> str | None:
    s3_bucket_in, s3_key_in = None, None
    tmp_pdf_path, tmp_image_path = None, None

    # Parse input S3
    if s3_file_location.startswith("s3://"):
        parts = s3_file_location[5:].split("/", 1)
        if len(parts) == 2:
            s3_bucket_in, s3_key_in = parts
        else:
            print(f"Invalid S3 input: {s3_file_location}")
            return None
    else:
        print("Input must be S3 URI")
        return None

    try:
        # Download PDF locally
        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf_path = tmp_pdf.name
        s3.download_file(s3_bucket_in, s3_key_in, tmp_pdf_path)

        # Textract
        job_id = start_textract_analysis(s3_bucket_in, s3_key_in)
        textract_output = get_textract_output(job_id)
        if not textract_output:
            return None

        # Process
        page_map = group_blocks_by_page(textract_output['Blocks'])
        for page_number, blocks in page_map.items():
            found = get_bedrock_coordinates(blocks, search_text)
            if not found:
                continue
            page_boxes = combine_bounding_boxes(found)
            if page_number not in page_boxes:
                continue

            combined_box = page_boxes[page_number]
            with NamedTemporaryFile(delete=False, suffix=".png") as tmp_img:
                tmp_image_path = tmp_img.name
            local_path = crop_and_save_image_locally(tmp_pdf_path, page_number, combined_box, tmp_image_path)
            if not local_path:
                return None

            # Upload to S3 if output provided
            if output_s3_location and output_s3_location.startswith("s3://"):
                parts = output_s3_location[5:].split("/", 1)
                s3_bucket_out, s3_key_out = parts
                return upload_to_s3(local_path, s3_bucket_out, s3_key_out)
            else:
                print("No output S3 specified. Returning local path.")
                return local_path

        print("No matching content found.")
        return None

    finally:
        if tmp_pdf_path and os.path.exists(tmp_pdf_path):
            os.remove(tmp_pdf_path)
        if tmp_image_path and os.path.exists(tmp_image_path):
            os.remove(tmp_image_path)
            
from fastapi import Query
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

S3_BUCKET_NAME = "azcdi-us-ops-report-ds-dev"
S3_REGION_NAME = "us-east-1"

try:
    s3_client = boto3.client(
        's3',
        region_name=S3_REGION_NAME
    )
except Exception as e:
    # Handle initialization error outside the request path
    print(f"Boto3 S3 Client Initialization Error: {e}")
    # Set to None and rely on the request handler to raise 500
    s3_client = None


def synchronous_file_iterator(body_stream):
    """A synchronous generator function to read the Boto3 stream chunk-by-chunk."""
    # Boto3's Body object acts like a file-like object with synchronous read()
    chunk_size = 65536
    while True:
        chunk = body_stream.read(chunk_size)
        if not chunk:
            break
        yield chunk

@app.get("/view-document")
async def view_document(
    key: str = Query(..., description="The S3 object key (path) to retrieve.")
):
    """
    Secure server-side proxy to fetch and stream an S3 document using synchronous Boto3.
    The synchronous calls are automatically run in a threadpool by FastAPI/Starlette.
    Endpoint: /view-document?key=path/to/my-private-file.pdf
    """
    if not key:
        raise HTTPException(status_code=400, detail="Missing document key.")
    if s3_client is None:
        raise HTTPException(status_code=500, detail="S3 client failed to initialize.")
    try:
        # Use run_in_threadpool to execute the synchronous Boto3 call without blocking the event loop
        s3_object_response = await run_in_threadpool(
            s3_client.get_object,
            Bucket=S3_BUCKET_NAME,
            Key=key
        )
        
        # Extract metadata for headers
        content_type = s3_object_response.get("ContentType", "application/octet-stream")
        content_length = s3_object_response.get("ContentLength")
        
        # --- 3. Stream the Content ---
        # Note: StreamingResponse automatically handles running a synchronous generator 
        # (like the one returned by synchronous_file_iterator) in a separate thread.
        
        body_stream = s3_object_response['Body']
        
        headers = {
            "Content-Type": content_type,
            # Use 'inline' to tell the browser to display it in the window/viewer
            "Content-Disposition": f"inline; filename=\"{os.path.basename(key)}\"",
        }
        if content_length is not None:
             headers["Content-Length"] = str(content_length)
        
        return StreamingResponse(
            synchronous_file_iterator(body_stream),
            headers=headers,
            status_code=200
        )

    # Catch specific S3 exceptions from botocore
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey':
            raise HTTPException(status_code=404, detail="Document not found or key is incorrect.")
        else:
            print(f"S3 access error (Code: {error_code}): {e}")
            # Log the full exception in a real application
            raise HTTPException(status_code=500, detail="Could not retrieve document due to an AWS error.")
    except Exception as e:
        print(f"Server or streaming error: {e}")
        raise HTTPException(status_code=500, detail="Could not retrieve document due to a server error.")


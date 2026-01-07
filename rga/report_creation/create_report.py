from report_creation.utils import (store_report_queue,  convert_date_ranges, extract_product_report_version_for_all, handle_validation_failure, delete_s3_folder, get_mapping_list, validate_pqr_files, 
                                   sort_versions, report_id_file_version_extracted, update_report_queue_status, update_report_queue_overwrite, update_report_queue_uploadfolder, sitev_extract_template, extract_reporting_period_product_name_site_name, 
                                   get_list_of_files, extract_version, extract_dates, delete_reportRecords)
from config import REPORTSQUEUE_DYNAMOTABLE, TEMPLATEMASTER_DYNAMOTABLE, INTPUTS_PATH, OUTPUTS_PATH, BUCKET_NAME, MAPPING_FILE_PATH, SHEET_NAME_MAPPING, REPORTS_DYNAMOTABLE
from fastapi import FastAPI, HTTPException, Response, UploadFile, status, File, Form, Depends
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from datetime import datetime
from fastapi import APIRouter
from collections import defaultdict
from typing import Optional, List, Dict, Sequence
import uuid
import random
import string
import json
import os
import boto3
from uuid import uuid4
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from datetime import datetime
from typing import Optional, List, Dict
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from loguru import logger
import ast
# import ast
###############with app
from data import  ReportTrackingCompletion, ReportTrackingSection, ReportTrackingWelcomePageAllProduct, ReportQueue, FileNameTypePair
###############

# Status constants
STATUS_CONFLICT = "conflict"

# Message constants
MESSAGE_CONFLICT = "A similar report has been created already. Choose an option:"

MESSAGE_REQUEST_QUEUED = (
    "Your request for report generation has been queued.\n"
    "Once your report is ready, you will be notified by email."
)

# Action choices
ACTION_OVERWRITE = "Overwrite"
ACTION_CANCEL = "Cancel"
ACTION_NEW_VERSION = "Create new_version"

CONFLICT_CHOICES = [ACTION_OVERWRITE, ACTION_CANCEL, ACTION_NEW_VERSION]

create_report_router = APIRouter()
# Initialize FastAPI app
#router = APIRouter()
boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)
# dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
# table = dynamodb.Table(REPORTS_DYNAMOTABLE)

bucket_name = f"{BUCKET_NAME}"
pqr_param_json_filename = "pqr_param.json"
input_folder = (
    f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
)
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save

s3_res = boto3.resource('s3')
bucket = s3_res.Bucket(bucket_name)

session = boto3.session.Session()
client = session.client(service_name="secretsmanager", region_name="us-east-1")

# Get environment variables      
secret_name = os.getenv("secret_name")

# Retrieve secret value
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
secret = json.loads(get_secret_value_response["SecretString"])

INPUT_SIZE_LIMIT =  int(secret.get("INPUT_SIZE_LIMIT", 0))
# logger.info(f"INPUT_SIZE_LIMIT: \t{INPUT_SIZE_LIMIT}")

list_site_names = ast.literal_eval(secret.get("LIST_SITE_NAMES", "[]"))
list_product_names = ast.literal_eval(secret.get("LIST_PRODUCT_NAMES", "[]"))
# logger.info(f"\nlist_site_names: \t{list_site_names}")
# logger.info(f"\nlist_product_names: \t{list_product_names}")


date_pattern = r"""
    (\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})  # First date
    .+?                                                            # Any text in between
    (\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})  # Second date
"""

async def s3_upload(contents: bytes, key: str):
    logger.info(f'Uploading {key} to s3 bucket: {bucket}')
    bucket.put_object(Key=key, Body=contents)


@create_report_router.post("/report")
async def create_report(files: List[UploadFile],
                        # file_types:  Optional[List[str]] = Form(None),
                        userId: Optional[str] = Form(None),
                        emailId: Optional[str] = Form(None),
                        name: Optional[str] = Form(None),
                        sessionId: Optional[str] = Form(None), 
                        template_fullname: Optional[str] = Form(None), 
                        # filewithType: Optional[List[FileNameTypePair]] = Form(None),
                        filewithType: Optional[str] = Form(None),
                        action: Optional[str] = Form(None)
                       ):
    """
    Create a new report request or handle existing report conflicts.

    Args:
        files (List[UploadFile]): List of files to be uploaded for the report.
        userId (Optional[str], optional): User ID of the requesting user. Defaults to None.
        emailId (Optional[str], optional): Email address of the requesting user. Defaults to None.
        name (Optional[str], optional): Name of the requesting user. Defaults to None.
        sessionId (Optional[str], optional): Session ID associated with the request. Defaults to None.
        template_fullname (Optional[str], optional): Full name of the report template. Defaults to None.
        filewithType (Optional[str], optional): JSON string containing file names and types. Defaults to None.
        action (Optional[str], optional): Action to be taken in case of a conflict (overwrite, new_version, cancel). Defaults to None.

    Returns:
        JSONResponse or dict: JSON response indicating the status of the request (success, conflict, failed) and appropriate messages.

    Raises:
        HTTPException: If an exception occurs during the request processing.

    This function handles the creation of a new report request or manages existing report conflicts based on the provided input. It performs the following tasks:

    1. Uploads the provided files to an S3 bucket.
    2. Extracts relevant information from the uploaded files and performs validation checks based on the report template type (ISPR or SVR).
    3. Checks for conflicts with existing reports in a DynamoDB table.
    4. Handles different actions (overwrite, new version, cancel) based on the user's choice in case of a conflict.
    5. Stores the report request in a DynamoDB table for further processing.

    Function Calls:
    - `sitev_extract_template(TEMPLATEMASTER_DYNAMOTABLE, template_fullname)`: 
    This function extracts template information (template_id, docx_s3_key, excel_s3_key, section_name_list, template_name, doc_typ_list, doc_typ_full_list) from the TEMPLATEMASTER_DYNAMOTABLE DynamoDB table based on the provided template_fullname.
    
    - `extract_product_report_version_for_all(bucket_name, upload_folder, date_pattern, list_site_names, list_product_names, INPUT_SIZE_LIMIT, [])`: 
    This function is used for ISPR validation, where it extracts product, reporting period, file types, versions, and other information from the uploaded files.
    
    - `validate_pqr_files(source_file_names, product_list, reporting_period_list, file_types, list_size_exceeded_flag, bucket_name, upload_folder, section_names)`: 
    This function performs validation on the uploaded files for ISPR reports.
    
    - `get_mapping_list(bucket_name, excel_file_path, az_mapping_sheet_name)`: 
    This function retrieves mapping information from an Excel file stored in an S3 bucket.
    
    - `report_id_file_version_extracted(template_fullname, source_file_names, REPORTSQUEUE_DYNAMOTABLE)`: 
    This function extracts the report_id, file_version, created_at, and status_in_queue from the REPORTSQUEUE_DYNAMOTABLE DynamoDB table based on the provided template_fullname and source_file_names.
    
    - `update_report_queue_uploadfolder(old_rep_id, old_file_version, bucket_name, upload_folder, REPORTSQUEUE_DYNAMOTABLE)`: 
    This function updates the upload_folder field in the REPORTSQUEUE_DYNAMOTABLE DynamoDB table for a specific report_id and file_version.
    
    - `update_report_queue_status(old_rep_id, old_file_version, "queued", REPORTSQUEUE_DYNAMOTABLE)`: 
    This function updates the status_in_queue field to "queued" in the REPORTSQUEUE_DYNAMOTABLE DynamoDB table for a specific report_id and file_version.
    
    - `delete_reportRecords(old_rep_id, old_created_at, REPORTS_DYNAMOTABLE)`: 
    This function deletes records from the REPORTS_DYNAMOTABLE DynamoDB table based on the provided report_id and created_at timestamp.
    
    - `store_report_queue(queue_obj, REPORTSQUEUE_DYNAMOTABLE)`: 
    This function stores a ReportQueue object in the REPORTSQUEUE_DYNAMOTABLE DynamoDB table.
    """
    ###################################################################################
    source_file_names = []
    file_types = []
    # logger.info(f"create_report: 0 len(action): \t{len(action)}")
    logger.info(f"create_report: 0 action: \t{action}")
    logger.info(f"create_report: 0 template_fullname: \t{template_fullname}")
    logger.info(f"create_report: 0 name: \t{name}")
    logger.info(f"create_report: 0 filewithType: \t{filewithType}")

    # logger.info("Create Report: Requesting Creation of a report")
    dynamodb = boto3.resource('dynamodb')
    table=dynamodb.Table(REPORTSQUEUE_DYNAMOTABLE)
    first_records=[]
    request_id = ''.join(random.choice(string.ascii_lowercase) for i in range(6))  

    ############################## Upload files
    ########## Create a dedicated temporary upload_folder in s3 to save files uploaded by the user
    upload_folder = "tmp" + "_" + userId + "_" + request_id + "/"
    # source_file_names = []
    pqr_param_json_filename = ""
    
    # file_types = ast.literal_eval(file_types)
    
    # try:
    #     file_types: List[str] = json.loads(file_types_list)
    #     # Now you can use file_types_list as a list of strings
    #     logger.info(f"\nInput file types: \t{file_types}")
    # except json.JSONDecodeError:
    #     logger.info(f"\nInput file types - Invalid JSON in file_types_list")

            
    for file in files:
        try:
            logger.info(f'Create Report: STARTING S3 UPLOAD FOR: {file}')
            contents = await file.read()
            file_name = upload_folder + file.filename
            # source_file_names.append(file.filename)
            # contents = file["file"].read()
            # file_name = "tmp/"+ file["filename"]
            logger.info(f"Create report: STARTING S3 UPLOAD FOR: {file_name}")
            await s3_upload(contents=contents, key=file_name)
            #s3_upload(contents=contents, key=file_name)
        except (BotoCoreError, ClientError) as e:
            logger.error(f'for file {file}, S3 upload failed: {str(e)}') 
            #raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")
            return JSONResponse(content=str(e), status_code=500)
    
    ################################ ISPR and SVR Validation - Start ###############################################################
    # The following columns from the DB table TEMPLATEMASTER_DYNAMOTABLE (aig-azcdi-us-ops-report-templatemaster-<env_name>) are retrieved:
    # "template_id", "s3_path", "s3_excel_path", "section_names", "template_name", "doc_types", "doc_types_full"
    # The search is performed using the "template_fullname" column.
    template_id, docx_s3_key, excel_s3_key, section_name_list, template_name, doc_typ_list, doc_typ_full_list = sitev_extract_template(TEMPLATEMASTER_DYNAMOTABLE, template_fullname)
    product_name = ""
    reporting_period = ""
    list_versions = []
    
    ############## ISPR Validation
    if "ISPR" in template_fullname :
            logger.info("create_report: 1 : ISPR in template_fullname")
            # list_pqr_file_name, product_list, reporting_period_list, list_filesite_name, list_versions, list_size_exceeded_flag, extraction_successful, caught_exception =  extract_product_report_version_for_all(bucket_name, upload_folder, date_pattern, list_site_names, list_product_names, INPUT_SIZE_LIMIT, [])
            source_file_names, product_list, reporting_period_list, file_types, list_versions, list_size_exceeded_flag, extraction_successful, caught_exception =  extract_product_report_version_for_all(bucket_name, upload_folder, date_pattern, list_site_names, list_product_names, INPUT_SIZE_LIMIT, [])
        
        # Validation for supported structures.
            if not extraction_successful:
                response =  handle_validation_failure("Validation failed: One or more output variables are missing or null during extraction process")
                logger.info(f"create_report: 2 :response.status_code: \t{response.status_code}")
                delete_s3_folder(bucket_name, upload_folder)
                logger.info("create_report: 3")
                return response
            
            if source_file_names and (len(source_file_names) == len(file_types) == len(list_versions)):
                # list_pqr_file_name, list_filesite_name, list_versions = sort_versions(list_pqr_file_name, list_filesite_name, list_versions)
                logger.info("create_report: 4")
                source_file_names, file_types, list_versions = sort_versions(source_file_names, file_types, list_versions)
                
            ####### Get the first product as the product_name: We assusme here then the len(unique(product_list)) is 1.
            if len(product_list) > 0 and len(reporting_period_list) > 0 and len(file_types) > 0:
                logger.info("create_report: 5")
                product_name = product_list[0]
                reporting_period = reporting_period_list[0]
                
                ######### Make the output json more context specific, to enable concurrent user
                pqr_param_json_filename = "pqr_param" + "-" + product_name + "-" + reporting_period + "-" + userId + ".json"
                logger.info(f'UPLOADED ALL FILES TO S3 CORRECTLY')
                logger.info("create_report: 6")
        
          #### ISPR START VALIDATION #####
                #Extraction the section names key search from mapping
                mapping_dic = get_mapping_list(bucket_name, excel_file_path=f"{MAPPING_FILE_PATH}",
                             az_mapping_sheet_name=f"{SHEET_NAME_MAPPING}")
                section_names = mapping_dic["section_names_keysearch"] 
                logger.info("create_report: 7")
                response = validate_pqr_files(
                source_file_names, product_list, reporting_period_list, file_types, list_size_exceeded_flag,
                bucket_name, upload_folder, section_names)
                logger.info(f"create_report: 8 :response.body: \t{response.body}")
            
                if response.status_code==400:
                    logger.info(f"create_report: 9:response.status_code: \t{response.status_code}")
                    delete_s3_folder(bucket_name, upload_folder)
                    logger.info("create_report: 10")
                    return response
            #### END VALIDATION #####
            else:
                logger.info("create_report: 11")
                delete_s3_folder(bucket_name, upload_folder)
                return handle_validation_failure("Validation failed: Missing productname or reporting period or site name on the firstpage.")
    
    ##############################################################################################################
    ############## SVR STARValidation
    else:
        logger.info(f"create_report: SVR filewithType:: \t{filewithType}")
        filewithType = json.loads(filewithType)

        for i in range(len(filewithType)):
            source_file_names.append(filewithType[i]["file_name"])
            file_types.append(filewithType[i]["file_type"])

        logger.info("create_report: SVR - Validation: Validation Started")
        logger.info(f"create_report: Template file types: \t{doc_typ_full_list}")
        logger.info(f"create_report: Input file types: \t{file_types}")

        # Validation logic to handle when file_types has fewer elements than doc_typ_full_list
        if len(file_types) < len(doc_typ_full_list):
            # Ensure file_types is a subset of doc_typ_full_list
            if not set(file_types).issubset(set(doc_typ_full_list)):
                logger.info(f"create_report: SVR - Validation: Validation Failed - file_types is not a subset of doc_typ_full_list")
                return handle_validation_failure("Validation failed: file types not a subset of the expected template types")
            
            # Ensure file_types has no duplicates
            if len(file_types) != len(set(file_types)):
                logger.info(f"create_report: SVR - Validation: Validation Failed - duplicates found in file_types")
                return handle_validation_failure("Validation failed: duplicate file type selected")
            
            # If subset and no duplicates, validation succeeded
            logger.info(f"create_report: SVR - Validation: Validation Succeeded (subset with no duplicates)")

        # Existing validation logic for equal length lists
        elif sorted(doc_typ_full_list) == sorted(file_types):
            logger.info(f"create_report: SVR - Validation: Validation Succeeded")
        else:
            logger.info(f"create_report: SVR - Validation: Validation Failed")
            return handle_validation_failure("Validation failed: duplicate file type selected")

        # logger.info(f"create_report: SVR filewithType:: \t{filewithType}")
        # filewithType = json.loads(filewithType)

        # for i in range(len(filewithType)):
        #     source_file_names.append(filewithType[i]["file_name"])
        #     file_types.append(filewithType[i]["file_type"])

        # logger.info(f"create_report: SVR - Validation: Validation Started")
        # logger.info(f"create_report: Template file types: \t{doc_typ_full_list}")
        # logger.info(f"create_report: Input file types: \t{file_types}")
                
        # if sorted(doc_typ_full_list) == sorted(file_types):
        #     logger.info(f"create_report: SVR - Validation: Validation Succeeded")
        # else:
        #     logger.info(f"create_report: SVR - Validation: Validation Failed")
        #     return handle_validation_failure("Validation failed: duplicate file type selected")
        
        source_file_names, file_types = zip(*sorted(zip(source_file_names, file_types)))
        
    ################################ ISPR and SVR Validation - End ###############################################################
    logger.info(f"create_report: 12 len(action): \t{len(action) if action else 0}")
        
    if action is None or len(action) == 0:  ################ First time user raising the request:
            logger.info(f"create_report: 13 No conflict: len(action): \t{len(action) if action else 0}")
            try:
                response = table.scan(
                   FilterExpression=(Attr("template_fullname").eq(template_fullname) &
                                     Attr("source_file_names").eq(source_file_names) &
                                     Attr("status_in_queue").is_in(["queued", "processing", "completed"])
                 ) 
                )

                if response['Items']: ########## conflict detected
                        logger.info(f"create_report: 14 conflict detected: len(action): \t{len(action) if action else 0}")
                        #  return  {"status": "conflict", "message": "A similar report has been triggered already. Choose an option:",  "choices": ["overwrite", "cancel", "new_version"]} 
                        return  {"status": STATUS_CONFLICT, "message": MESSAGE_CONFLICT,  "choices": CONFLICT_CHOICES} 

                else: ######### New Report to be insert into the queue
                    logger.info(f"create_report: 15 Report is being requested.")
                    queue_obj = ReportQueue(
                      ############## Preparation record to insert in the queue
                      report_id = str(uuid4()), 
                      file_version = 1, ######### has been defined as a string attribute in DynamoTable
                      created_by = userId,
                      emailId = emailId,
                      name = name,
                      session_id = sessionId,
                      created_at = datetime.now().isoformat(),
                      updated_at = "",
                      status_in_queue = "queued",                          
                      upload_folder = upload_folder, 
                      pqr_param_json_filename = pqr_param_json_filename,
                      product_name = product_name,
                      reporting_period = reporting_period,    
                      validation_status = "success",
                      completion_percentage = 0,
                      # source_file_names = sorted(source_file_names),
                      source_file_names = source_file_names,
                      source_file_types = file_types,
                      list_versions = list_versions,
                      template_fullname = template_fullname,
                      template_name = template_name 
                      )
                    store_report_queue(queue_obj, REPORTSQUEUE_DYNAMOTABLE)
                    logger.info("create_report: 16 report queued.")
                    return {"status": "success", "messages": MESSAGE_REQUEST_QUEUED} 
                
            except Exception as e:
                logger.error(f"create_report: 17 {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
    
    else:
        logger.info("create_report: Conflict Detected: Requesting Creation of a new/overwrite/cancel of a report")
        old_rep_id, old_file_version, old_created_at, status_in_queue = report_id_file_version_extracted(template_fullname, source_file_names, REPORTSQUEUE_DYNAMOTABLE)
            
        if action  == ACTION_OVERWRITE:
            logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   1")

            if status_in_queue in ["completed", "queued"]:
                logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   2")
                created_at_ = datetime.now().isoformat()
                update_report_queue_overwrite(old_rep_id, old_file_version, bucket_name, upload_folder, userId, emailId, name, sessionId, created_at_, REPORTSQUEUE_DYNAMOTABLE)
                logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   3")
                update_report_queue_status(old_rep_id, old_file_version, "queued", REPORTSQUEUE_DYNAMOTABLE)
                logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   4")

                if status_in_queue == "completed":
                    logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   5")
                    old_rep_id, old_file_version, old_created_at = report_id_file_version_extracted(template_fullname, source_file_names, REPORTS_DYNAMOTABLE)
                    logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   6")
                    delete_reportRecords(old_rep_id, old_created_at, REPORTS_DYNAMOTABLE)
                    logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   7")
                return {"status": "success", "messages": "Overwrite succeed, request has been queued"} 
                    
            if status_in_queue == "processing":
                logger.info(f"create_report: Conflict Detected: {ACTION_OVERWRITE}   8")
                return {"status": "failed", "messages": "Can't Overwrite: Same Request are running. Please try later"}
                
        if action  == ACTION_NEW_VERSION:
            logger.info(f"create_report: Conflict Detected: {ACTION_NEW_VERSION}   1")
            # old_rep_id, old_file_version = report_id_file_version_extracted(request_object.template_fullname, reqest_rep.request_object.source_file_names)
            queue_obj = ReportQueue(
                  ############## Preparation record to insert in the queue
                  report_id = old_rep_id, 
                  file_version = old_file_version + 1, ######### has been defined as a string attribute in DynamoTable
                  created_by = userId,
                  emailId = emailId,
                  name = name,
                  session_id = sessionId,
                  created_at = datetime.now().isoformat(),
                  updated_at = "",
                  status_in_queue = "queued",                          
                  upload_folder = upload_folder,
                  pqr_param_json_filename = pqr_param_json_filename,
                  product_name = product_name,
                  reporting_period = reporting_period,    
                  validation_status = "success",
                  completion_percentage = 0,
                  # source_file_names = sorted(source_file_names),
                  source_file_names = source_file_names,
                  source_file_types = file_types,
                  list_versions = list_versions,
                  template_fullname = template_fullname,
                  template_name = template_name 
                  )
            logger.info(f"create_report: Conflict Detected: {ACTION_NEW_VERSION}   2")
            store_report_queue(queue_obj, REPORTSQUEUE_DYNAMOTABLE)
            logger.info(f"create_report: Conflict Detected: {ACTION_NEW_VERSION}   3")
            return {"status": "success", "messages": "New Version added"} 

        if action  == ACTION_CANCEL:
            logger.info(f"create_report: Conflict Detected: {ACTION_CANCEL}   1")
            return {"status": "success", "messages": "cancel succeed"} 
# Standard Library Imports
import os
import uuid
import ast
import posixpath
from uuid import uuid4
from datetime import datetime
from collections import defaultdict
from typing import Optional, List, Dict, Sequence

# Third-Party Imports
import pandas as pd
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
from fastapi import APIRouter, FastAPI, HTTPException, Response, UploadFile, status, File, Form
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from loguru import logger

# Local Application Imports
from config import (
    REPORTS_DYNAMOTABLE,
    REPORTSQUEUE_DYNAMOTABLE,
    INTPUTS_PATH,
    OUTPUTS_PATH,
    BUCKET_NAME,
)
from data import (
    User,
    ReportFile,
    ReportTrackingSection,
    ReportTrackingCompletion,
    ReportSelectForEditionOutput,
    IngestResult,
)
from report_edition.utils import (
    generate_presigned_urldownload,
    delete_reportRecords,
    delete_reportqueueRecords,
    update_report_locked_status,
    convert_docx_to_html_mammoth,
    store_reporttrackingcompletion,
    save_ispr_html,
    save_ispr_html_html2docx,
    set_table_borders,
    get_image_size,
    # update_report_metadata,
    upload_file_to_s3,
    handle_s3_file_versioning,
    backup_existing_file,
    get_s3_key,
    generate_timestamp,
    get_report_tracking_completion
    # get_created_at_for_report
)
# ###############
report_edition_router = APIRouter()
#####################
s3_res = boto3.resource('s3')

boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)

# Constants
NO_FILE_UPLOADED = "No file uploaded"
RECORD_LOCKED_ERROR = "Record is being edited by another user."
UPLOAD_SUCCESS_MESSAGE = "File uploaded successfully."
S3_PATH_PREFIX = "s3://"
ERROR_DETAIL_INTERNAL = "Internal server error."
SUCCESS_STATUS = "success"
COL_LOCKED = "locked"
INDEX_NAME = "report_file_path-index"

bucket_name = f"{BUCKET_NAME}"
pqr_param_json_filename = "pqr_param.json"
input_folder = (
    f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
)
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save

bucket = s3_res.Bucket(bucket_name)

async def s3_upload(contents: bytes, key: str):
    logger.info(f'Uploading {key} to s3 bucket: {bucket}')
    bucket.put_object(Key=key, Body=contents)


async def s3_delete(key: str, extension_name: str = None):
    """
    Delete the original file and its backup (if exists) from S3.

    Args:
        key: S3 object key of the original file.
        extension_name: Optional custom extension name used during backup creation.
                        If None, assumes backup was created using timestamp format.
    """
    try:
        # Delete the original file
        s3_res.Object(bucket_name=bucket_name, key=key).delete()
        logger.info(f"Deleted original file: {key}")
    except ClientError as err:
        logger.error(f"Failed to delete original file {key}: {str(err)}")

    # Now compute the backup file key
    original_file_name = os.path.basename(key)
    name_part, ext_part = os.path.splitext(original_file_name)

    # If extension_name is provided, use it. Otherwise, try both ways.
    if extension_name:
        backup_file_name = f"{name_part}_{extension_name}{ext_part}"
    else:
        # If no extension_name is given, backup file could have any timestamp.
        # Since we don't know timestamp, you may need to list and match here.
        # But as per your requirement, we'll assume fixed extension_name if provided.
        logger.warning("No extension_name provided. Skipping backup file deletion as timestamp is unknown.")
        return

    # Construct full backup file key
    backup_file_key = posixpath.join(os.path.dirname(key), backup_file_name)

    try:
        s3_res.Object(bucket_name=bucket_name, key=backup_file_key).delete()
        logger.info(f"Deleted backup file: {backup_file_key}")
    except ClientError as err:
        if err.response['Error']['Code'] == "NoSuchKey":
            logger.warning(f"Backup file not found: {backup_file_key}")
        else:
            logger.error(f"Failed to delete backup file {backup_file_key}: {str(err)}")

##########################################
@report_edition_router.post('/download/report')
async def download(report_file: ReportFile):
    
    if not report_file.report_file_path:
        raise HTTPException(
            status_code=500, 
            detail='No file name provided'
        )
        
    logger.info(f"\nTo download: report_file.report_file_path is : \t{report_file.report_file_path}")
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
        
    try:
        response = table.query(
                IndexName = "report_file_path-index",
                KeyConditionExpression=Key('report_file_path').eq(report_file.report_file_path),
                ScanIndexForward=False,
                Limit=1        
              )
        if response['Items']:
            first_record = response['Items'][0]            
            if first_record["locked"]:
            # ISPR in Edition Modus already
                # result = IngestResult(filename=first_record["IsprFilename"], status="ispr_in_edition", comment="Report is currently locked for editing by another user, refresh landing page UI")
                return {"status": "report locked in edition"} 
            
    except ClientError as err:
        # delete_incoming_files(bucket_name, upload_folder, output_folder, IsprFilename.pqr_param_json_filename)
        # print(f"Error in deleting ispr: {err}")
        logger.info(f'\nERROR: Download failed to fech the table: {err}')
        return {"status": "error"}
    # Generate the S3 URL
    try:
        s3_url = f"s3://{bucket_name}/{report_file.report_file_path}"
        presigned_url = generate_presigned_urldownload(s3_url)    
        # Return the presigned URL in a JSON response
        return {"status": "success", "downloadUrl": presigned_url}
    except Exception as e:
        print(f"Error in downloading report: {e}")
        return {"status": "error"}
#     contents =await s3_download(key=IsprFilename)
#     return Response(
#         content=contents,
#         headers={
#             'Content-Disposition': f'attachment;filename={IsprFilename}',
#             'Content-Type': 'application/octet-stream',
#             #'Access-Control-Expose-Headers': 'Content-Disposition'
#         }

#########################################################
@report_edition_router.post('/delete/report')
async def delete(report_file: ReportFile):
    
    if not report_file.report_file_path:
        raise HTTPException(
            status_code=500, 
            detail='No file name provided'
        )
        
    logger.info(f"\nTo download: report_file.report_file_path is : \t{report_file.report_file_path}")
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
        
    try:
        response = table.query(
                IndexName = "report_file_path-index",
                KeyConditionExpression=Key('report_file_path').eq(report_file.report_file_path),
                ScanIndexForward=False,
                Limit=1        
              )
        if response['Items']:
            first_record = response['Items'][0]            
            if first_record["locked"]:
            # ISPR in Edition Modus already
                # result = IngestResult(filename=first_record["IsprFilename"], status="ispr_in_edition", comment="Report is currently locked for editing by another user, refresh landing page UI")
                return {"status": "report locked in edition"} 
            
    except ClientError as err:
        # delete_incoming_files(bucket_name, upload_folder, output_folder, IsprFilename.pqr_param_json_filename)
        # print(f"Error in deleting ispr: {err}")
        logger.info(f'\nERROR: Deletion failed to fech the table: {err}')
        return {"status": "error"}
    # Generate the S3 URL
                    
    try:        
        await s3_delete(key=report_file.report_file_path, extension_name="backup")
        delete_reportRecords(report_file.report_file_path, REPORTS_DYNAMOTABLE)
        delete_reportqueueRecords(report_file, REPORTSQUEUE_DYNAMOTABLE)
        # delete_statusRecords(IsprFilename.IsprFilename, ISPR_STATUS_TABLE)
        return {"status": "success"}
    except ClientError as err:
        # delete_incoming_files(bucket_name, upload_folder, output_folder, IsprFilename.pqr_param_json_filename)
        print(f"Error in deleting ispr: {err}")
        return {"status": "error"}

######################################
@report_edition_router.post('/updatetime/report')
async def update_time_report(report_file: ReportFile):
    """
    This function is the heartbeat of the Edited record sent from the UI to the backend.
    It updates the last edit time for a given product in the DynamoDB table.
    
    Additional check: Check if it is true or not.

    Args:
        ispr_record (ProductVersion): The product version object containing the necessary data.

    Returns:
        dict: A dictionary containing a success message if the update is successful.

    Raises:
        Exception: If there is an error during the DynamoDB operation, it raises an exception with the error message.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)

    try:
        response = table.query(
                IndexName = "report_file_path-index",
                KeyConditionExpression=Key('report_file_path').eq(report_file.report_file_path),
                ScanIndexForward=False,
                Limit=1        
              )

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.isoformat()

        if response['Items']:
            first_record = response['Items'][0]
            report_id = first_record['report_id']
            created_at = first_record['created_at']
            is_report_record_updating = first_record['locked']
            logger.info(f'is_ispr_record_updating: {type(is_report_record_updating)} \t{is_report_record_updating}')
            
            # Update heartbeat only if the record is in edit mode.
            if is_report_record_updating:
                # Update the ISPR edit status with the new timestamp
                update_report_locked_status(report_id, created_at, is_report_record_updating, REPORTS_DYNAMOTABLE, True, formatted_timestamp)
                logger.info("Report time logged successfully")
                return {"message": "Report time logged successfully"}
            else:
                logger.info(f'\nReport time could not be logged as record is not editing')
                return {"message": "Report time could not be logged as record is not editings"}

    except Exception as e:
        logger.error(f'Error while logging heatbeat in Report table.: {str(e)}')
        
###################################################
@report_edition_router.post('/select/report')
async def choose_report_for_edit_html(report_file: ReportFile):
    """
    Selects an ISPR for editing based on the provided ProductName_User.
    Args:
        IsprProduct (IsprTrackingProduct): The IsprTrackingProduct object containing the ProductName_User.

    Returns:
        IsprSelectForEditionOutput or IngestResult: The output containing the selected ISPR for editing or an 
        error message.

    Raises:
        HTTPException: If an exception occurs during the process.
    """
    
    logger.info(f"\nchoose_report_for_edit_html: START ---------->")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
        
    try:
        response = table.query(
                IndexName = "report_file_path-index",
                KeyConditionExpression=Key('report_file_path').eq(report_file.report_file_path),
                ScanIndexForward=False,
                Limit=1        
              )
        
        if response['Items']:
            first_record = response['Items'][0]
            logger.info(f"choose_report_for_edit_html: 1: first_record[locked]:{first_record['locked']}")
            logger.info(f"choose_report_for_edit_html: 2: first_record[updated_at]:{first_record['updated_at']}")
            
            # Check if Ispr_Editor_Status is True and DateofEdition has a non-null value
            if first_record["locked"] and first_record["updated_at"]:
                result = IngestResult(filename=first_record["report_file_path"], status="report_in_edition", comment="Report is currently being edited by another user")
                # logger.info(f"\n/selectedit/report: Returning without edit---------->")
                logger.info(f"choose_report_for_edit_html: 3: Report is currently being edited by another user.")
                return result
                
            logger.info(f"choose_report_for_edit_html: 4: first_record[updated_at]:{first_record['updated_at']}")
            current_datetime = datetime.now()
            formatted_timestamp = current_datetime.isoformat()
            
            # If the above condition is not met, proceed with the function
            report_id = first_record['report_id']
            created_at = first_record['created_at'] 
            
            # When the record is edited, the DateofEdition needs to be populated with current time.
            # update_ispr_edit_status(session_id, timestamp, True, ISPR_DYNAMOTABLE)
            update_report_locked_status(report_id, created_at, True, REPORTS_DYNAMOTABLE, True, formatted_timestamp)
            logger.info(f"/selectedit/report: Report is Updated ------------->")

            logger.info(f'choose_report_for_edit_html: Length  -----{len(first_record["report_file_path"])}')
            logger.info("choose_report_for_edit_html: Creating a backup prior to editing.")

            final_s3_key = handle_s3_file_versioning(
                s3_res=s3_res,
                s3_client=s3_client,
                bucket_name=bucket_name,
                final_s3_key=first_record["report_file_path"]
            )
            
            # Modification: Rich Text
            #html_content = convert_docx_to_html(bucket_name, first_record["IsprFilename"])
            html_content = convert_docx_to_html_mammoth(bucket_name, first_record["report_file_path"])
            # logger.info(f"selecteditproduct: html_content is retrieved ------{html_content}")
            # logger.info(f"choose_report_for_edit_html: Converted to HTML ------- {len(html_content)}")
            if len(html_content) > 0:

                first_record_report = ReportTrackingCompletion(
                                                 report_id = first_record["report_id"],
                                                 created_by = first_record["created_by"],
                                                 name = first_record["name"],
                                                 session_id = first_record["session_id"],
                                                 created_at = first_record["created_at"],
                                                 file_version = first_record["file_version"],
                                                 report_file_path = first_record["report_file_path"],
                                                 template_name = first_record["template_name"],
                                                 template_fullname = first_record["template_fullname"],
                                                 product_name = first_record["product_name"],  ############ Or Molecule Name 
                                                 pqr_param_json_filename = first_record["pqr_param_json_filename"], 
                                                 reporting_period = first_record["reporting_period"], 
                                                 edit_status = first_record["edit_status"],
                                                 completion_detail_section=first_record["completion_detail_section"],
                                                 completion = first_record["completion"],
                                                 updated_at = first_record["updated_at"],
                                                 edited_by = first_record["edited_by"],
                                                 locked = first_record["locked"],
                                                 source_file_names = first_record["source_file_names"],
                                                 site_names = first_record["site_names"],
                                                 report_title = first_record["report_title"],
                                                 html_content = html_content
                )

                logger.info(f"choose_report_for_edit_html: result created ------------->")
                
            else:
                update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE)
                return {"status": "error", "message": "Something went wrong. Please download the report, make updates locally and re-upload."}
                

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
          
    return first_record_report


#######################################################################
@report_edition_router.post('/save/report') 
async def update_report_html(report_record: ReportSelectForEditionOutput):
    """
    This API handles the process of saving an edited report in HTML format.

    Args:
        report_record (ReportSelectForEditionOutput): The report data to be saved.

    Returns:
        dict: A success message indicating that the report edition was stored successfully.

    Raises:
        HTTPException: If an exception occurs during the process.

    Steps:
        1. Retrieve the report record data from the input parameter.
        2. Initialize a DynamoDB resource and table.
        3. Extract the target file path from the report record data.
        4. Get the current datetime and format it as an ISO string.
        5. Update the session ID, created timestamp, edited by user ID, and locked status in the report record data.
        6. Set the edit status based on the completion percentage value.
        7. Log a message indicating the start of the update process.
        8. Temporarily store the HTML content and replace it with an empty HTML string.
        9. Call the `store_reporttrackingcompletion` function to store the report tracking completion data in the DynamoDB table.
        10. Log the target file path.
        11. Call the `save_ispr_html` function to save the HTML content in the specified S3 bucket.
        12. Log a message indicating the successful storage of the ISPR edition.
        13. Return a success message.
        14. If an exception occurs, raise an HTTP exception with a 500 status code and the exception message as the detail.
    """
    logger.info(f"update_report_html: -------1------>")

    # Initialize DynamoDB resource and table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    
    # Extract report data from the input parameter
    report_records_dict = report_record.report_select_edit.dict()
    target_filename = report_records_dict["report_file_path"]
    
    # Get the current datetime and format it as an ISO string
    current_datetime = datetime.now()
    formatted_timestamp = current_datetime.isoformat()
    
    # Update report record data
    report_record.report_select_edit.session_id = report_record.user.sessionId
    report_record.report_select_edit.created_at = formatted_timestamp
    
    # The following DateofEdition indicates that it needs to be blank during Saving.
    report_record.report_select_edit.updated_at = None
    report_record.report_select_edit.edited_by = report_record.user.id
    report_record.report_select_edit.locked = False
    
    # Set the edit status based on the completion percentage
    if (report_record.report_select_edit.completion < 100):
        report_record.report_select_edit.edit_status = "In Progress"
    
    else:
         if (report_record.report_select_edit.completion == 100):
            report_record.report_select_edit.edit_status = "Completed"
    
    try:
        logger.info("\nupdate_report_html: ----------- 1 -----------")
        
        html_content = report_record.report_select_edit.html_content
        report_record.report_select_edit.html_content  = "<html></html>"

        # Store the report tracking completion data in the DynamoDB table
        store_reporttrackingcompletion(report_record.report_select_edit, REPORTS_DYNAMOTABLE)
        target_filename = report_record.report_select_edit.report_file_path
        logger.info(f"update_report_html:----------- 2 -----------\n{target_filename}")
        
        # Save the HTML content in the specified S3 bucket
        save_ispr_html(bucket_name, html_content, target_filename)
        logger.info("\nupdate_report_html: ----------- 3 -----------")
        return {"message": "Edited report stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        

####################################################################################
@report_edition_router.post('/release/lock')
async def releaselock_ispr(report_file: ReportFile):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    
    try:
        response = table.query(
                IndexName = "report_file_path-index",
                KeyConditionExpression=Key('report_file_path').eq(report_file.report_file_path),
                ScanIndexForward=False,
                Limit=1        
              )
        if response['Items']:
            first_record = response['Items'][0]                          
            report_id = first_record['report_id']
            created_at = first_record['created_at'] 
            update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE)
            return {"message": "Report-Edition unlocked successfully"}               
                
    except Exception as e:
        logger.error(f'Lock release Editor error": {str(e)}')

# COMMENTED OUT FOR IMPROVEMENT
# TODO: REMOVE
# @report_edition_router.post('/upload/report')
# async def upload_report(
#     file: UploadFile,
#     report_id: Optional[str] = Form(None),
#     file_version: Optional[int] = Form(None), 
#     report_file_path: Optional[str] = Form(None),
#     user_id: Optional[str] = Form(None),
# ):
#     """
#     API endpoint to upload a report file to S3 with versioning & metadata update.
#     """
#     logger.info("upload_report: ----- START -----")
#     # logger.info(f"upload_report: ----- report_id -----{report_id}")
#     # logger.info(f"upload_report: ----- file_version -----{file_version}")
#     # logger.info(f"upload_report: ----- report_file_path -----{report_file_path}")
#     # logger.info(f"upload_report: ----- user_id -----{user_id}")

#     try:
#         # Condition check 1 #################
#         if not file or not file.filename:
#             logger.error("No file uploaded.")
#             # raise HTTPException(status_code=400, detail="No file uploaded")
#             result = IngestResult(filename=first_record["report_file_path"], status="report_in_edition", comment="No file uploaded")
#             logger.info(f"upload_report: No file uploaded.")
#             return result

#         # # Condition check 2 #################
#         dynamodb = boto3.resource('dynamodb')
#         table = dynamodb.Table(REPORTS_DYNAMOTABLE)
            
#         response = table.query(
#                 IndexName = "report_file_path-index",
#                 KeyConditionExpression=Key('report_file_path').eq(report_file_path),
#                 ScanIndexForward=False,
#                 Limit=1        
#             )
#         if response['Items']:
#             first_record = response['Items'][0]            
#             if first_record["locked"]:
#                 logger.info(f"upload_report: first_record[locked]:{first_record['locked']}")
#                 logger.info(f"upload_report: first_record[updated_at]:{first_record['updated_at']}")
#                 raise HTTPException(status_code=400, detail="Record is being edited by another user.")
#             else:
#                 logger.info(f"upload_report: No records for this report_id: {report_id}")
                
#         # All conditions are passed
#         logger.info("upload_report: All conditions passed.")
#         current_datetime = datetime.now()
#         formatted_timestamp = current_datetime.isoformat()
#         created_at = get_created_at_for_report(report_id)
#         update_report_locked_status(report_id, created_at, True, REPORTS_DYNAMOTABLE, True, formatted_timestamp)

#         uploaded_filename = file.filename
#         final_s3_key = get_s3_key(uploaded_filename, report_file_path)

#         final_s3_key = handle_s3_file_versioning(
#             s3_res=s3_res,
#             s3_client=s3_client,
#             bucket_name=bucket_name,
#             final_s3_key=final_s3_key
#         )

#         await upload_file_to_s3(file, final_s3_key)

#         logger.info("upload_report: ----- END -----")
#         update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE, True, None)

#         return {
#             "status": "success",
#             "message": "File uploaded successfully.",
#             "s3_path": f"s3://{bucket_name}/{final_s3_key}"
#         }

#     except HTTPException as he:
#         logger.warning(f"Client error: {he.detail}")
#         raise he

#     except Exception as e:
#         logger.error(f"Unexpected server error: {str(e)}")
#         raise HTTPException(status_code=500, detail="Internal server error.")


@report_edition_router.post('/upload/report')
async def upload_report(
    file: UploadFile,
    report_id: Optional[str] = Form(None),
    file_version: Optional[int] = Form(None),
    report_file_path: Optional[str] = Form(None),
    user_id: Optional[str] = Form(None),
):
    """
    Upload a report file to S3 with versioning and metadata update.

    Args:
        file (UploadFile): The file to be uploaded.
        report_id (Optional[str], optional): The ID of the report. Defaults to Form(None).
        file_version (Optional[int], optional): The version of the file. Defaults to Form(None).
        report_file_path (Optional[str], optional): The file path of the report. Defaults to Form(None).
        user_id (Optional[str], optional): The ID of the user uploading the file. Defaults to Form(None).

    Raises:
        HTTPException: If no file is uploaded or if the record is being edited by another user.

    Returns:
        dict: A dictionary containing the status, message, and S3 path of the uploaded file.
    """
    logger.info("upload_report: ----- START -----")

    try:
        # Condition check 1: Check if a file is uploaded
        if not file or not file.filename:
            logger.error(NO_FILE_UPLOADED)
            raise HTTPException(status_code=400, detail=NO_FILE_UPLOADED)

        # Condition check 2: Check if the record is locked
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(REPORTS_DYNAMOTABLE)
        response = table.query(
            IndexName=INDEX_NAME,
            KeyConditionExpression=Key('report_file_path').eq(report_file_path),
            ScanIndexForward=False,
            Limit=1
        )

        if response['Items']:
            first_record = response['Items'][0]
            if first_record[COL_LOCKED]:
                # logger.info(f"upload_report: first_record[locked]:{first_record['locked']}")
                # logger.info(f"upload_report: first_record[updated_at]:{first_record['updated_at']}")
                raise HTTPException(status_code=400, detail=RECORD_LOCKED_ERROR)

        # All conditions are passed
        logger.info("upload_report: All conditions passed.")
        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.isoformat()
        # created_at = get_created_at_for_report(report_id)
        report_tracking_completion = get_report_tracking_completion(report_id)
        created_at = report_tracking_completion.created_at
        update_report_locked_status(report_id, created_at, True, REPORTS_DYNAMOTABLE, True, formatted_timestamp)

        uploaded_filename = file.filename
        final_s3_key = get_s3_key(uploaded_filename, report_file_path, report_id, created_at, REPORTS_DYNAMOTABLE)

        final_s3_key = handle_s3_file_versioning(
            s3_res=s3_res,
            s3_client=s3_client,
            bucket_name=bucket_name,
            final_s3_key=final_s3_key
        )

        await upload_file_to_s3(file, final_s3_key)

        logger.info("upload_report: ----- END -----")
        update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE, True, None)

        return {
            "status": SUCCESS_STATUS,
            "message": UPLOAD_SUCCESS_MESSAGE,
            "s3_path": f"{S3_PATH_PREFIX}{bucket_name}/{final_s3_key}"
        }

    except HTTPException as he:
        logger.warning(f"upload_report:Client error: {he.detail}")
        raise he

    except Exception as e:
        logger.error(f"upload_report:Unexpected server error: {str(e)}")
        raise HTTPException(status_code=500, detail=ERROR_DETAIL_INTERNAL)
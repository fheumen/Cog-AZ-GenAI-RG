# from dashboard.utils import *
from config import REPORTS_DYNAMOTABLE, TEMPLATEMASTER_DYNAMOTABLE, USERS_DYNAMOTABLE
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
from fastapi import APIRouter
from collections import defaultdict
from typing import Optional, List, Dict, Sequence
import uuid
from uuid import uuid4
import os
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from datetime import datetime
from typing import Optional, List, Dict
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from loguru import logger

###############with app
from data import  User, UserTemplate, ReportTrackingCompletion, ReportTrackingSection, ReportTrackingWelcomePageAllProduct, WelcomeFilter, UserOut, ReportFile, ReportFileRename

from utils import update_report_locked_status
###############

dashboard_router = APIRouter()
# Initialize FastAPI app
#router = APIRouter()
boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)
# dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
# table = dynamodb.Table(REPORTS_DYNAMOTABLE)


@dashboard_router.post("/firstpage/")
async def welcome_page(user: User):   

    logger.info("WELCOME PAGE STARTED V2")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    first_records = []
    
    if len(user.sessionId) == 0:
        user.sessionId=str(uuid4())
                
    try:
        # Scan the entire table
        response = table.scan()        
        for item in response['Items']:
            first_record = item
            first_record_report = ReportTrackingCompletion(
                                             # SessionID = first_record["SessionID"],
                                             # Timestamp = first_record["Timestamp"],
                                             # Timestamp = first_record["Timestamp"],
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
                                             html_content = first_record["html_content"]
                                             
            )
                                             
            first_records.append(first_record_report)
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
    result = ReportTrackingWelcomePageAllProduct(user=user, report_status_main=first_records)
    return result


# @site_validation_router.get("/get/dashboard/filter")
@dashboard_router.post("/firstpage/filter")
async def dashboard_filter(filterparam: WelcomeFilter):   
    
#   "pr_ids": ["user123", "user456"],
#   "statuses": ["In Progress", "Completed"],
#   "sort_by": "updated_at", 
#   "sort_order": "desc"

    logger.info("\ndashboard_filter ---- 1 ----")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    first_records = []
    
    # if len(filterparam.user.sessionId) == 0:
    #     filterparam.user.sessionId=str(uuid4())
                
    try:
        # logger.info(f"\n\ndashboard_filter: Type of filterparam.pr_ids: {type(filterparam.pr_ids)}")
        # logger.info(f"dashboard_filter: Contents of filterparam.pr_ids: {filterparam.pr_ids}")
        # logger.info(f"dashboard_filter: Type of filterparam.statuses: {type(filterparam.statuses)}")
        # logger.info(f"dashboard_filter: Contents of filterparam.statuses: {filterparam.statuses}")
        # Scan the entire table
        if filterparam.pr_ids:
            logger.info("dashboard_filter: filterparam.pr_ids IF BLOCK -------------")
            response = table.scan(
                    FilterExpression=(Attr('created_by').is_in(filterparam.pr_ids) &
                                      Attr('edit_status').is_in(filterparam.statuses) 
                                     )
                )

            # Print the type of the response object
            logger.info(f"dashboard_filter: response type: {type(response)}")

            # Get the number of items fetched
            num_items = len(response['Items'])
            logger.info(f"dashboard_filter: Number of items fetched: {num_items}")
            
            response['Items'].sort(
                key=lambda x: datetime.fromisoformat(x["created_at"]),
                reverse=(filterparam.sort_order.lower() == "desc")
                # reverse=(sort_order.lower() == "desc")
            )
        else:
            logger.info("dashboard_filter: filterparam.pr_ids ELSE BLOCK -------------")
            
            response = table.scan(
                FilterExpression=(
                    Attr('edit_status').is_in(filterparam.statuses) 
                    )
            )
            
            # Print the type of the response object
            logger.info(f"dashboard_filter: response type: {type(response)}")

            # Get the number of items fetched
            num_items = len(response['Items'])
            logger.info(f"dashboard_filter: Number of items fetched: {num_items}")

            response['Items'].sort(
                key=lambda x: datetime.fromisoformat(x["created_at"]),
                reverse=(filterparam.sort_order.lower() == "desc")
                # reverse=(sort_order.lower() == "desc")
            )
        for item in response['Items']:
            first_record = item
            logger.info(f"\n\ndashboard_filter: FOR LOOP: \nname: {first_record['name']}")
            first_record_report = ReportTrackingCompletion(
                                             # SessionID = first_record["SessionID"],
                                             # Timestamp = first_record["Timestamp"],
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
                                             html_content = first_record["html_content"]
                                             
            )
                                             
            first_records.append(first_record_report)
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
    result = ReportTrackingWelcomePageAllProduct(user=filterparam.user, report_status_main=first_records)
    return result

@dashboard_router.post("/report_templates/") 
async def get_report_templates(user:User):
    logger.info("Getting report templates")
    dynamodb = boto3.resource('dynamodb')
    table=dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    first_records=[]
    
    if len(user.sessionId)==0:
        user.sessionId=str(uuid4())

    try:
        # Scan the entire table
        response = table.scan()        
        for item in response['Items']:
            first_records.append(item["template_fullname"])
                             
                                             
            
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
    # result = ReportTrackingWelcomePageAllProduct(user=user, report_status_main=first_records)
    return {"status": "success", "template_fullnames": first_records}    


@dashboard_router.post("/report_templates/file-types")
async def get_file_types(user: UserTemplate):
    logger.info("Getting report list of report types for a given template")
    
    dynamodb = boto3.resource('dynamodb')
    table=dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    first_records=[]
    
      
    try:
        response = table.scan(
           FilterExpression=Attr("template_fullname").eq(user.template_fullname)
         ) 
        
                           
    except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
    # result = ReportTrackingWelcomePageAllProduct(user=user, report_status_main=first_records)
    return {"status": "success", "template_types": response['Items'][0]["doc_types_full"]}    
    

@dashboard_router.get("/users")
async def get_users():
    """
    Retrieve a list of users from the DynamoDB table.

    Returns:
        List[UserOut]: A list of UserOut objects representing the users in the DynamoDB table.

    Raises:
        HTTPException: If there is an error retrieving data from DynamoDB or an unexpected exception occurs.
    """
    logger.info("users -------Start-------")
    try:
        dynamodb = boto3.resource('dynamodb')

        # Access the DynamoDB table
        users_table = dynamodb.Table(USERS_DYNAMOTABLE)
        response = users_table.scan()
        items = response.get('Items', [])

        # Format response
        users = []

        for item in items:
            email_id = item.get("email_id", "")  # Handle missing or blank email_id
            name = item.get("name", "")  # Handle missing or blank name
            pr_id = item.get("pr_id", "")  # Handle missing or blank pr_id

            user = UserOut(
                pr_id=pr_id.lower(),
                name=name,
                email_id=email_id.lower()
            )
            users.append(user)

        logger.info("users -------End-------")
        return users

    except boto3.exceptions.DynamoDBException as e:
        logger.error(f"users: The following DynamoDB error occurred: \n{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"users: The following unexpected error occurred: \n{str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")
        
        
@dashboard_router.post("/report/rename")
async def renaming_report_title(report_file: ReportFileRename):
    """
    This function allow the user to rename the report title
    
    Additional check: Check if it is true or not.

    Args:
        ispr_record (ProductVersion): The product version object containing the necessary data.

    Returns:
        dict: A dictionary containing a success message if the rename is successful.

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

        if response['Items']:
            first_record = response['Items'][0]
            report_id = first_record['report_id']
            created_at = first_record['created_at']
            created_by = first_record['created_by']
            is_report_record_updating = first_record['locked']
            logger.info(f'is_ispr_record_updating: {type(is_report_record_updating)} \t{is_report_record_updating}')
            
            # Rename the Title only if the record is not locked.
            if not is_report_record_updating:
                if report_file.userId == created_by:
                    current_datetime = datetime.now()
                    formatted_timestamp = current_datetime.isoformat()
                    update_report_locked_status(report_id, created_at, True, REPORTS_DYNAMOTABLE, True, formatted_timestamp)

                    logger.info(f"\nupdate_report_title-------START-------")
                     # Construct the update expression
                    update_expression = "SET report_title = :report_title_value"
                    expression_attribute_values = {":report_title_value": report_file.new_title}

                    key = {
                            'report_id': report_id,
                            'created_at': created_at
                        }

                    # Update the item in DynamoDB
                    response = table.update_item(
                        Key=key,
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_attribute_values,
                        ReturnValues="UPDATED_NEW"
                    )
                    logger.info(f"\nupdate_report_title-------END-------")
                    update_report_locked_status(report_id, created_at, False, REPORTS_DYNAMOTABLE)
                    return {"status": "success", "message": "Report-Renaming successfully"}  
                else:
                    return {"status": "failed", "message": "You are not allowed to rename"}
                                      

            else:
                logger.info(f'\nRenaming could not happen since report is in editing')
                return {"status": "failed", "message": "Report Title  could not be rename as record is in editings"}

   

    except Exception as e:
        logger.error(f'Error while logging in Report table.: {str(e)}')
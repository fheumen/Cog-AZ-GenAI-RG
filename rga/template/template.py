# template/template.py

from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, status, Body
from fastapi.responses import JSONResponse, StreamingResponse
from typing import List
from uuid import uuid4
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError
from loguru import logger
import base64

from app.config import TEMPLATEMASTER_DYNAMOTABLE, BUCKET_NAME, SHAREPOINT_MAIN_URL, TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT
from template.template_utils import (
    create_and_upload_docx,
    store_template_master_record,
    dynamo_list_to_python_list_str,
    python_list_to_dynamo_list_str,
    download_template_docx,
    delete_template_docx,
    delete_template_records,
    update_template_locked_updatedat_created_at,
    update_template_locked_status,
    store_templatetrackingcompletion,
    get_graph_token,
    get_graph_token_using_cert,
    # extract_server_relative_url,
    # get_file_metadata_via_graph,
    # generate_sharepoint_wopi_link_with_graph
    generate_embed_view_url
    
)

from data import (
    User,
    Section,
    TemplateMaster,
    TemplateDashboardResponse,
    TemplateCreateRequest,
    TemplateFile,
    TemplateFilter
)

# Constants
S3_PREFIX = "inputs/sv_docx_templates"
DEFAULT_TEMPLATE_DOC_NAME = "test"
DOCX_EXTENSION = ".docx"

STATUS_SUCCESS = "success"
STATUS_FAILURE = "failed"

MSG_TEMPLATE_CREATED = "New template created."
MSG_TEMPLATE_CREATION_FAILED = "Failed to create new template."

s3 = boto3.client("s3")

template_router = APIRouter()


def get_templates_for_user(user: User) -> TemplateDashboardResponse:
    """
    Fetch all template records from DynamoDB and return for the given user.

    Args:
        user (User): The user requesting templates.

    Returns:
        TemplateDashboardResponse: Response containing user and list of template metadata.

    Raises:
        HTTPException: If the DynamoDB scan operation fails.
    """
    logger.info("Fetching templates for user...")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    templates: List[TemplateMaster] = []

    if not user.sessionId:
        user.sessionId = str(uuid4())

    try:
        # response = table.scan()
        # items = response.get("Items", [])
        response = table.scan()
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))

        if not items:
            logger.warning("No template records found in TemplateMaster table.")
            return TemplateDashboardResponse(user=user, template_status_main=[])

        for item in items:
            try:
                if "template_id" not in item:
                    logger.warning("Skipping record without template_id")
                    continue

                # Normalize potential DynamoDB-typed lists
                for key in ["doc_types", "doc_types_full", "product_names"]:
                    val = item.get(key)
                    if isinstance(val, list):
                        item[key] = dynamo_list_to_python_list_str(val)
                    elif isinstance(val, str):
                        item[key] = [val]
                    elif val is not None:
                        logger.warning(f"Unexpected type for {key}: {type(val)}. Setting to empty list.")
                        item[key] = []
                    else:
                        item[key] = []

                template = TemplateMaster(**item)
                templates.append(template)

            except Exception as parse_error:
                logger.error(f"Failed to parse template record: {parse_error} -- Item: {item}")
                continue

    except (ClientError, BotoCoreError) as e:
        logger.exception("Error while scanning DynamoDB template master table.")
        raise HTTPException(
            status_code=502,
            detail=f"AWS Error while retrieving templates: {str(e)}"
        )
    except Exception:
        logger.exception("Unexpected error while retrieving templates.")
        raise HTTPException(
            status_code=500,
            detail="Unable to retrieve template records. Please try again later or contact support."
        )

    return TemplateDashboardResponse(user=user, template_status_main=templates)

def get_templates_filter_for_user(filterparam: TemplateFilter) -> TemplateDashboardResponse:

    """
    Fetch all template records from DynamoDB and return for the given user.

    Args:
        filterparam (TemplateFilter): The filter conditions for templates.

    Returns:
        TemplateDashboardResponse: Response containing user and list of template metadata.

    Raises:
        HTTPException: If the DynamoDB scan operation fails.
    """
    logger.info("Fetching templates for user...")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    templates: List[TemplateMaster] = []

    if not filterparam.user.sessionId:
        filterparam.user.sessionId = str(uuid4())

    try:
        # response = table.scan()
        # items = response.get("Items", [])
        if filterparam.pr_ids:
            response = table.scan(FilterExpression=(Attr('created_by').is_in(filterparam.pr_ids) &
                                          Attr('approval_status').is_in(filterparam.statuses) 
                                         ))
            items = response.get("Items", [])
    
            while "LastEvaluatedKey" in response:
                response = table.scan(FilterExpression=(Attr('created_by').is_in(filterparam.pr_ids) &
                                          Attr('approval_status').is_in(filterparam.statuses) 
                                         ),
                                      ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))
    
            if not items:
                logger.warning("No template records found in TemplateMaster table.")
                return TemplateDashboardResponse(user=filterparam.user, template_status_main=[])

            
        else:
            response = table.scan(FilterExpression=(
                                          Attr('approval_status').is_in(filterparam.statuses) 
                                         ))
            items = response.get("Items", [])
    
            while "LastEvaluatedKey" in response:
                response = table.scan(  FilterExpression=(
                                          Attr('approval_status').is_in(filterparam.statuses) 
                                         ),
                                      ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))
    
            if not items:
                logger.warning("No template records found in TemplateMaster table.")
                return TemplateDashboardResponse(user=filterparam.user, template_status_main=[])

        sorted_items = sorted(
                    items,
                    key=lambda x: datetime.fromisoformat(x["created_at"]),
                    reverse=(filterparam.sort_order.lower() == "desc")
                )
    
        for item in sorted_items:
            try:
                if "template_id" not in item:
                    logger.warning("Skipping record without template_id")
                    continue

                # Normalize potential DynamoDB-typed lists
                for key in ["doc_types", "doc_types_full", "product_names"]:
                    val = item.get(key)
                    if isinstance(val, list):
                        item[key] = dynamo_list_to_python_list_str(val)
                    elif isinstance(val, str):
                        item[key] = [val]
                    elif val is not None:
                        logger.warning(f"Unexpected type for {key}: {type(val)}. Setting to empty list.")
                        item[key] = []
                    else:
                        item[key] = []

                template = TemplateMaster(**item)
                templates.append(template)

            except Exception as parse_error:
                logger.error(f"Failed to parse template record: {parse_error} -- Item: {item}")
                continue
            

    except (ClientError, BotoCoreError) as e:
        logger.exception("Error while scanning DynamoDB template master table.")
        raise HTTPException(
            status_code=502,
            detail=f"AWS Error while retrieving templates: {str(e)}"
        )
    except Exception:
        logger.exception("Unexpected error while retrieving templates.")
        raise HTTPException(
            status_code=500,
            detail="Unable to retrieve template records. Please try again later or contact support."
        )

    return TemplateDashboardResponse(user=filterparam.user, template_status_main=templates)


def create_new_template_func(request: TemplateCreateRequest) -> dict:
    """
    Creates a new template:
    1. Builds and uploads a DOCX to S3.
    2. Saves template metadata to DynamoDB.

    Args:
        request (TemplateCreateRequest): Request payload from client.

    Returns:
        dict: Response containing template ID and upload path, or failure info.
    """
    try:
        logger.info(f"Creating new template for user: {request.user_id}")

        template_id = str(uuid4())
        current_time = datetime.utcnow().isoformat()

        raw_document_name = request.document_name.strip() if request.document_name else DEFAULT_TEMPLATE_DOC_NAME

        template_name = raw_document_name
        template_fullname = raw_document_name

        if not raw_document_name.lower().endswith(DOCX_EXTENSION):
            document_name_for_s3 = raw_document_name + DOCX_EXTENSION
        else:
            document_name_for_s3 = raw_document_name

        # Prepare section data for DOCX and store section titles only
        docx_section_dicts = []
        section_titles_only = []
        section_descs_only = []
        section_name_desc_list = []

        for section in request.section_names or []:
            # Support both pydantic models and raw dicts
            title = getattr(section, "title", "") if not isinstance(section, dict) else section.get("title", "")
            desc = getattr(section, "desc", "") if not isinstance(section, dict) else section.get("desc", "")
            docx_section_dicts.append({"title": title, "desc": desc})
            section_name_desc_list.append(Section(title=title, desc=desc))
            section_titles_only.append(title)
            section_descs_only.append(desc)

        # Upload document to S3
        s3_path = create_and_upload_docx(
            section_names=docx_section_dicts,
            document_name=document_name_for_s3,
            s3_bucket=BUCKET_NAME,
            s3_prefix=S3_PREFIX
        )

        doc_types = getattr(request, "doc_types", []) or []
        doc_types_full = getattr(request, "doc_types_full", []) or []
        product_names = getattr(request, "product_names", []) or []

        # # Convert to DynamoDB format if not empty
        doc_types = python_list_to_dynamo_list_str(doc_types) if doc_types else None
        doc_types_full = python_list_to_dynamo_list_str(doc_types_full) if doc_types_full else None
        product_names = python_list_to_dynamo_list_str(product_names) if product_names else None

        # Construct TemplateMaster record
        template_record = TemplateMaster(
            template_id=template_id,
            created_at=current_time,
            created_by=request.user_id,
            section_names_desc=section_name_desc_list,
            section_names=section_titles_only,
            # section_descs=section_descs_only,
            s3_path=s3_path,
            template_name=template_name,
            template_fullname=template_fullname,
            is_active=True,
            locked=False,
            doc_types=doc_types,
            doc_types_full=doc_types_full,
            kb_name=None,
            product_names=product_names,
            s3_excel_path=None,
            updated_at=None,
            updated_by=None,
        )

        # Store the record in DynamoDB
        store_template_master_record(template_record)

        return {
            "status": STATUS_SUCCESS,
            "template_id": template_id,
            "s3_path": s3_path
        }

    except (ClientError, BotoCoreError) as aws_error:
        logger.exception("AWS error while creating template.")
        return {
            "status": STATUS_FAILURE,
            "messages": f"AWS error while creating template: {str(aws_error)}"
        }

    except Exception as e:
        logger.exception(f"Unexpected error during template creation: {e}")
        return {
            "status": STATUS_FAILURE,
            "messages": "Failed to create new template"
        }


@template_router.post("/landing/")
async def template_landing_page(user: User):
    """
    API endpoint: Returns all available templates for a given user.

    Args:
        user (User): User information from request body.

    Returns:
        TemplateDashboardResponse: User + list of templates.
    """
    return get_templates_for_user(user)

@template_router.post("/landing/filter")
async def template_landing_page_filter(filterparam: TemplateFilter): 
    """
    API endpoint: Returns all available templates filling the filter conditions

    Args:
        filterparam (TemplateFilter): Filter information from request body.

    Returns:
        TemplateDashboardResponse: User + list of templates.
    """

    return get_templates_filter_for_user(filterparam)


@template_router.post("/create/")
async def create_new_template(request: TemplateCreateRequest):
    """
    API endpoint: Creates and uploads a new template to S3 and DynamoDB.

    Args:
        request (TemplateCreateRequest): Request body with template metadata and sections.

    Returns:
        JSONResponse: Contains status, template ID, and S3 path on success; error message on failure.
    """
    result = create_new_template_func(request)
    if result["status"] == STATUS_SUCCESS:
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=result)
    else:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=result)


def get_template_record_by_s3_path(s3_path: str):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)

    # Scan for record with matching s3_path (assuming s3_path is unique)
    response = table.scan(
        FilterExpression=Attr("s3_path").eq(s3_path)
    )
    items = response.get("Items", [])
    if not items:
        return None
    # Assuming only one record per s3_path
    return items[0]

def get_template_record_by_template_id(template_id: str) -> TemplateMaster:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)

    # Scan for record with matching s3_path (assuming s3_path is unique)
    response = table.scan(
        FilterExpression=Attr("template_id").eq(template_id)
    )
    items = response.get("Items", [])
    if not items:
        return None
    # Assuming only one record per s3_path
    return items[0]


# @template_router.post("/delete/")
# async def delete_template(file: TemplateFile):
#     """
#     Endpoint to delete a template file from S3 and remove its record from DynamoDB.

#     Args:
#         file (TemplateFile): Request body containing the S3 path of the template to delete.

#     Raises:
#         HTTPException 400: If no S3 path is provided in the request.
#         HTTPException 500: If deletion fails due to server error.

#     Returns:
#         JSONResponse: Success status and DynamoDB deletion result if successful.
#     """
#     # Validate presence of S3 path in the request body
#     if not file.s3_path:
#         raise HTTPException(status_code=400, detail="No S3 path provided")

#     logger.info(f"Attempting to delete template file: {file.s3_path}")

#     try:
#         # Delete the template file from S3 storage (synchronous function called directly)
#         delete_template_docx(file.s3_path, BUCKET_NAME)

#         # Delete corresponding metadata record from DynamoDB
#         dynamo_delete_result = delete_template_records(file.s3_path, TEMPLATEMASTER_DYNAMOTABLE)

#         # Return success response with DynamoDB deletion result
#         return JSONResponse(status_code=200, content={
#             "status": "success",
#             "dynamo_delete": dynamo_delete_result
#         })

#     except HTTPException as http_err:
#         # Reraise HTTP exceptions to propagate client errors
#         raise http_err
#     except Exception:
#         # Log unexpected exceptions and return 500 error
#         logger.exception("Failed to delete template")
#         raise HTTPException(status_code=500, detail="Failed to delete template")


@template_router.post("/delete/")
async def delete_template(file: TemplateFile):
    if not file.s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    logger.info(f"Attempting to delete template file: {file.s3_path}")

    # Check if template_id starts with 'temp_'
    record = get_template_record_by_s3_path(file.s3_path)
    if record is None:
        raise HTTPException(status_code=404, detail="Template record not found")

    template_id = record.get("template_id", "")
    if template_id.startswith("temp_"):
        raise HTTPException(
            status_code=403,
            detail=f"Old existing templates cannot be deleted."
        )

    try:
        delete_template_docx(file.s3_path, BUCKET_NAME)
        dynamo_delete_result = delete_template_records(file.s3_path, TEMPLATEMASTER_DYNAMOTABLE)
        return JSONResponse(status_code=200, content={
            "status": "success",
            "dynamo_delete": dynamo_delete_result
        })

    except HTTPException as http_err:
        raise http_err
    except Exception:
        logger.exception("Failed to delete template")
        raise HTTPException(status_code=500, detail="Failed to delete template")


@template_router.post("/download/")
async def download_template(file: TemplateFile):
    """
    Endpoint to generate and return a pre-signed download URL for a template file stored in S3.

    Args:
        file (TemplateFile): Request body containing the S3 path of the template to download.

    Raises:
        HTTPException 400: If no S3 path is provided in the request.
        HTTPException 500: If generating the download URL fails.

    Returns:
        JSONResponse: Contains the pre-signed download URL if successful.
    """
    # Validate presence of S3 path in the request body
    if not file.s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    try:
        # Generate a pre-signed URL for the template file in S3
        download_url = download_template_docx(file.s3_path, BUCKET_NAME)

        # Return the download URL in the response
        return JSONResponse(status_code=200, content={"download_url": download_url})

    except Exception as e:
        # Log the error and raise HTTP 500 on failure
        logger.error(f"Failed to generate download URL: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

@template_router.post("/edit/")
async def edit_template(file: TemplateFile):
    if not file.s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    logger.info(f"Attempting to edit template file: {file.template_id}")

    # Check if template_id starts with 'temp_'
    current_datetime = datetime.now()
    formatted_timestamp = current_datetime.isoformat()
    update_template_locked_status(file.template_id, True, TEMPLATEMASTER_DYNAMOTABLE, True, formatted_timestamp)
    record = get_template_record_by_template_id(file.template_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Template record not found")
        update_template_locked_status(file.template_id, False, TEMPLATEMASTER_DYNAMOTABLE)
    else:       
        return TemplateMaster(**record)


@template_router.post("/view/")
async def view_template(file: TemplateFile):
    if not file.s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    logger.info(f"Attempting to edit template file: {file.template_id}")

    # Check if template_id starts with 'temp_'
    # current_datetime = datetime.now()
    # formatted_timestamp = current_datetime.isoformat()
    # update_template_locked_status(file.template_id, True, TEMPLATEMASTER_DYNAMOTABLE, True, formatted_timestamp)
    record = get_template_record_by_template_id(file.template_id)

    site_url = SHAREPOINT_MAIN_URL
    full_url = record["sharepoint_link"]
    action = "view"
    # token = get_graph_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT)

    # link = generate_sharepoint_wopi_link_with_graph(site_url, full_url, token, action)
    link = generate_embed_view_url(site_url, full_url, token)

    if link:
        return {"status": "sucess", "embed_link":link}
    else:
        return {"status": "failed", "embed_link":link}


@template_router.post('/release/lock')
async def releaselock_ispr(file: TemplateFile):
    try:
        record = get_template_record_by_template_id(file.template_id)
        update_template_locked_status(record["template_id"], False, TEMPLATEMASTER_DYNAMOTABLE)
        return {"message": "Template-Edition unlocked successfully"}
            
    except Exception as e:
        logger.error(f'Lock release Editor error": {str(e)}')

@template_router.post('/save/template') 
async def update_template_html(template_record: TemplateMaster):
    """
    This API handles the process of saving an edited template in HTML format.

    Args:
        template_record (TemplateMaster): The template data to be saved.

    Returns:
        dict: A success message indicating that the report edition was stored successfully.

    Raises:
        HTTPException: If an exception occurs during the process.

    Steps:
        1. Retrieve the template record data from the input parameter.
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

    
    # Extract report data from the input parameter
    template_records_dict = template_record.dict()
    target_filename = template_records_dict["s3_path"]
    
    # Get the current datetime and format it as an ISO string
    current_datetime = datetime.now()
    formatted_timestamp = current_datetime.isoformat()
    
    # Update report record data
    template_record.created_at = formatted_timestamp    
    # The following DateofEdition indicates that it needs to be blank during Saving.
    template_record.updated_at = None
    template_record.locked = False
    
    try:
        logger.info("\nupdate_template_html: ----------- 1 -----------")
        # Store the report tracking completion data in the DynamoDB table
        target_filename = template_record.s3_path
        logger.info(f"update_template_html:----------- 2 -----------\n{target_filename}")
        
        # Save the HTML content in the specified S3 bucket
        docx_section_dicts = []

        for section in template_record.section_names_desc or []:
            # Support both pydantic models and raw dicts
            title = getattr(section, "title", "") if not isinstance(section, dict) else section.get("title", "")
            desc = getattr(section, "desc", "<p></p>") if not isinstance(section, dict) else section.get("desc", "<p></p>")
            docx_section_dicts.append({"title": title, "desc": desc})

        # Upload document to S3
        s3_path = create_and_upload_docx(
            section_names=docx_section_dicts,
            document_name=target_filename,
            s3_bucket=BUCKET_NAME,
            s3_prefix=""
        )
        logger.info("\nupdate_report_html: ----------- 3 -----------")
        store_templatetrackingcompletion(template_record, TEMPLATEMASTER_DYNAMOTABLE)
        return {"message": "Edited report stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
##################################### This is an utility for developers. #####################################
# # Pydantic models
# class TableCopyRequest(BaseModel):
#     source_table: str
#     destination_table: str

# @template_router.post("/copy-table/")
# async def copy_dynamodb_table(request: TableCopyRequest):
#     """
#     Copies all items from the source DynamoDB table to the destination table.
#     This is an utility for developers. 
#     Not to be exposed to outside world
    
#     Returns:
#         JSONResponse with status and number of copied items.
#     """
#     try:
#         logger.info(f"Starting copy from '{request.source_table}' to '{request.destination_table}'")

#         # Initialize DynamoDB resource using default config (region pulled from environment/profile)
#         dynamodb = boto3.resource('dynamodb')

#         src_table = dynamodb.Table(request.source_table)
#         dest_table = dynamodb.Table(request.destination_table)

#         total_copied = 0
#         response = src_table.scan()
#         items = response.get("Items", [])

#         with dest_table.batch_writer() as batch:
#             for item in items:
#                 batch.put_item(Item=item)
#                 total_copied += 1

#             # Handle pagination
#             while "LastEvaluatedKey" in response:
#                 response = src_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
#                 for item in response.get("Items", []):
#                     batch.put_item(Item=item)
#                     total_copied += 1

#         logger.info(f"Copy completed: {total_copied} items copied from '{request.source_table}' to '{request.destination_table}'")

#         return JSONResponse(status_code=200, content={
#             "status": "success",
#             "message": f"Copied {total_copied} items from '{request.source_table}' to '{request.destination_table}'"
#         })

#     except (ClientError, BotoCoreError) as aws_err:
#         logger.exception("AWS error during DynamoDB copy")
#         raise HTTPException(status_code=502, detail=f"AWS error: {str(aws_err)}")
    
#     except Exception as e:
#         logger.exception("Unexpected error during DynamoDB copy")
#         raise HTTPException(status_code=500, detail="Internal server error during table copy.")

@template_router.post("/template_binary/")
async def binary_template(file: TemplateFile):
    
    if not file.s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    logger.info(f"Attempting to open a template file: {file.s3_path}")

   
    # record = get_template_record_by_s3_path(file.s3_path)
    # if record is None:
    #     raise HTTPException(status_code=404, detail="Template record not found")

    # template_id = record.get("template_id", "")

    # Check if template_id starts with 'temp_'
    # if template_id.startswith("temp_"):
    #     raise HTTPException(
    #         status_code=403,
    #         detail=f"Old existing templates cannot be deleted."
    #     )

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file.s3_path)
        # template_bytes = response['Body'].read()
        # template_file = BytesIO(template_bytes)
         # Generate a pre-signed URL for the template file in S3
        # download_url = download_template_docx(file.s3_path, BUCKET_NAME)
        # Return the download URL in the response
        file_stream = response["Body"]
        
        # return {
        #     "status": "success",
        #     "download_url": download_url,
        #     "template_binary_base64_utf8": base64.b64encode(template_bytes).decode("utf-8")
        # }

        return StreamingResponse(
                file_stream,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={
                    "Content-Disposition": f'inline; filename="{(file.s3_path).split("/")[-1]}"'
                }
            )


    except HTTPException as http_err:
        raise http_err
    except Exception:
        logger.exception("Failed to open the template")
        raise HTTPException(status_code=500, detail="Failed to open template")


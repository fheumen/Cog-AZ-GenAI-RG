# template/template_utils.py
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from boto3.dynamodb.conditions import Key, Attr
from io import BytesIO
from bs4 import BeautifulSoup
from dateutil import parser
import base64
import requests
import urllib.parse
import msal

import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException
from loguru import logger
from docx import Document as Document_docx
from docx.image.exceptions import UnrecognizedImageError

from data import TemplateMaster
from app.config import TEMPLATEMASTER_DYNAMOTABLE, BUCKET_NAME

from PIL import Image
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches


###################
def store_templatetrackingcompletion(interaction: TemplateMaster, table_name:str):
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
        interaction (TemplateMaster): The TemplateMaster interaction to be stored.
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
    table = dynamodb.Table(table_name)
    ############## First delete all previous record from the same ProductName&UserID
    response = table.scan(
        FilterExpression=Attr("template_id").eq(interaction.template_id)
    )

    # Step 2: Delete existing records using correct keys
    for item in response.get('Items', []):
        table.delete_item(Key={
            "template_id": item["template_id"]  # Correct partition key
            # "created_at": item["created_at"]   # Correct sort key
        })
    logger.info("store_templatecompletion ---------- Deleted --------")
    #################### Insert New Records
    try:
        item = interaction.dict()
        ############ delete all previous records about the interaction.ProductName_User
        table.put_item(Item=item)
        logger.info("store_templatecompletion ---------- END --------Report Edition interaction stored successfully")
        return {"message": "Template-Edition interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def update_template_locked_updatedat_created_at(template_id, is_locked, created_at, update_at, table_name):
    logger.info(f"\nupdate_template_locked_updatedat_created_at -------START-------is_locked: {is_locked}")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Construct the update expression
    update_expression = "SET locked = :locked_status, created_at = :created_at_var, updated_at = :updated_at_var"
    expression_attribute_values = {":locked_status": is_locked,
                                   ":created_at_var": created_at,
                                   ":updated_at_var": updated_at
                                    }

    key = {
        'template_id': template_id
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


def update_template_locked_status(template_id, is_locked, table_name, edit_dates_enabled = False, date_of_edit=None):
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
    logger.info(f"\nupdate_template_locked_status -------START-------is_locked: {is_locked}")
    logger.info(f"\nupdate_template_locked_status -------edit_dates_enabled: {edit_dates_enabled}")
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
        'template_id': template_id
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

def scan_and_update_template_status(default_n_mins: int, table_name: str):
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
            # timestamp_placeholder = '#TS'

            # Scan the table for records where Ispr_Editor_Status is True
            response = table.scan(
                FilterExpression=Attr('locked').eq(True),
                ProjectionExpression=f'template_id, locked, updated_at'
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
                logger.info("scan_and_update_template_status: Iterating records")
                
                # Check if the 'DateofEdition' attribute exists
                if 'updated_at' in item:
                    date_of_edit = parser.isoparse(item['updated_at'])
                
                    # Check if the record was edited more than default_n_mins minutes ago
                    if datetime.now() - date_of_edit > timedelta(minutes=default_n_mins):
                        logger.info(f"scan_and_update_template_status: Updating records which are {default_n_mins} min old.")
                
                        # Update the Ispr_Editor_Status to False for the qualifying record
                        update_template_locked_status(item['template_id'],  False, table_name, True)
                        logger.info("scan_and_update_template_status: Updated ----------->")

                else:
                    # Handle the case where 'DateofEdition' is null or missing
                    logger.warning(f"scan_and_update_template_status: 'updated_at' is missing for item {item}")
                    continue

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
    except Exception as e:
        logger.error(f"startup error: {str(e)}")
        
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

def save_html_docx(html_content: str, doc: Document_docx) -> Document_docx:
    """
    Convert an HTML string to a text, table and image and add into a docx object and upload .

    Args:
        html_content (str): The HTML content to be converted to DOCX format.
        doc (Document_docx): The DOCX Object to be used.

    Returns:
        Document_docx

    Raises:
        Exception: If any error occurs during the conversion or upload process.

    This function uses BeautifulSoup to parse the HTML content and convert it to a DOCX document
    using the python-docx library. It handles various HTML elements such as headings, paragraphs,
    images (including base64-encoded images), tables, line breaks, and bold text. The resulting
    DOCX Object will be used later.
    """
    try:
        # Parse HTML using BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")
        # Iterate over HTML elements and convert them
        for element in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "img", "table", "br", "b", "strong"]):
            try:
                # logger.info(f"\nsave_ispr_html ----------- 3.1 -----------")

                if element.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                    logger.info(f"\nsave_html_docx ----------- 3.2 -----------")
                    doc.add_heading(element.get_text(strip=True), level=int(element.name[1]))  # Add headings
                elif element.name == "p":
                    logger.info(f"\nsave_html_docx ----------- 3.3 -----------")
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
                    logger.info(f"\nsave_html_docx ----------- 3.4 -----------")
                    img_src = element["src"]

                    if img_src.startswith("data:image"):  # Handle base64 encoded images
                        logger.info(f"\nsave_html_docx ----------- 3.5 -----------")
                        format, encoded = img_src.split(";base64,")
                        img_ext = format.split("/")[-1]  # Extract image format
                        img_bytes = base64.b64decode(encoded)  # Decode base64 image data

                        # Get original image size
                        width, height = get_image_size(img_bytes)

                        # Add picture to DOCX with the original size
                        doc.add_picture(BytesIO(img_bytes), width=Inches(width))

                elif element.name == "table":
                    logger.info(f"\nsave_html_docx ----------- 3.6 -----------")
                    rows = element.find_all("tr")
                    if rows:
                        logger.info(f"\nsave_html_docx ----------- 3.7 -----------")
                        cols = rows[0].find_all(["th", "td"])  # Get column count from first row
                        table = doc.add_table(rows=len(rows), cols=len(cols))  # Create table with correct number of rows and columns
                        set_table_borders(table)  # Apply borders
                        logger.info(f"\nsave_html_docx ----------- 3.8 -----------")

                        for r_idx, row in enumerate(rows):
                            logger.info(f"\nsave_html_docx ----------- 3.9 -----r_idx: {r_idx}------")
                            cells = row.find_all(["th", "td"])

                            # Check if the number of cells matches the number of columns
                            if len(cells) != len(cols):
                                logger.warning(f"save_html_docx: Number of cells in row {r_idx} does not match the number of columns. Skipping row.")
                                continue

                            for c_idx, cell in enumerate(cells):
                                # Check if the current cell index is within the number of columns
                                if c_idx < len(cols):
                                    logger.info(f"\nsave_html_docx ----------- 3.10 ----c_idx: {c_idx} \tr_idx: {r_idx}-------")
                                    table.cell(r_idx, c_idx).text = cell.get_text(strip=True)  # Add text to each cell
                                    logger.info(f"\nsave_html_docx ----------- 3.11 ----c_idx: {c_idx} \tr_idx: {r_idx} -------")
                                else:
                                    logger.warning(f"save_html_docx: ----------- 3.12 ----Too many cells in row {r_idx}. Skipping additional cells.")
                                    break

                elif element.name == "br":
                    logger.info(f"\nsave_html_docx ----------- 3.13 -----------")
                    doc.add_paragraph().add_run().add_break()  # Add line break

            # Handle the UnrecognizedImageError exception
            except UnrecognizedImageError as uie:
                error_message = f"\nsave_html_docx ----------- 3.14 -----------\nError processing image: {uie}"
                logger.error(error_message)
                # You can log additional information or raise a custom exception
                continue

            except Exception as ex:
                error_message = f"\nsave_html_docx ----------- 3.15 ----------- \nError processing image: {ex}"
                logger.error(error_message)
                continue

        return doc
    except Exception as e:
        logger.error(f"Error occurred while saving: {str(e)}", exc_info=True)
        logger.exception(e)


def create_and_upload_docx(
    section_names: Optional[List[Dict[str, str]]],
    document_name: str,
    s3_bucket: str,
    s3_prefix: str
) -> str:
    """
    Create a DOCX file from provided sections and upload to an S3 bucket.

    Args:
        section_names (Optional[List[Dict[str, str]]]): List of sections with 'title' and 'desc'.
        document_name (str): Name of the document (must end with .docx).
        s3_bucket (str): Target S3 bucket.
        s3_prefix (str): S3 folder prefix path.

    Returns:
        str: Full S3 key (path) of the uploaded file.

    Raises:
        Exception: On DOCX generation or upload failure.
    """
    try:
        doc = Document_docx()
        doc.add_heading(document_name, level=1)

        if section_names:
            logger.info(f"Sections passed: {section_names}")
            for section in section_names:
                title = section.get("title", "Untitled Section")
                desc_html = section.get("desc", "")
                doc.add_heading(title, level=2)
                doc = save_html_docx(desc_html, doc)
                # doc.add_paragraph(desc or "")
        else:
            doc.add_paragraph("No sections provided.")

        output_stream = BytesIO()
        doc.save(output_stream)
        output_stream.seek(0)

        if len(s3_prefix) > 0:
            s3_key = f"{s3_prefix.rstrip('/')}/{document_name}"
        else:
            s3_key = document_name
        s3 = boto3.client('s3')

        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=output_stream.read(),
            ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

        logger.info(f"DOCX uploaded to s3://{s3_bucket}/{s3_key}")
        return s3_key

    except ClientError as e:
        logger.error(f"AWS ClientError during DOCX upload: {e}")
        raise

    except Exception as e:
        logger.error(f"Failed to create/upload DOCX: {e}")
        raise


def dynamo_list_to_python_list_str(dynamo_list: Optional[List[Any]]) -> List[str]:
    """
    Converts a DynamoDB-style list of strings (or a mix) into a plain Python list of strings.

    Args:
        dynamo_list (Optional[List[Any]]): List from DynamoDB, possibly with dicts like {"S": value}.

    Returns:
        List[str]: Cleaned list of string values.
    """
    if not dynamo_list:
        return []

    result = []
    for item in dynamo_list:
        if isinstance(item, dict) and "S" in item:
            result.append(item["S"])
        elif isinstance(item, str):
            result.append(item)
        else:
            # Skip items with unexpected format
            logger.warning(f"Skipping item with unexpected format in DynamoDB list: {item}")
            continue

    return result


def python_list_to_dynamo_list_str(py_list: Optional[List[str]]) -> List[Dict[str, str]]:
    """
    Converts a Python list of strings into DynamoDB string format.

    Args:
        py_list (Optional[List[str]]): List of strings.

    Returns:
        List[Dict[str, str]]: List formatted as [{"S": value}, ...]
    """
    if not py_list:
        return []

    return [{"S": v} for v in py_list]


def store_template_master_record(
    template: TemplateMaster,
    table_name: Optional[str] = None
) -> Dict[str, str]:
    """
    Store a TemplateMaster record in DynamoDB.

    Args:
        template (TemplateMaster): The template record to store.
        table_name (Optional[str]): Optional override for the DynamoDB table name.

    Returns:
        Dict[str, str]: Status message dict.

    Raises:
        HTTPException: If the record is invalid or DynamoDB errors occur.
    """
    table_name = table_name or TEMPLATEMASTER_DYNAMOTABLE
    logger.info("Storing new template record to DynamoDB...")

    if not template.template_id:
        logger.error("Missing required field: template_id")
        raise HTTPException(status_code=400, detail="Missing template_id in TemplateMaster record")

    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        item = template.dict()

        # Convert list fields to DynamoDB-compatible format
        if item.get("doc_types") is not None:
            item["doc_types"] = python_list_to_dynamo_list_str(item["doc_types"])
        if item.get("doc_types_full") is not None:
            item["doc_types_full"] = python_list_to_dynamo_list_str(item["doc_types_full"])
        if item.get("product_names") is not None:
            item["product_names"] = python_list_to_dynamo_list_str(item["product_names"])

        table.put_item(Item=item)
        logger.info(f"Template '{template.template_id}' stored successfully.")

        return {"message": "Template record stored successfully"}

    except ClientError as e:
        logger.exception("DynamoDB ClientError occurred")
        raise HTTPException(status_code=500, detail=f"DynamoDB Error: {e.response['Error']['Message']}")

    except Exception as e:
        logger.exception("Unexpected error occurred while storing template")
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")


def download_template_docx(s3_path: str, bucket_name: str, expiration: int = 3600) -> str:
    """
    Generate a pre-signed URL to download a DOCX file from S3.

    Args:
        s3_path (str): S3 key (path) to the DOCX file.
        bucket_name (str): S3 bucket name.
        expiration (int): URL expiration time in seconds.

    Returns:
        str: Pre-signed URL for downloading the file.

    Raises:
        HTTPException: On AWS errors or URL generation failure.
    """
    try:
        s3_client = boto3.client("s3")
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': s3_path},
            ExpiresIn=expiration
        )
        return response
    except ClientError as e:
        logger.error(f"Failed to generate pre-signed URL: {e}")
        raise HTTPException(status_code=500, detail="Error generating download URL.")
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error during download request.")


def delete_template_docx(s3_path: str, bucket_name: str) -> dict:
    """
    Delete a DOCX file from S3.

    Args:
        s3_path (str): S3 key (path) to the DOCX file.
        bucket_name (str): S3 bucket name.

    Returns:
        dict: Deletion status message.

    Raises:
        HTTPException: On AWS errors or deletion failure.
    """
    try:
        s3_client = boto3.client("s3")
        s3_client.delete_object(Bucket=bucket_name, Key=s3_path)
        logger.info(f"Deleted s3://{bucket_name}/{s3_path}")
        return {"status": "success", "message": "Template deleted successfully"}
    except ClientError as e:
        logger.error(f"Failed to delete file from S3: {e}")
        raise HTTPException(status_code=500, detail="Error deleting template file.")
    except Exception as e:
        logger.error(f"Unexpected error during deletion: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error during delete request.")

def delete_template_records(s3_path: str, tablename: str) -> dict:
    """
    Delete template record(s) from DynamoDB based on S3 path.

    Args:
        s3_path (str): The S3 path of the template to delete.
        tablename (str): Name of the DynamoDB table.

    Returns:
        dict: Status message.

    Raises:
        HTTPException: On failure.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)

    try:
        response = table.scan(
            FilterExpression=Attr("s3_path").eq(s3_path)
        )
        items = response.get("Items", [])

        if not items:
            logger.warning(f"No template records found for S3 path: {s3_path}")
            return {"status": "not_found", "message": "No records found to delete"}

        for item in items:
            if 'template_id' in item:
                table.delete_item(
                    Key={'template_id': item['template_id']}
                )
                logger.info(f"Deleted template metadata: {item['template_id']}")

        return {"status": "success", "message": f"Records for template {s3_path} deleted"}

    except Exception as e:
        logger.exception("Failed to delete template records from DynamoDB")
        raise HTTPException(status_code=500, detail=f"Error deleting template records: {str(e)}")

# ==========================================================
# AUTHENTICATION
# ==========================================================
def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    """Obtain Microsoft Graph API token via client credentials flow."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    response = requests.post(token_url, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"🛑 Failed to get token: {response.status_code} {response.text}")
        return None

def get_graph_token_using_cert(tenant_id, client_id,  s3_private_key, thumbprint) -> Optional[str]:

       # Option 1: private key string (PEM)
    # --- Step 1: Download the PEM private key from S3 ---
    s3 = boto3.client("s3")
    pem_object = s3.get_object(Bucket=BUCKET_NAME, Key=s3_private_key)
    private_key_pem = pem_object["Body"].read().decode("utf-8")
    
    cert = {
        "private_key": private_key_pem,
        # the cert thumbprint (hex, no spaces, uppercase or lowercase fine)
        "thumbprint": thumbprint
        # optionally: "public_certificate": "<PEM public cert>" to send x5c header
    }
    
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=cert
    )
    
    # Acquire token for Graph app-only:
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    if "access_token" in result:
        print("access_token:", result["access_token"])
        return result["access_token"]
    else:
        print("error:", result.get("error"), result.get("error_description"))
        return None

# ==========================================================
# HELPER — EXTRACT RELATIVE PATH FROM LINK
# ==========================================================
def extract_server_relative_url(full_url: str | None, main_url: str | None) -> str:    

    if main_url in full_url:
        path = full_url.split(main_url, 1)[1]
    else:
        path = full_url
    return path
    
# ==========================================================
# GRAPH CALL
# ==========================================================
def get_file_metadata_via_graph(site_url: str, token: str, file_rel_url: str) -> Optional[dict]:
    """Get file metadata, including the SharePoint-specific UniqueId (GUID)."""
    try:
        decoded_path = urllib.parse.unquote(file_rel_url).strip('/')
        parts = decoded_path.split('/')

        # --- Path Parsing ---
        site_name = None
        site_path = None
        site_index = -1
        for i, part in enumerate(parts):
            if part.lower() == 'sites' and i + 1 < len(parts):
                site_name = parts[i + 1]
                site_index = i
                # Get the site-relative path (e.g., /sites/SCTASK1629542)
                site_path = '/' + '/'.join(parts[:i+2])
                break
        
        if not site_name:
            raise ValueError("Could not extract site name. Check URL structure.")

        library_start_index = site_index + 2 
        library_name = parts[library_start_index]
        file_path_segments = parts[library_start_index + 1:]
        
        if not library_name or not file_path_segments:
             raise ValueError("Could not extract document library or file path.")
             
        file_path = '/'.join(file_path_segments)
        
        print(f"📂 Site: {site_name}")
        print(f"📁 Library: {library_name}")
        print(f"📄 Decoded File path: {file_path}")

        # --- API Setup ---
        hostname = site_url.replace('https://', '').replace('http://', '')
        site_graph_path = f"sites/{hostname}:/sites/{site_name}"
        drives_url = f"https://graph.microsoft.com/v1.0/{site_graph_path}:/drives"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        # 1. Get Drive ID
        drives_resp = requests.get(drives_url, headers=headers)
        if drives_resp.status_code != 200:
            print(f"🛑 Error getting drives: {drives_resp.text}")
            return None

        drives = drives_resp.json().get('value', [])
        drive = next((d for d in drives if d.get('name', '').lower() == library_name.lower()), None)
        if not drive:
            raise ValueError(f"Could not find document library '{library_name}'")

        drive_id = drive['id']
        
        # 2. Get File Metadata and SharePoint IDs
        encoded_file_path = urllib.parse.quote(file_path, safe='/') 
        
        # 💥 CRITICAL FIX: Explicitly request the sharepointIds facet to get the UniqueId/GUID
        file_metadata_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}:/?$select=id,sharepointIds"
        
        file_resp = requests.get(file_metadata_url, headers=headers)

        if file_resp.status_code == 200:
            data = file_resp.json()
            
            # The 'UniqueId' property is the GUID needed for the Doc.aspx sourcedoc parameter.
            sharepoint_guid = data.get('sharepointIds', {}).get('listItemUniqueId')
            
            if not sharepoint_guid:
                print("⚠️ Warning: Could not find listItemUniqueId. Falling back to DriveItem ID.")
                sharepoint_guid = data.get('id')
            
            return {
                'id': sharepoint_guid, # Use the SharePoint Unique ID
                'site_path': site_path, # e.g., /sites/SCTASK1629542
                'name': data.get('name'),
            }
        else:
            print(f"🛑 Graph API Error {file_resp.status_code}: {file_resp.text}")
            return None

    except Exception as e:
        print(f"🛑 Error getting file metadata: {e}")
        return None


# ==========================================================
# EMBED VIEW LINK GENERATOR (DOC.ASPX FORMAT)
# ==========================================================
def generate_embed_view_url(site_url: str, full_url: str, token: str) -> Optional[str]:
    """
    Generates the Doc.aspx?sourcedoc={GUID}&action=embedview link.
    """
    file_rel_url = extract_server_relative_url(full_url, site_url)
    print(f"🔍 Extracted file relative URL for embed: {file_rel_url}")

    metadata = get_file_metadata_via_graph(site_url, token, file_rel_url)
    if not metadata or not metadata.get('id') or not metadata.get('site_path'):
        print("🛑 Failed to get file GUID or site path.")
        return None

    # The 'id' is now the SharePoint UniqueId (GUID)
    file_guid = metadata['id']
    site_path = metadata['site_path']
    
    # Example Target: https://azcollaborationtst.sharepoint.com/sites/SCTASK1629542/_layouts/15/Doc.aspx?sourcedoc={GUID}&action=embedview
    
    # 1. Construct the base path
    base_embed_url = f"{site_url}{site_path}/_layouts/15/Doc.aspx"
    
    # 2. Construct the final URL (GUID must be wrapped in curly braces)
    final_embed_url = (
        f"{base_embed_url}?"
        f"sourcedoc={{{file_guid}}}"
        f"&action=embedview"
    )

    return final_embed_url

# ==========================================================
# WOPI LINK GENERATOR
# ==========================================================
# def generate_sharepoint_wopi_link_with_graph(site_url: str, full_url: str, token: str, action: str) -> Optional[str]:
#     """Generate WOPI edit/view link from a full SharePoint link."""
#     file_rel_url = extract_server_relative_url(full_url, site_url)
#     print(f" Extracted file relative URL: {file_rel_url}")

#     metadata = get_file_metadata_via_graph(site_url, token, file_rel_url)
#     if not metadata:
#         return None

#     web_url = metadata.get('webUrl', '')
#     if web_url:
#         if action == 'edit':
#             return f"{web_url}?web=1&action=edit" if '?web=1' not in web_url else web_url.replace('?web=1', '?web=1&action=edit')
#         else:
#             return web_url if '?web=1' in web_url else f"{web_url}?web=1"
#     return None

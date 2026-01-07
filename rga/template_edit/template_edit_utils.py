from typing import Optional
import boto3
import msal
import requests
from loguru import logger
import time
import asyncio
from urllib.parse import quote
from datetime import datetime, timezone
from io import BytesIO

from utils import send_email
from config import EMAIL_SENDER

dynamodb = boto3.resource('dynamodb')

def get_graph_token_using_cert(tenant_id: str, client_id: str, s3_private_key: str, thumbprint: str, bucket_name: str) -> Optional[str]:
    """Get Microsoft Graph API token using certificate authentication."""
    try:
        s3 = boto3.client("s3")
        pem_object = s3.get_object(Bucket=bucket_name, Key=s3_private_key)
        private_key_pem = pem_object["Body"].read().decode("utf-8")
        
        cert = {
            "private_key": private_key_pem,
            "thumbprint": thumbprint
        }
        
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=cert
        )
        
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" in result:
            logger.info("Successfully obtained access token")
            return result["access_token"]
        else:
            logger.error(f"Token acquisition failed: {result.get('error')}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting Graph token: {str(e)}")
        return None

def copy_sharepoint_file_to_draft(token: str, filename: str, user_id: str, hostname: str, site_name: str, library_name: str, edit_id: str = None) -> Optional[str]:
    """Copy file from templates folder to Draft/{user_id} folder and return edit link."""
    try:

        # Remove https:// or http:// from hostname
        hostname = hostname.replace("https://", "").replace("http://", "")

        source_file_path = f"templates/{filename}"
        # Include edit_id in path for history tracking
        draft_path = f"Draft_Templates/{user_id}/{edit_id}" if edit_id else f"Draft_Templates/{user_id}"
        
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}:/drives"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
        
        drives_resp = requests.get(drives_url, headers=headers)
        if drives_resp.status_code != 200:
            logger.error(f"Error getting drives: {drives_resp.text}")
            return None
            
        drives = drives_resp.json().get('value', [])
        drive = next((d for d in drives if d.get('name').lower() == library_name.lower()), None)
        
        if not drive:
            logger.error(f"Document library '{library_name}' not found")
            return None
            
        drive_id = drive['id']
        
        # Verify source file exists
        source_file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{source_file_path}"
        source_resp = requests.get(source_file_url, headers=headers)
        
        if source_resp.status_code != 200:
            logger.error(f"Source file not found: {source_resp.text}")
            return None
        
        source_item_id = source_resp.json().get('id')
        
        # Ensure Draft_Templates folder structure exists
        encoded_draft_path = quote(draft_path)
        user_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_draft_path}"
        user_resp = requests.get(user_folder_url, headers=headers)
        if user_resp.status_code == 404:
            draft_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/Draft_Templates"
            draft_resp = requests.get(draft_folder_url, headers=headers)
            if draft_resp.status_code == 404:
                create_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/children"
                requests.post(create_folder_url, headers=headers, json={"name": "Draft_Templates", "folder": {}})
            
            create_user_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/Draft_Templates:/children"
            requests.post(create_user_folder_url, headers=headers, json={"name": user_id, "folder": {}})
            
            # Create edit_id subfolder if provided
            if edit_id:
                user_resp = requests.get(user_folder_url, headers=headers)
                if user_resp.status_code == 404:
                    encoded_user_path = quote(f"Draft_Templates/{user_id}")
                    user_folder_url_parent = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_user_path}"
                    user_resp = requests.get(user_folder_url_parent, headers=headers)
                
                if user_resp.status_code == 200:
                    encoded_user_path = quote(f"Draft_Templates/{user_id}")
                    create_edit_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_user_path}:/children"
                    requests.post(create_edit_folder_url, headers=headers, json={"name": edit_id, "folder": {}})
            
            user_resp = requests.get(user_folder_url, headers=headers)
        
        if user_resp.status_code != 200:
            logger.error(f"Cannot access destination folder: {user_resp.text}")
            return None
        
        parent_id = user_resp.json().get('id')
        
        # Copy using item ID
        copy_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{source_item_id}/copy"
        copy_data = {
            "parentReference": {
                "driveId": drive_id,
                "id": parent_id
            },
            "name": filename
        }
        
        copy_resp = requests.post(copy_url, headers=headers, json=copy_data)
        if copy_resp.status_code in [200, 201, 202]:
            time.sleep(2)
            
            encoded_file_path = quote(f"{draft_path}/{filename}")
            draft_file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}"
            for _ in range(5):
                file_resp = requests.get(draft_file_url, headers=headers)
                if file_resp.status_code == 200:
                    web_url = file_resp.json().get('webUrl', '')
                    if web_url:
                        edit_url = f"{web_url}?web=1&action=edit" if '?web=1' not in web_url else web_url.replace('?web=1', '?web=1&action=edit')
                        logger.info(f"Edit URL generated: {edit_url}")
                        return edit_url
                time.sleep(1)
        
        logger.error(f"Failed to copy file: {copy_resp.status_code} - {copy_resp.text}")
        return None
        
    except Exception as e:
        logger.error(f"Error copying SharePoint file: {str(e)}")
        return None

def replace_original_template_with_draft(token: str, filename: str, user_id: str, hostname: str, site_name: str, library_name: str, edit_id: str = None) -> bool:
    """Replace original template file with the edited draft version."""
    try:
        # Remove https:// or http:// from hostname
        hostname = hostname.replace("https://", "").replace("http://", "")

        # Include edit_id in path for history tracking
        draft_path = f"Draft_Templates/{user_id}/{edit_id}/{filename}" if edit_id else f"Draft_Templates/{user_id}/{filename}"
        original_path = f"templates/{filename}"
        
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}:/drives"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
        
        drives_resp = requests.get(drives_url, headers=headers)
        if drives_resp.status_code != 200:
            logger.error(f"Error getting drives: {drives_resp.text}")
            return False
            
        drives = drives_resp.json().get('value', [])
        drive = next((d for d in drives if d.get('name').lower() == library_name.lower()), None)
        
        if not drive:
            logger.error(f"Document library '{library_name}' not found")
            return False
            
        drive_id = drive['id']
        
        # Get draft file ID
        encoded_draft_path = quote(draft_path)
        draft_file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_draft_path}"
        draft_resp = requests.get(draft_file_url, headers=headers)
        if draft_resp.status_code != 200:
            logger.error(f"Draft file not found: {draft_resp.text}")
            return False
        
        draft_item_id = draft_resp.json().get('id')
        
        # Delete original file first
        original_file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{original_path}"
        original_resp = requests.get(original_file_url, headers=headers)
        if original_resp.status_code == 200:
            original_item_id = original_resp.json().get('id')
            delete_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{original_item_id}"
            delete_resp = requests.delete(delete_url, headers=headers)
            if delete_resp.status_code not in [200, 204]:
                logger.error(f"Failed to delete original file: {delete_resp.status_code} - {delete_resp.text}")
                return False
            logger.info(f"Deleted original file: {filename}")
            time.sleep(1)
        
        # Get templates folder ID
        templates_folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/templates"
        templates_resp = requests.get(templates_folder_url, headers=headers)
        if templates_resp.status_code != 200:
            logger.error(f"Templates folder not found: {templates_resp.text}")
            return False
        
        templates_folder_id = templates_resp.json().get('id')
        
        # Copy draft to templates
        copy_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{draft_item_id}/copy"
        copy_data = {
            "parentReference": {
                "driveId": drive_id,
                "id": templates_folder_id
            },
            "name": filename
        }
        
        copy_resp = requests.post(copy_url, headers=headers, json=copy_data)
        if copy_resp.status_code in [200, 201, 202]:
            time.sleep(3)
            
            for _ in range(10):
                verify_resp = requests.get(original_file_url, headers=headers)
                if verify_resp.status_code == 200:
                    logger.info(f"Successfully replaced original template: {filename}")
                    return True
                time.sleep(1)
        
        logger.error(f"Failed to copy draft to templates: {copy_resp.status_code} - {copy_resp.text}")
        return False
        
    except Exception as e:
        logger.error(f"Error replacing template: {str(e)}")
        return False

def copy_draft_to_s3(token: str, draft_path: str, new_s3_path: str, site_url: str, site_name: str, doc_library: str, bucket_name: str):
    """
    Copies a draft file from SharePoint to a new versioned path in S3.
    """
    hostname = site_url.replace('https://', '').replace('http://', '')
    sp_site_path = f"/sites/{site_name}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        # 1. Get Site ID
        site_info_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{sp_site_path}"
        site_info_resp = requests.get(site_info_url, headers=headers)
        site_info_resp.raise_for_status()
        site_id = site_info_resp.json()["id"]

        # 2. Get Drive ID (for the document library)
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        drives_resp = requests.get(drives_url, headers=headers)
        drives_resp.raise_for_status()
        drives = drives_resp.json().get("value", [])
        drive_id = next((d["id"] for d in drives if d["name"].lower() == doc_library.lower()), None)
        if not drive_id:
            raise ValueError(f"Document library '{doc_library}' not found.")

        # 3. Download file content from SharePoint
        encoded_draft_path = quote(draft_path)
        download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_draft_path}:/content"
        download_resp = requests.get(download_url, headers=headers)
        download_resp.raise_for_status()
        file_content = download_resp.content

        # 4. Upload to S3
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(BytesIO(file_content), bucket_name, new_s3_path)
        
        logger.info(f"Successfully copied draft '{draft_path}' to S3 at '{new_s3_path}'")
        return True

    except Exception as e:
        logger.error(f"Failed to copy draft from SharePoint to S3: {e}")
        return False


# User and notification utility functions

def get_user_info(user_id: str, users_table_name: str) -> Optional[dict]:
    """Get user information from DynamoDB."""
    users_table = dynamodb.Table(users_table_name)
    response = users_table.get_item(Key={"pr_id": user_id})
    return response.get("Item")

def has_bpo_role(user_id: str, users_table_name: str) -> bool:
    """Check if user has BPO role."""
    user = get_user_info(user_id, users_table_name)
    if not user:
        return False
    user_groups = user.get("user_group", [])
    for group in user_groups:
        if isinstance(group, dict) and group.get("S") == "BPO":
            return True
        if isinstance(group, str) and group == "BPO":
            return True
    return False

def get_all_bpo_users(users_table_name: str):
    """Get all users with BPO role."""
    users_table = dynamodb.Table(users_table_name)
    response = users_table.scan()
    bpo_users = []
    for user in response.get("Items", []):
        user_groups = user.get("user_group", [])
        for group in user_groups:
            is_bpo = False
            if isinstance(group, dict):
                is_bpo = group.get("S") == "BPO"
            elif isinstance(group, str):
                is_bpo = group == "BPO"
            if is_bpo:
                bpo_users.append(user)
                break
    return bpo_users

async def send_approval_notification(template_id: str, template_name: str, submitted_by: str, users_table_name: str, send_email_func):
    """Send email notification to all BPO users when template is submitted for approval."""
    
    try:
        bpo_users = get_all_bpo_users(users_table_name)
        user = get_user_info(submitted_by, users_table_name)
        if user:
            submitted_by = user.get("email_id")
            submitted_by_name = user.get("name")
        
        async def send_single_email(bpo_user):
            email = bpo_user.get("email_id")
            if email and bpo_user.get("pr_id") and bpo_user.get("pr_id") in ["kkrt332", "kjrs049"] :
                subject = f"Template Approval Required: {template_name}"
                body = f"""A template has been submitted for approval.
                
Template ID: {template_id}
Template Name: {template_name}
Submitted By: {submitted_by_name}
                
Please review and approve/reject the template."""
                
                await asyncio.to_thread(send_email_func, mail_body=body, email_to=email, subject=subject, email_function_name=EMAIL_SENDER)
        
        await asyncio.gather(*[send_single_email(user) for user in bpo_users])
    except Exception as e:
        logger.error(f"Failed to send approval notification: {str(e)}")

async def send_approval_decision_notification(user_id: str, template_name: str, action: str, users_table_name: str, send_email_func, rejection_reason: str = None):
    """Send email notification to user when template is approved or rejected."""
    
    try:
        user = get_user_info(user_id, users_table_name)
        if not user:
            return
            
        email = user.get("email_id")
        if not email:
            return
            
        if action == "approved":
            subject = f"Template Approved: {template_name}"
            body = f"""Your template has been approved.

Template Name: {template_name}

The template has been updated and is now available."""
        else:
            subject = f"Template Rejected: {template_name}"
            body = f"""Your template has been rejected.

Template Name: {template_name}
Reason: {rejection_reason or 'No reason provided'}

Please review the feedback and resubmit if needed."""
        
        await asyncio.to_thread(send_email_func, mail_body=body, email_to=email, subject=subject, email_function_name=EMAIL_SENDER)
    except Exception as e:
        logger.error(f"Failed to send approval decision notification: {str(e)}")

def log_audit(template_id: str, action_type: str, user_id: str, previous_status: str, new_status: str, 
              audit_table_name: str, edit_id: str = None, metadata: dict = None, rejection_reason: str = None):
    """Log action to audit table"""
    
    audit_table = dynamodb.Table(audit_table_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Append timestamp to action_type to make SK unique
    action_type_with_timestamp = f"{action_type}#{timestamp}"
    
    item = {
        "template_id": template_id,
        "action_type": action_type_with_timestamp,  # SK with timestamp
        "timestamp": timestamp,
        "action_type_name": action_type,  # Store original action type
        "user_id": user_id,
        "previous_status": previous_status,
        "new_status": new_status
    }
    
    if edit_id:
        item["edit_id"] = edit_id
    if rejection_reason:
        item["rejection_reason"] = rejection_reason
    if metadata:
        item["metadata"] = metadata
    
    audit_table.put_item(Item=item)

def get_template_info(template_id: str, template_table_name: str) -> Optional[dict]:
    """Get template information from DynamoDB"""
    master_table = dynamodb.Table(template_table_name)
    response = master_table.get_item(Key={"template_id": template_id})
    return response.get("Item")

def get_sharepoint_file_url(token: str, file_path: str, hostname: str, site_name: str, library_name: str, view_only: bool = False) -> Optional[dict]:
    """Get SharePoint file URLs with both view link and embed URL"""
    try:
        hostname = hostname.replace("https://", "").replace("http://", "")
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}:/drives"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
        
        drives_resp = requests.get(drives_url, headers=headers)
        if drives_resp.status_code != 200:
            logger.error(f"Failed to access SharePoint: {drives_resp.text}")
            return None
        
        drives = drives_resp.json().get('value', [])
        drive = next((d for d in drives if d.get('name').lower() == library_name.lower()), None)
        
        if not drive:
            logger.error(f"Document library '{library_name}' not found")
            return None
        
        drive_id = drive['id']
        from urllib.parse import quote
        encoded_file_path = quote(file_path)
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}:/?$select=id,webUrl,sharepointIds"
        
        file_resp = requests.get(file_url, headers=headers)
        if file_resp.status_code != 200:
            logger.error(f"File not found in SharePoint: {file_resp.text}")
            return None
        
        item_id = file_resp.json().get('id')
        web_url = file_resp.json().get('webUrl', '')
        
        # Create organization sharing link
        share_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/createLink"
        share_data = {
            "type": "view" if view_only else "edit",
            "scope": "organization"
        }
        share_resp = requests.post(share_url, headers=headers, json=share_data)
        
        view_link = None
        if share_resp.status_code in [200, 201]:
            view_link = share_resp.json().get('link', {}).get('webUrl')
        
        # Fallback to web URL
        if not view_link and web_url:
            action = "view" if view_only else "edit"
            view_link = f"{web_url}?web=1&action={action}"
        
        # Create embed URL for iframe
        # Create embed URL for iframe using sharepointIds
        embed_url = None
        sharepoint_ids = file_resp.json().get('sharepointIds', {})
        list_item_unique_id = sharepoint_ids.get('listItemUniqueId')
        if list_item_unique_id:
            embed_url = f"https://{hostname}/sites/{site_name}/_layouts/15/Doc.aspx?sourcedoc={{{list_item_unique_id}}}&action=embedview"
        elif web_url:
            embed_url = f"{web_url}?web=1&action=embedview"
        
        if view_link:
            logger.info(f"Created {'view' if view_only else 'edit'} links")
            return {"view_link": view_link, "embed_url": embed_url or view_link}
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting SharePoint file URL: {str(e)}")
        return None

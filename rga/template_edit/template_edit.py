from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Attr
from loguru import logger
import uuid
import os

from config import TEMPLATEMASTER_DYNAMOTABLE, USERS_DYNAMOTABLE, TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT, BUCKET_NAME, SHAREPOINT_MAIN_URL, SP_SITE_NAME, SP_DOCUMENT_LIBRARY, TEMPLATE_AUDIT_LOG_TABLE
from template_edit.template_edit_utils import (
    get_graph_token_using_cert, copy_sharepoint_file_to_draft, replace_original_template_with_draft, copy_draft_to_s3, has_bpo_role, send_approval_notification, send_approval_decision_notification,
    log_audit, get_template_info, get_sharepoint_file_url
)
from utils import send_email

template_edit_router = APIRouter()

dynamodb = boto3.resource('dynamodb')

class EditTemplateRequest(BaseModel):
    template_id: str
    user_id: str

class SubmitForApprovalRequest(BaseModel):
    template_id: str
    user_id: str

class ApprovalRequest(BaseModel):
    template_id: str
    bpo_user_id: str
    action: str
    rejection_reason: Optional[str] = None

class ViewTemplateRequest(BaseModel):
    template_id: str
    edit_id: str

@template_edit_router.post("/edit/start")
async def start_template_edit(request: EditTemplateRequest):
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    
    template = get_template_info(request.template_id, TEMPLATEMASTER_DYNAMOTABLE)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    current_status = template.get("lock_status", "UNLOCKED")
    locked_by = template.get("locked_by")
    
    # Allow editing if: UNLOCKED, APPROVED, None status, or locked by same user
    if current_status in ["UNLOCKED", "APPROVED", None] or locked_by is None:
        # Proceed with edit
        pass
    elif current_status == "LOCKED" and locked_by != request.user_id:
        raise HTTPException(status_code=409, detail="Template is already being edited by another user")
    elif current_status == "PENDING_APPROVAL":
        raise HTTPException(status_code=409, detail="Template is pending approval and cannot be edited")
    
    try:
        s3_path = template.get("s3_path", "")
        filename = s3_path.split("/")[-1] if s3_path else None
        if not filename:
            raise HTTPException(status_code=404, detail="Template file not found")
        
        token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT, BUCKET_NAME)
        if not token:
            raise HTTPException(status_code=500, detail="Failed to authenticate")
        
        now = datetime.now(timezone.utc).isoformat()
        edit_id = uuid.uuid4().hex
        
        edit_link = copy_sharepoint_file_to_draft(token, filename, request.user_id, SHAREPOINT_MAIN_URL, SP_SITE_NAME, SP_DOCUMENT_LIBRARY, edit_id)
        if not edit_link:
            raise HTTPException(status_code=500, detail="Failed to create draft copy")
        
        draft_path = f"Draft_Templates/{request.user_id}/{edit_id}/{filename}"
        
        current_version = template.get("file_version", 0)
        new_version = current_version + 1
        
        master_table.update_item(
            Key={"template_id": request.template_id},
            UpdateExpression="SET lock_status = :status, locked_by = :user, locked_at = :time, draft_path = :draft, updated_at = :updated, updated_by = :user, edit_id = :edit_id",
            ExpressionAttributeValues={
                ":status": "LOCKED",
                ":user": request.user_id,
                ":time": now,
                ":draft": draft_path,
                ":updated": now,
                ":edit_id": edit_id
            }
        )        
        log_audit(request.template_id, "LOCK", request.user_id, current_status, "LOCKED", TEMPLATE_AUDIT_LOG_TABLE, edit_id, {"draftPath": draft_path, "version": new_version})
        
        return {
            "template_id": request.template_id,
            "edit_link": edit_link,
            "draft_path": draft_path,
            "status": "LOCKED"
        }
    except Exception as e:
        logger.error(f"Error starting edit: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@template_edit_router.post("/edit/submit")
async def submit_for_approval(request: SubmitForApprovalRequest):
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    
    template = get_template_info(request.template_id, TEMPLATEMASTER_DYNAMOTABLE)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    if template.get("lock_status") != "LOCKED":
        raise HTTPException(status_code=400, detail="Template is not in locked state")
    
    if template.get("locked_by") != request.user_id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    now = datetime.now(timezone.utc).isoformat()
    
    edit_id = template.get("edit_id")
    
    master_table.update_item(
        Key={"template_id": request.template_id},
        UpdateExpression="SET lock_status = :status, approval_submitted_at = :time, updated_at = :updated",
        ExpressionAttributeValues={
            ":status": "PENDING_APPROVAL",
            ":time": now,
            ":updated": now
        }
    )
    
    log_audit(request.template_id, "SUBMIT_APPROVAL", request.user_id, "LOCKED", "PENDING_APPROVAL", TEMPLATE_AUDIT_LOG_TABLE, edit_id)

    template_name = template.get("template_fullname", "Unknown Template")
    await send_approval_notification(request.template_id, template_name, request.user_id, USERS_DYNAMOTABLE, send_email)
    
    return {"message": "Template submitted for approval", "status": "PENDING_APPROVAL"}

@template_edit_router.get("/edit/my-edits/{user_id}")
async def get_my_edits(user_id: str):
    audit_table = dynamodb.Table(TEMPLATE_AUDIT_LOG_TABLE)
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    
    # Query using UserId-Timestamp-Index GSI
    response = audit_table.query(
        IndexName="UserId-Timestamp-Index",
        KeyConditionExpression="user_id = :uid",
        ExpressionAttributeValues={":uid": user_id},
        ScanIndexForward=False
    )
    
    # Group by edit_id and get latest status
    edits_map = {}
    for item in response.get("Items", []):
        edit_id = item.get("edit_id")
        if not edit_id:
            continue
            
        if edit_id not in edits_map or item["timestamp"] > edits_map[edit_id]["timestamp"]:
            edits_map[edit_id] = item
    
    # Map action_type to display status
    status_map = {
        "LOCK": "Active",
        "SUBMIT_APPROVAL": "Submitted for approval",
        "APPROVE": "Approved",
        "REJECT": "Rejected",
        "DISCARD": "Discarded"
    }
    
    # Enrich with template info
    result = []
    for edit_id, audit_item in edits_map.items():
        template_id = audit_item.get("template_id")
        template = master_table.get_item(Key={"template_id": template_id}).get("Item", {})
        
        # Get version from LOCK action for this edit_id
        version = None
        lock_response = audit_table.query(
            KeyConditionExpression="template_id = :tid",
            ExpressionAttributeValues={
                ":tid": template_id
            }
        )
        # Filter in code for edit_id and LOCK action
        for item in lock_response.get("Items", []):
            if item.get("edit_id") == edit_id and item.get("action_type_name") == "LOCK":
                metadata = item.get("metadata", {})
                version = metadata.get("version")
                break
        
        result.append({
            "edit_id": edit_id,
            "template_id": template_id,
            "template_name": template.get("template_name", "Unknown"),
            "template_fullname": template.get("template_fullname", "Unknown"),
            "lock_status": status_map.get(audit_item.get("action_type_name"), audit_item.get("action_type_name")),
            "timestamp": audit_item["timestamp"],
            "version": version,
            "rejection_reason": audit_item.get("rejection_reason"),
            "s3_path": template.get("s3_path", ""),
            "draft_path": template.get("draft_path", "")
        })
    
    return {"edits": result}


@template_edit_router.get("/edit/pending-approvals")
async def get_pending_approvals(user_id: str):
    if not has_bpo_role(user_id, USERS_DYNAMOTABLE):
        raise HTTPException(status_code=403, detail="User does not have BPO role")
    
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    response = master_table.scan(FilterExpression=Attr("lock_status").eq("PENDING_APPROVAL"))
    items = response.get("Items", [])
    for item in items:
        item["file_version"] = int(item.get("file_version", 0)) + 1
    return {"pending_approvals": items}

@template_edit_router.post("/edit/view")
async def get_template_edit_link(request: ViewTemplateRequest):
    """Get view link for specific edit version by edit_id."""
    template = get_template_info(request.template_id, TEMPLATEMASTER_DYNAMOTABLE)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    try:
        s3_path = template.get("s3_path", "")
        filename = s3_path.split("/")[-1] if s3_path else None
        if not filename:
            raise HTTPException(status_code=404, detail="Template file not found")
        
        # Get user_id from audit log
        audit_table = dynamodb.Table(TEMPLATE_AUDIT_LOG_TABLE)
        audit_response = audit_table.query(
            KeyConditionExpression="template_id = :tid",
            FilterExpression="edit_id = :eid",
            ExpressionAttributeValues={
                ":tid": request.template_id,
                ":eid": request.edit_id
            }
        )
        audit_items = audit_response.get("Items", [])
        if not audit_items:
            raise HTTPException(status_code=404, detail="Edit not found")
        
        user_id = audit_items[0].get("user_id")
        if not user_id:
            raise HTTPException(status_code=404, detail="User not found for edit")
        
        file_path = f"Draft_Templates/{user_id}/{request.edit_id}/{filename}"
        
        token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT, BUCKET_NAME)
        if not token:
            raise HTTPException(status_code=500, detail="Failed to authenticate")
        
        urls = get_sharepoint_file_url(token, file_path, SHAREPOINT_MAIN_URL, SP_SITE_NAME, SP_DOCUMENT_LIBRARY, view_only=True)
        if not urls:
            raise HTTPException(status_code=500, detail="Failed to generate view link")
        
        return {
            "template_id": request.template_id,
            "edit_id": request.edit_id,
            "view_link": urls.get('view_link'),
            "embed_url": urls.get('embed_url')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting view link: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@template_edit_router.post("/edit/approve")
async def approve_or_reject(request: ApprovalRequest):
    if not has_bpo_role(request.bpo_user_id, USERS_DYNAMOTABLE):
        raise HTTPException(status_code=403, detail="User does not have BPO role")
    
    if request.action not in ["approved", "rejected"]:
        raise HTTPException(status_code=400, detail="Action must be 'approved' or 'rejected'")
    
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    
    template = get_template_info(request.template_id, TEMPLATEMASTER_DYNAMOTABLE)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    if template.get("lock_status") != "PENDING_APPROVAL":
        raise HTTPException(status_code=400, detail="Template is not pending approval")
    
    now = datetime.now(timezone.utc).isoformat()
    s3_path = template.get("s3_path", "")
    filename = s3_path.split("/")[-1] if s3_path else None
    locked_by = template.get("locked_by")
    template_name = template.get("template_fullname", "Unknown Template")
    
    if request.action == "approved":
        draft_path = template.get("draft_path")
        current_version = template.get("file_version", 0)
        new_version = current_version + 1

        if not s3_path or not draft_path:
            raise HTTPException(status_code=400, detail="Template record is missing s3_path or draft_path.")

        parent_dir = os.path.dirname(s3_path)
        
        # If the parent directory is a version folder (e.g. v1, v2), go up one level
        if os.path.basename(parent_dir) == f"v{current_version}":
            parent_dir = os.path.dirname(parent_dir)
        
        # Create a new versioned path. Example: inputs/sv_docx_templates/v1/MyTemplate.docx
        new_s3_path = os.path.join(parent_dir, f"v{new_version}", filename).replace("\\", "/")

        token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT, BUCKET_NAME)
        if not token:
            raise HTTPException(status_code=500, detail="Failed to authenticate with SharePoint")

        # Copy from SharePoint draft to new S3 versioned path
        copy_success = copy_draft_to_s3(
            token, draft_path, new_s3_path,
            SHAREPOINT_MAIN_URL, SP_SITE_NAME, SP_DOCUMENT_LIBRARY, BUCKET_NAME
        )

        if not copy_success:
            raise HTTPException(status_code=500, detail="Failed to copy approved template from SharePoint to S3.")

        edit_id = template.get("edit_id")
        replace_success = replace_original_template_with_draft(token, filename, locked_by, SHAREPOINT_MAIN_URL, SP_SITE_NAME, SP_DOCUMENT_LIBRARY, edit_id)
        if not replace_success:
            logger.warning(f"Failed to replace original template in SharePoint for {request.template_id}")

        master_table.update_item(
            Key={"template_id": request.template_id},
            UpdateExpression="SET lock_status = :status, approved_by = :approver, approved_at = :time, locked_by = :null, locked_at = :null, draft_path = :null, updated_at = :updated, file_version = :new_version, s3_path = :new_s3_path",
            ExpressionAttributeValues={
                ":status": "APPROVED",
                ":approver": request.bpo_user_id,
                ":time": now,
                ":null": None,
                ":updated": now,
                ":new_version": new_version,
                ":new_s3_path": new_s3_path
            }
        )
        
        log_audit(request.template_id, "APPROVE", locked_by, "PENDING_APPROVAL", "APPROVED", TEMPLATE_AUDIT_LOG_TABLE, edit_id)
        await send_approval_decision_notification(locked_by, template_name, "approved", USERS_DYNAMOTABLE, send_email)
        
        return {"message": "Template approved", "status": "APPROVED"}
    
    else:  # rejected
        master_table.update_item(
            Key={"template_id": request.template_id},
            UpdateExpression="SET lock_status = :status, locked_by = :null, locked_at = :null, draft_path = :null, approver_id = :null, updated_at = :updated",
            ExpressionAttributeValues={
                ":status": "UNLOCKED",
                ":null": None,
                ":updated": now
            }
        )
        
        edit_id = template.get("edit_id")
        log_audit(request.template_id, "REJECT", locked_by, "PENDING_APPROVAL", "REJECTED", TEMPLATE_AUDIT_LOG_TABLE, edit_id, 
                 {"rejectedBy": request.bpo_user_id, "rejectedAt": now}, request.rejection_reason)
        await send_approval_decision_notification(locked_by, template_name, "rejected", USERS_DYNAMOTABLE, send_email, request.rejection_reason)
        
        return {"message": "Template rejected", "status": "UNLOCKED"}

@template_edit_router.post("/edit/discard")
async def discard_edit(template_id: str, user_id: str):
    master_table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)
    
    template = get_template_info(template_id, TEMPLATEMASTER_DYNAMOTABLE)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    if template.get("lock_status") != "LOCKED":
        raise HTTPException(status_code=400, detail="Template is not in locked state")
    
    if template.get("locked_by") != user_id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    now = datetime.now(timezone.utc).isoformat()
    
    master_table.update_item(
        Key={"template_id": template_id},
        UpdateExpression="SET lock_status = :status, locked_by = :null, locked_at = :null, draft_path = :null, updated_at = :updated",
        ExpressionAttributeValues={
            ":status": "UNLOCKED",
            ":null": None,
            ":updated": now
        }
    )
    
    edit_id = template.get("edit_id")
    log_audit(template_id, "DISCARD", user_id, "LOCKED", "DISCARDED", TEMPLATE_AUDIT_LOG_TABLE, edit_id)
    
    return {"message": "Edit discarded", "status": "UNLOCKED"}

@template_edit_router.get("/audit/{template_id}")
async def get_audit_log(template_id: str):
    """Get audit log for a template"""
    audit_table = dynamodb.Table(TEMPLATE_AUDIT_LOG_TABLE)
    response = audit_table.query(
        KeyConditionExpression="template_id = :tid",
        ExpressionAttributeValues={":tid": template_id},
        ScanIndexForward=False
    )
    return {"audit_log": response.get("Items", [])}

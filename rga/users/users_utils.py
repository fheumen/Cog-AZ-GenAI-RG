"""
Chat service for the application.

This module provides functions for managing chat interactions.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import re
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Attr
from fastapi import HTTPException
# from config import CHAT_DYNAMOTABLE
from pathlib import Path
from enum import Enum
from loguru import logger




USERS_DYNAMOTABLE = "aig-azcdi-us-ops-report-users-dev"

from pydantic import BaseModel
class User(BaseModel):
    id: str
    sessionId: str
    language: str
    platform: str

def get_list_user_profiles(user: User 
        ):
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
    table = dynamodb.Table(USERS_DYNAMOTABLE)
    # templates: List[TemplateMaster] = []

    if not user.sessionId:
        user.sessionId = str(uuid4())

    try:
    # response = table.scan()
    # items = response.get("Items", [])
    # if user.user_id:
        response = table.scan(FilterExpression=(Attr('pr_id').eq(user.id)))
                                 
        items = response.get("Items", [])
    
        while "LastEvaluatedKey" in response:
            response = table.scan(FilterExpression=(Attr('created_by').eq(user.id)
                                     ),
                                  ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
    
        if not items:
            logger.warning("User is not found in User table.")
            return {"status": "failed", "user_profiles":[], "comment": f"{user.id} is not in any user groups"}
        else:
            for item in items:
                return {"status": "success", "user_profiles":item["user_group"], "comment": f"{user.id} is part of following group: {item["user_group"]}"}
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

# user = User(id="khld677", sessionId="fffff", language="gggg", platform="en")
# print(get_list_user_profiles(user))

def get_list_user_profiles(user: User 
        ):
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
    table = dynamodb.Table(USERS_DYNAMOTABLE)
    # templates: List[TemplateMaster] = []

    if not user.sessionId:
        user.sessionId = str(uuid4())

    try:
    # response = table.scan()
    # items = response.get("Items", [])
    # if user.user_id:
        response = table.scan(FilterExpression=(Attr('pr_id').eq(user.id)))
                                 
        items = response.get("Items", [])
    
        while "LastEvaluatedKey" in response:
            response = table.scan(FilterExpression=(Attr('created_by').eq(user.id)
                                     ),
                                  ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
    
        if not items:
            logger.warning("User is not found in User table.")
            return {"status": "failed", "user_profiles":[], "comment": f"{user.id} is not in any user groups"}
        else:
            for item in items:
                return {"status": "success", "user_profiles":item["user_group"], "comment": f"{user.id} is part of following group: {item["user_group"]}"}
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


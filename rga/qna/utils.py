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
from fastapi import HTTPException
from config import CHAT_DYNAMOTABLE
from pathlib import Path
from enum import Enum


from data import (
    ChatInteraction, ChatMetadata, Citation
)


boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamodb.Table(CHAT_DYNAMOTABLE)

def session_history(session_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get session history for a session ID.
    
    Args:
        session_id: Session ID
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Session history data
    """
    try:
        response = table.query(
            IndexName="SessionId-Timestamp-index",
            KeyConditionExpression=Key('SessionId').eq(session_id),
            ScanIndexForward=True  # Explicitly request ascending order by sort key (Timestamp)
        )
        # Sort items by Timestamp in ascending order
        sorted_items = response.get('Items', [])
        # Group sorted items by SessionId
        grouped_conversations = {session_id: sorted_items}
        return grouped_conversations
    except Exception as e:
        print(f"Error in chat search: {e}")
        return {}
    
def extract_chat_history(data: Dict[str, List[Dict[str, Any]]]) -> List[tuple]:
    """
    Extract chat history from data.
    
    Args:
        data: Dictionary containing chat history data
        
    Returns:
        List[tuple]: List of (question, answer) tuples
    """
    chat_history = []
    
    # Check if the data is in the expected format
    if not isinstance(data, dict):
        print("Error: Input data must be a dictionary.")
        return chat_history

    # Iterate through the dictionary (assuming the keys are session IDs)
    for session_id, messages in data.items():
        if not isinstance(messages, list):
            print(f"Warning: Session {session_id} does not contain a list of messages. Skipping.")
            continue  # Skip to the next session

        for message in messages:
            if not isinstance(message, dict):
                print(f"Warning: Invalid message format in session {session_id}. Skipping.")
                continue  # Skip to next message

            try:
                question = message.get('UserMessage')  # Use get() to handle missing keys
                answer = message.get('BotResponse')

                if question and answer:  # Only add if both question and answer are present
                    chat_history.append((question, answer))
                else:
                    if not question:
                        print(f"Warning: Missing 'UserMessageSearch' in message from session {session_id}.")
                    if not answer:
                        print(f"Warning: Missing 'BotResponse' in message from session {session_id}.")
            except Exception as e:
                print(f"Error processing message in session {session_id}: {e}")
                continue  # Continue to the next message

    return chat_history
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
from fastapi import APIRouter
from collections import defaultdict
from typing import Optional, List, Dict, Sequence
import uuid
import os
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from datetime import datetime
from typing import Optional, List, Dict
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from config import *

from data import (
    QueryRequest, QnaAnswer, AnswerRequest, User, Query, RequestQuery, Citation, 
    QuickReply, Result, QueryResponse, FeedbackDisplayOptions, Feedback, ChatInteraction, ChatMetadata, ChatHistorySearchRequest, FeedbackRequest,
    IngestResult, IngestResponse, IsprTrackingSection, IsprTrackingCompletion, IsprTrackingWelcomePageAllProduct, IsprUpdatingPage, IsprFile, IsprSelectForEditionOutput,
    IsprTrackingProduct
)

###############with app
from data import ChatInteraction
###############

chat_history_router = APIRouter()
# Initialize FastAPI app
#router = APIRouter()
boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamodb.Table(CHAT_DYNAMOTABLE)

def store_interaction(interaction: ChatInteraction, CHAT_DYNAMOTABLE: str):
    try:
        # message_id = str(uuid.uuid4())  #Generate unique message ID
        table = dynamodb.Table(CHAT_DYNAMOTABLE)
        item = interaction.dict()
        table.put_item(Item=item)
        return {"message": "User-bot interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@chat_history_router.post("/search/")
def search_chat(request: ChatHistorySearchRequest):
    try:
        #Extract parameters from the request body
        #apiKey = request.apiKey
        userId = request.userId
        keyword = request.keyword
        start_date = request.start_date
        end_date = request.end_date
        sort_order = request.sort_order
        
        # Parse start and end timestamps if provided
        start_timestamp = datetime.fromisoformat(start_date).isoformat() if start_date else None
        end_timestamp = datetime.fromisoformat(end_date).isoformat() if end_date else None
        
        # Set up initial query parameters
        query_params = {}
        if userId:
            key_condition = Key('UserId').eq(userId)
            if start_timestamp and end_timestamp:
                key_condition &= Key('Timestamp').between(start_timestamp, end_timestamp)
            elif start_timestamp:
                key_condition &= Key('Timestamp').gte(start_timestamp)
            elif end_timestamp:
                key_condition &= Key('Timestamp').lte(end_timestamp)
            query_params['KeyConditionExpression'] = key_condition
        else:
            query_params['FilterExpression'] = Attr('Timestamp').between(start_timestamp, end_timestamp) if start_timestamp and end_timestamp else None
        
        # Add FilterExpression for keyword if provided
        if keyword:
            keyword_filter = (
                Attr('UserMessage').contains(keyword) | Attr('BotResponse').contains(keyword)
            )
            if 'FilterExpression' in query_params and query_params['FilterExpression']:
                query_params['FilterExpression'] &= keyword_filter
            else:
                query_params['FilterExpression'] = keyword_filter

        # Execute query or scan based on UserId presence
        if userId:
            response = table.query(**query_params, Limit=100)
        else:
            response = table.scan(**query_params, Limit=100)

        # Sort items based on Timestamp
        response['Items'].sort(
            key=lambda x: datetime.fromisoformat(x['Timestamp']),
            reverse=(sort_order.lower() == "desc")
        )
        grouped_conversations = defaultdict(lambda: defaultdict(list))
        seen_sessions = set()
        
        for item in response['Items']:
            date_str = datetime.fromisoformat(item['Timestamp']).date().isoformat()
            session_id = item['SessionId']
            if session_id in seen_sessions:
                continue
            user_message = view_chat_by_session(ChatHistorySearchRequest(session_id=session_id))
            first_key = list(user_message.keys())[0]
            first_record = user_message[first_key][0]
            first_user_message = first_record['UserMessage']
            if not first_user_message:
                first_user_message = f"Summary the document - {first_record.get('ChatMetadata', {}).get('FileName', None)}"
            grouped_conversations[date_str][session_id] = {
                "UserMessage": first_user_message,
                "Timestamp": item['Timestamp'],
                "SessionId": session_id,
                "UserId": item['UserId']
            }
            seen_sessions.add(session_id)

        grouped_conversations = {date: dict(sessions) for date, sessions in grouped_conversations.items()}
        return grouped_conversations    
   
    except Exception as e:
        # Handle all other exceptions (e.g., DB errors, missing attributes)
        print(f"Error in chat search: {e}")
        return generate_technical_error_message("", 0 , "", "")


@chat_history_router.post("/session/")
def view_chat_by_session(request: ChatHistorySearchRequest) -> Dict[str, List[dict]]:
    try:
        response = table.query(
            IndexName="SessionId-index",
            KeyConditionExpression=Key('SessionId').eq(request.session_id)
        )
        # Sort items by Timestamp in ascending order
        sorted_items = sorted(response['Items'], key=lambda x: x['Timestamp'])        
        # Group sorted items by SessionId
        grouped_conversations = {request.session_id: sorted_items}        
        return grouped_conversations
    except Exception as e:
        print(f"Error in chat search: {e}")
        return generate_technical_error_message("", 0 , "", "")
"""        
@chat_history_router.post("/download/")
async def download_chat(request: ChatHistorySearchRequest):
    if validate_api_key(request.apiKey):
        raise HTTPException(status_code=401, detail=f"Authetication failed") 
    try:
        # Retrieve chat history using the provided request data
        chat_history = search_chat(ChatHistorySearchRequest(
            apiKey=request.apiKey,
            userId=request.userId,
            keyword=request.keyword,
            start_date=request.start_date,
            end_date=request.end_date,
            sort_order=request.sort_order
        ))
        df = pd.DataFrame(chat_history)        
        # Save the Excel file locally
        file_path = f"/tmp/{request.userId}_chat_history.xlsx"
        df.to_excel(file_path, index=False)        
        # Define S3 bucket and key
        bucket_name = BUCKET_NAME #Replace with your bucket name
        s3_key = f"{request.userId}_chat_history.xlsx"
        # Upload the file to S3
        s3_client.upload_file(file_path, bucket_name, s3_key)        
        # Generate the S3 URL
        s3_url = f"s3://{bucket_name}/{s3_key}"
        # Generate a presigned URL
        presigned_url = generate_presigned_url(s3_url, page_number=1)        
        # Return the presigned URL in a JSON response
        return {"status":"success", "downloadUrl": presigned_url}    
    except Exception as e:
        print(f"Error in downloading chat: {e}")
        return generate_technical_error_message("", 0 , "", "")
"""

@chat_history_router.post("/feedback/")
def update_feedback(feedback: FeedbackRequest):

    try:
        # Validate that necessary feedback fields are provided
        if feedback.isFeedbackPositive is None:
            raise HTTPException(status_code=400, detail="IsFeedbackPositive must be provided.")        
        # Query to get the primary key using MessageId and SessionId
        response = table.query(
            IndexName="MessageId-index",
            KeyConditionExpression=Key('MessageId').eq(feedback.messageId)
        )
        # Check if item exists
        if response['Items']:
            # Extract primary key values from the queried item
            item = response['Items'][0]
            user_id = item['UserId']
            timestamp = item['Timestamp']       
            # Construct the update expression and expression attribute values
            update_expression = "SET IsFeedbackPositive = :IsFeedbackPositive"
            expression_attribute_values = {
                ":IsFeedbackPositive": feedback.isFeedbackPositive
            }
            # Include FeedbackComment if provided
            update_expression += ", FeedbackComment = :FeedbackComment"
            if feedback.isFeedbackPositive:
                expression_attribute_values[":FeedbackComment"] = "LikedByUser"
            else:
                expression_attribute_values[":FeedbackComment"] = feedback.feedbackComment
            # Prepare the primary key for the update
            key = {
                'UserId': user_id,
                'Timestamp': timestamp
            }
            # Update the item in DynamoDB
            update_response = table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )
            return {
                "status": "success"
            }
        else: 
            return {
                "status": "error"
            }                
    except Exception as e:
        print(f"Error updating feedback: {str(e)}")  # Print the error for debugging
        return generate_technical_error_message(feedback.messageId, 0 , "", feedback.sessionId)

@chat_history_router.post("/recents/")    
def get_latest_active_sessions(request: ChatHistorySearchRequest):

    try:
        # Query using UserId as partition key and filter by active sessions
        response = table.query(
            KeyConditionExpression=Key('UserId').eq(request.userId),  # Query by UserId
            FilterExpression=(
                Attr('SessionStatus').eq('Active') &
                Attr('ChatMetadata.FlowName').eq('QnA')
            ),
            ScanIndexForward=False,
            Limit = 100
        )
        # Deduplicate session IDs
        items = response['Items']
        unique_session_ids = []
        unique_records = {}
        for item in items:
            key = (item['UserId'], item['SessionId'])
            if key not in unique_records:
                unique_records[key] = item
                unique_session_ids.append(item['SessionId'])
            if len(unique_session_ids) == 3:
                break
        active_sessions = []
        kb_type = ""
        for session_id in unique_session_ids:                        
            # Query to get only the first message for each session, sorted by Timestamp ascending
            session_message_response = table.query(
                IndexName='SessionId-Timestamp-index',
                KeyConditionExpression=Key('SessionId').eq(session_id),
                ScanIndexForward=True,
                Limit=1
            )            
            if session_message_response['Items']:
                first_item = session_message_response['Items'][0]
                first_message = first_item.get('UserMessage', None)                
                # Extract KbType from ChatMetadata if available
                kb_type = first_item.get('ChatMetadata', {}).get('KbType', None)                
                # Append session info with first message and KbType to active_sessions
                active_sessions.append({
                    'SessionId': session_id,
                    'Message': first_message,
                    'KbType': kb_type
                })        
        return active_sessions    
    except Exception as e:
        print(f"Error retrieving latest active sessions: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving latest active sessions")
        return []

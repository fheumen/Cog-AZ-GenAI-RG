import os
import uuid
from uuid import uuid4
from typing import Optional, List, Dict, Sequence
from datetime import datetime
from collections import defaultdict
import json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd

from fastapi import FastAPI, HTTPException, APIRouter, Response, UploadFile, status, File, Form
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from pydantic import BaseModel

from langchain_aws import ChatBedrock
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory

from collections.abc import Iterable

from loguru import logger

from config import CHAT_DYNAMOTABLE, SESSION_STATUS_ACTIVE, BUCKET_NAME, MODEL_ID, QNA_FLOW_NAME, KNOWLEDGEBASE_NAME_QnA, TEMPLATEMASTER_DYNAMOTABLE

from qna.utils import(session_history, extract_chat_history)

session = boto3.session.Session()
client = session.client(service_name="secretsmanager", region_name="us-east-1")

# Get environment variables      
secret_name = os.getenv("secret_name")

# Retrieve secret value
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
secret = json.loads(get_secret_value_response["SecretString"])
KBNAME_MAX_SIZE = int(secret.get("KBNAME_MAX_SIZE", 0))
QNA_MAX_TOKENS_VALUE = int(secret.get("QNA_MAX_TOKENS_VALUE", 0))
QNA_TEMPRATURE_VALUE = float(secret.get("QNA_TEMPRATURE_VALUE"))
QNA_TOP_P_VALUE = float(secret.get("QNA_TOP_P_VALUE"))

from utils import get_file_type, extract_pdf_contents, extract_text_from_word, generate_prompt, generate_technical_error_message
from data import FeedbackDisplayOptions, Feedback, Result, QueryResponse, ChatMetadata, ChatInteraction, QnAInputs, TemplateClass

from knowbase_utils import ( and_all_filter,
    retrieve_and_generate_report
)


# Initialize Router
qna_router = APIRouter()
boto_config = Config(retries={'max_attempts': 3}, max_pool_connections=50)
s3_client = boto3.client("s3", config=boto_config)
s3 = boto3.client("s3")

store = {}
history_store = {}

def get_user_memory(session_id: str):
    if session_id not in history_store:
        history_store[session_id] = InMemoryChatMessageHistory()
    return history_store[session_id]

def store_interaction(interaction: ChatInteraction, CHAT_DYNAMOTABLE: str):
    try:
        # message_id = str(uuid.uuid4())  #Generate unique message ID
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table(CHAT_DYNAMOTABLE)
        item = interaction.dict()
        table.put_item(Item=item)
        return {"message": "User-bot interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

        
############### online qna
@qna_router.post("/ask")
async def retrieve_and_generate(files: Optional[List[UploadFile]] = File(None), 
            apiKey: Optional[str] = Form(None),
            userId: Optional[str] = Form(None),
            sessionId: Optional[str] = Form(None), 
            language: Optional[str] = Form(None),
            platform: Optional[str] = Form(None),
            queryText: Optional[str] = Form(None), 
            transactionCount: Optional[int] = Form(None)    
        ):
    try:          
        content = ""
        session_id = sessionId or str(uuid.uuid4())
        file_type = ""
        msg_id = uuid.uuid4()
        answer = ""
        file_name = ""
        folder_path =""
        #Check if a file is provided
        print("before if")
        print(f"files{files}")
        print(isinstance(files, Iterable))
        logger.info("retrieve_and_generate ---- START")

        if files and isinstance(files, Iterable):
            print("if")
            for file in files:
                if file:
                    #Read the uploaded file as bytes (async)
                    try:
                        file_contents = await file.read()  # Corrected to async
                        print(f"contents{file_contents}")
                        print(f"file.file_name{file.filename}")
                        file_type = get_file_type(file.filename)
                        print(f"file_type{file_type}")
                    except Exception as e:
                        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
                    # Upload file to S3
                    try:
                        folder_path = "chats/"+f"{userId}/{session_id}"                
                        s3.put_object(Bucket=BUCKET_NAME, Key=f"{folder_path}/")
                        print(folder_path)
                        file_name = file.filename
                        print(f"file.filename{file.filename}")
                        print(f"file.content_type{file.content_type}")
                        s3.put_object(
                            Bucket=BUCKET_NAME,
                            Key=f"{folder_path}/{file_name}",
                            Body=file_contents,
                            #ContentType=file.content_type,
                        )
                    except (BotoCoreError, ClientError) as e:
                        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")
                    # Extract content from the PDF or Word file if applicable
                if file_contents:
                    try:
                        if file_type == ".pdf":
                            print("inside .pdf")
                            content += extract_pdf_contents(file_contents)
                            print(content)
                        elif file_type in [".doc", ".docx"]:
                            content += extract_text_from_word(file_contents)
                    except ValueError as e:
                        raise HTTPException(status_code=400, detail=f"Failed to extract content from the file: {str(e)}")
        
        logger.info("retrieve_and_generate ---- 1 ----")
        #Check if queryText or content is provided        
        if not queryText and not content:
            raise HTTPException(status_code=400, detail="QueryText or content from the file is required")            
        
        logger.info("retrieve_and_generate ---- 2 ----")
        prompt = generate_prompt(content, queryText)  # Generate the prompt based on queryText and content
        llm = ChatBedrock(model_id=MODEL_ID, 
                           model_kwargs={
                                "temperature": QNA_TEMPRATURE_VALUE, 
                                "top_p": QNA_TOP_P_VALUE,
                                "max_tokens": QNA_MAX_TOKENS_VALUE
                            }
                          )

        logger.info("retrieve_and_generate ---- 3 ----")
        chain = RunnableWithMessageHistory(llm, get_user_memory)

        #Run the model with the prompt
        try:
            summary = chain.invoke(
                prompt,                
                config={"configurable": {"session_id": session_id}},
            )

            logger.info("retrieve_and_generate ---- 4 ----")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error invoking model: {str(e)}")
        answer = summary.content      
        # Construct the feedback and result objects
        feedbackoptions = FeedbackDisplayOptions(thumbsUp="Y", thumbsDown="Y", feedbackText="Y")
        feedback = Feedback(feedbackDisplayOptions=feedbackoptions)
        result = Result(messageId=str(msg_id), answer=answer, transactionCount=transactionCount, feedback=feedback, citations=[])
        queryResponse = QueryResponse(status="success", sessionId=session_id, userQuery=queryText, result=result)
        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.isoformat()
        file_location = BUCKET_NAME+ folder_path + file_name
        logger.info("retrieve_and_generate ---- 5 ----")
        
        if userId:
            logger.info("retrieve_and_generate ---- 6 ----")
            chat_metadata = ChatMetadata(FileName=file_name,FileLocation=file_location,FlowName=QNA_FLOW_NAME,Department="")
            chat_interaction = ChatInteraction(
                UserId=userId,
                SessionId=session_id,
                UserMessage=queryText,
                BotResponse=answer,
                IsFeedbackPositive=None,
                FeedbackComment="",
                Timestamp=formatted_timestamp,
                SessionStatus=SESSION_STATUS_ACTIVE,
                MessageId=str(msg_id),
                ChatMetadata=chat_metadata
            )
            store_interaction(chat_interaction, CHAT_DYNAMOTABLE)
        return queryResponse    
    except HTTPException as http_exc:
        print(str(http_exc)) 
        logger.info(f"retrieve_and_generate ---- 7 ----{str(http_exc)}")
        return generate_technical_error_message(str(msg_id), transactionCount , queryText, sessionId)
    except Exception as e:
        print(str(e))
        logger.info(f"retrieve_and_generate ---- 8 ----{str(e)}")
        return generate_technical_error_message(str(msg_id), transactionCount , queryText, sessionId)
    
    
################ offline (based on KB) qna
@qna_router.post("/ask_kb")
async def retrieve_and_generate(  qnain: QnAInputs
            # apiKey: Optional[str] = Form(None),
            # userId: Optional[str] = Form(None),
            # sessionId: Optional[str] = Form(None), 
            # template_name: Optional[str] = Form(None),
            # product_name: Optional[str] = Form(None), 
            # created_by: Optional[str] = Form(None), 
            # created_at: Optional[str] = Form(None),
            # reporting_period: Optional[str] = Form(None),
            # section_name: Optional[str] = Form(None),           
            # language: Optional[str] = Form(None),
            # platform: Optional[str] = Form(None),
            # queryText: Optional[str] = Form(None), 
            # transactionCount: Optional[int] = Form(None)    
        ):
    try:          
        session_id = qnain.sessionId or str(uuid.uuid4())
        msg_id = uuid.uuid4()
        answer = ""
        content = ""
        sources = []
      
        logger.info("retrieve_and_generate ---- 1 ----")
        #Check if queryText or content is provided        
        if not qnain.queryText:
            raise HTTPException(status_code=400, detail="QueryText is required")            
        
        logger.info("retrieve_and_generate ---- 2 ----")
#         prompt = generate_prompt(content, queryText)  # Generate the prompt based on queryText and content
#         llm = ChatBedrock(model_id=MODEL_ID)

#         logger.info("retrieve_and_generate ---- 3 ----")
#         chain = RunnableWithMessageHistory(llm, get_user_memory)

        #Run the model with the prompt
        try:
            params = {"template_name":qnain.template_name, "product_name":  [s.lower() for s in qnain.product_name] if qnain.product_name else None, "created_by": qnain.created_by, "created_start_date": qnain.created_start_date, "created_end_date": qnain.created_end_date, "reporting_period": [s.lower() for s in qnain.reporting_period] if qnain.reporting_period else None, "section_name": [s.lower() for s in qnain.section_name] if qnain.section_name else None} 
            
            logger.info(f"retrieve_and_generate_params: {params}")
            
            previous_context = ""
            if session_id:
                print("Inside session")
                history = session_history(session_id)
                chat_history = extract_chat_history(history)

                # Build previous conversation context
                for question, answer in chat_history:
                    previous_context += f"User: {question}\nAssistant: {answer}\n"

            
            answer, sources = retrieve_and_generate_report(qnain.queryText, KNOWLEDGEBASE_NAME_QnA, MODEL_ID, KBNAME_MAX_SIZE, params, QNA_MAX_TOKENS_VALUE, QNA_TEMPRATURE_VALUE, QNA_TOP_P_VALUE, previous_context)
            logger.info(f"retrieve_and_generate_source: {sources}")
            logger.info("retrieve_and_generate---- 4 ----")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error invoking model: {str(e)}")
        # answer = summary.content      
        # Construct the feedback and result objects
        feedbackoptions = FeedbackDisplayOptions(thumbsUp="Y", thumbsDown="Y", feedbackText="Y")
        feedback = Feedback(feedbackDisplayOptions=feedbackoptions)
        result = Result(messageId=str(msg_id), answer=answer, transactionCount=qnain.transactionCount, feedback=feedback, citations=sources)
        queryResponse = QueryResponse(status="success", sessionId=session_id, userQuery=qnain.queryText, result=result)
        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.isoformat()
        folder_path = "outputs/"
        # file_location = folder_path + file_name
        logger.info("retrieve_and_generate ---- 5 ----")
        
        if qnain.userId:
            logger.info("retrieve_and_generate ---- 6 ----")
            chat_metadata = ChatMetadata(FileName="",FileLocation="", FlowName=QNA_FLOW_NAME,Department="")
            chat_interaction = ChatInteraction(
                UserId=qnain.userId,
                SessionId=session_id,
                UserMessage=qnain.queryText,
                BotResponse=answer,
                IsFeedbackPositive=None,
                FeedbackComment="",
                Timestamp=formatted_timestamp,
                SessionStatus=SESSION_STATUS_ACTIVE,
                MessageId=str(msg_id),
                ChatMetadata=chat_metadata
            )
            store_interaction(chat_interaction, CHAT_DYNAMOTABLE)
        return queryResponse    
    except HTTPException as http_exc:
        print(str(http_exc)) 
        logger.info(f"retrieve_and_generate ---- 7 ----{str(http_exc)}")
        return generate_technical_error_message(str(msg_id), qnain.transactionCount , qnain.queryText, qnain.sessionId)
    except Exception as e:
        print(str(e))
        logger.info(f"retrieve_and_generate ---- 8 ----{str(e)}")
        return generate_technical_error_message(str(msg_id), qnain.transactionCount , qnain.queryText, qnain.sessionId)
    

    ################ offline (based on KB) qna
@qna_router.post("/ask_product_list")
async def get_product_list( 
            template: TemplateClass
        ):
    
      # Create a DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    
    # Get the table from the DynamoDB resource
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)

    try:
        # Scan the table for items matching the provided template_fullname
        response = table.scan(
            FilterExpression=Attr('template_fullname').eq(template.template_fullname)
        )

        # Get the first (and only) item from the response
        res = response['Items'][0]

        # Check if the template is an ISPR template
    
            # Return template information for non-ISPR templates
        return { "status": "sucsess", "list_product_names": res["product_names"]}

    except Exception as e:
        # Log the error and raise an HTTP exception
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again later.")

import boto3
import pickle
from typing import Sequence
from langchain.schema import BaseMessage
from langchain.schema import BaseChatMessageHistory
from fastapi import HTTPException
from config import *


s3 = boto3.client('s3')

class ChatHistory(BaseChatMessageHistory):
    
    def __init__(self, session_id:str):
        self.session_id = session_id
        self.messages = []
        
    def add_messages(self, messages: Sequence[BaseMessage]):
        self.messages.extend(messages)
        try:
            updated_pickle_data = pickle.dumps(self)
            # Upload the updated pickle file back to S3
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key= f"cache/{self.session_id}.pkl",
                Body=updated_pickle_data
            )
        except Exception as e:
            print(str(e))
            raise HTTPException(status_code=500, detail=f"Error while {self.session_id} storing the memory pkl file qna answer: {str(e)}")  
            
    def clear(self):        
        self.messages.clear()
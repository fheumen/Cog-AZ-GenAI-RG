import io
import os
import boto3
from fastapi import APIRouter, HTTPException
import json

REGION_NAME=None
EMBEDDING_MODELID=None
MODEL_ID=None
model_max_tokens = None
BUCKET_NAME = None
INTPUTS_PATH = None
UPLOAD_TMP_PATH = None
OUTPUTS_PATH = None
MAPPING_FILE_PATH = None
ISPR_TEMP_PATH_NAME = None
SHEET_NAME_MAPPING = None
TEST_FILE_PATH = None
TEST_RESULT_FILE_PATH = None
QNA_FLOW_NAME=None
ISPR_FLOW_NAME=None
SESSION_STATUS_ACTIVE = None
QNA_PROMPT = None
KNOWLEDGEBASE_NAME = None
KNOWLEDGEBASE_NAME_QnA = None
API_KEY = None
SECRET_KEY = None
TOKEN_EXPIRE_MINUTES = None
TOKEN_GRACE_PERIOD_MINUTES = None
ALGORITHM = None
REPORTS_DYNAMOTABLE = None
REPORTSQUEUE_DYNAMOTABLE = None 
TEMPLATEMASTER_DYNAMOTABLE = None
CHAT_DYNAMOTABLE = None
USERS_DYNAMOTABLE = None
TEMPLATE_APPROVAL_MAPPING_DYNAMOTABLE = None
TEMPLATE_AUDIT_LOG_DYNAMOTABLE = None
EMAIL_SENDER = None
KBNAME_MAX_SIZE = None
QNA_MAX_TOKENS_VALUE = None
QNA_TEMPRATURE_VALUE = None
QNA_TOP_P_VALUE = None
GUARDRAIL_ID = None
GUARDRAIL_VERSION_ID = None

TENANT_ID = None
CLIENT_ID = None
PRIVATE_KEY_FILE = None 
CERT_THUMBPRINT = None
SHAREPOINT_MAIN_URL = None
SP_SITE_NAME = None
SP_DOCUMENT_LIBRARY = None

TENANT_ID_POC = None
CLIENT_ID_POC = None
PRIVATE_KEY_FILE_POC = None 
CERT_THUMBPRINT_POC = None
SHAREPOINT_MAIN_URL_POC = None
SP_SITE_NAME_POC = None
SP_DOCUMENT_LIBRARY_POC = None

TEMPLATE_APPROVAL_MAPPING_DYNAMOTABLE = None
TEMPLATE_AUDIT_LOG_DYNAMOTABLE = None

config_router = APIRouter()

@config_router.get("/config")
async def load_config():
    try:
        load_values()
        return {"message": "Config loaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in loading config data: {str(e)}")

def load_values():
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name="us-east-1")
        secret_name = os.getenv("secret_name")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])

        global REGION_NAME, EMBEDDING_MODELID, MODEL_ID
        global CHAT_DYNAMOTABLE, REPORTS_DYNAMOTABLE, REPORTSQUEUE_DYNAMOTABLE, TEMPLATEMASTER_DYNAMOTABLE, USERS_DYNAMOTABLE
        global TEMPLATE_APPROVAL_MAPPING_DYNAMOTABLE, TEMPLATE_AUDIT_LOG_DYNAMOTABLE
        global TEST_FILE_PATH, TEST_RESULT_FILE_PATH, QNA_FLOW_NAME, ISPR_FLOW_NAME, SESSION_STATUS_ACTIVE, QNA_PROMPT
        global BUCKET_NAME, KNOWLEDGEBASE_NAME, INTPUTS_PATH, UPLOAD_TMP_PATH, OUTPUTS_PATH, MAPPING_FILE_PATH, ISPR_TEMP_PATH_NAME, SHEET_NAME_MAPPING
        global model_max_tokens, API_KEY, SECRET_KEY,TOKEN_EXPIRE_MINUTES,TOKEN_GRACE_PERIOD_MINUTES,ALGORITHM, EMAIL_SENDER, KNOWLEDGEBASE_NAME_QnA
        global QNA_MAX_TOKENS_VALUE, QNA_TEMPRATURE_VALUE, QNA_TOP_P_VALUE, GUARDRAIL_ID, GUARDRAIL_VERSION_ID, TENANT_ID, CLIENT_ID, PRIVATE_KEY_FILE, CERT_THUMBPRINT, SHAREPOINT_MAIN_URL
        global SP_SITE_NAME, SP_DOCUMENT_LIBRARY
        global TENANT_ID_POC, CLIENT_ID_POC, PRIVATE_KEY_FILE_POC, CERT_THUMBPRINT_POC, SHAREPOINT_MAIN_URL_POC, SP_SITE_NAME_POC, SP_DOCUMENT_LIBRARY_POC
        global TEMPLATE_APPROVAL_MAPPING_TABLE, TEMPLATE_AUDIT_LOG_TABLE

        EMBEDDING_MODELID=secret.get("EMBEDDING_MODELID")
        MODEL_ID=secret.get("MODEL_ID")
        REPORTS_DYNAMOTABLE = secret.get("REPORTS_DYNAMOTABLE")
        REPORTSQUEUE_DYNAMOTABLE = secret.get("REPORTSQUEUE_DYNAMOTABLE")
        CHAT_DYNAMOTABLE=secret.get("CHAT_DYNAMOTABLE")
        TEMPLATEMASTER_DYNAMOTABLE=secret.get("TEMPLATEMASTER_DYNAMOTABLE")
        USERS_DYNAMOTABLE=secret.get("USERS_DYNAMOTABLE")
        TEMPLATE_APPROVAL_MAPPING_DYNAMOTABLE=secret.get("TEMPLATE_APPROVAL_MAPPING_DYNAMOTABLE")
        TEMPLATE_AUDIT_LOG_DYNAMOTABLE=secret.get("TEMPLATE_AUDIT_LOG_DYNAMOTABLE")
        model_max_tokens = int(secret.get("model_max_tokens"))
        BUCKET_NAME = secret.get("BUCKET_NAME")
        INTPUTS_PATH = secret.get("INTPUTS_PATH")
        UPLOAD_TMP_PATH = secret.get("UPLOAD_TMP_PATH")
        OUTPUTS_PATH = secret.get("OUTPUTS_PATH")
        MAPPING_FILE_PATH = secret.get("MAPPING_FILE_PATH")
        ISPR_TEMP_PATH_NAME = secret.get("ISPR_TEMP_PATH_NAME")
        SHEET_NAME_MAPPING = secret.get("SHEET_NAME_MAPPING")
        TEST_FILE_PATH = secret.get("TEST_FILE_PATH")
        TEST_RESULT_FILE_PATH = secret.get("TEST_RESULT_FILE_PATH")
        QNA_FLOW_NAME=secret.get("QNA_FLOW_NAME")
        ISPR_FLOW_NAME=secret.get("ISPR_FLOW_NAME")
        SESSION_STATUS_ACTIVE = secret.get("SESSION_STATUS_ACTIVE")
        QNA_PROMPT = secret.get("QNA_PROMPT")
        KNOWLEDGEBASE_NAME = secret.get("KNOWLEDGEBASE_NAME")
        API_KEY = secret.get("API_KEY")
        SECRET_KEY = secret.get("SECRET_KEY")
        TOKEN_EXPIRE_MINUTES = secret.get("TOKEN_EXPIRE_MINUTES")
        TOKEN_GRACE_PERIOD_MINUTES = secret.get("TOKEN_GRACE_PERIOD_MINUTES")
        ALGORITHM = secret.get("ALGORITHM")
        EMAIL_SENDER = secret.get("EMAIL_SENDER")
        KBNAME_MAX_SIZE = secret.get("KBNAME_MAX_SIZE")
        KNOWLEDGEBASE_NAME_QnA = secret.get("KNOWLEDGEBASE_NAME_QnA")
        QNA_MAX_TOKENS_VALUE = secret.get("QNA_MAX_TOKENS_VALUE")
        QNA_TEMPRATURE_VALUE = secret.get("QNA_TEMPRATURE_VALUE")
        QNA_TOP_P_VALUE = secret.get("QNA_TOP_P_VALUE")
        GUARDRAIL_ID = secret.get("GUARDRAIL_ID")
        GUARDRAIL_VERSION_ID = secret.get("GUARDRAIL_VERSION_ID")
        TENANT_ID = secret.get("TENANT_ID")
        CLIENT_ID = secret.get("CLIENT_ID")
        PRIVATE_KEY_FILE = secret.get("PRIVATE_KEY_FILE")
        CERT_THUMBPRINT  = secret.get("CERT_THUMBPRINT")
        SHAREPOINT_MAIN_URL = secret.get("SHAREPOINT_MAIN_URL")
        SP_SITE_NAME =  secret.get("SP_SITE_NAME")
        SP_DOCUMENT_LIBRARY = secret.get("SP_DOCUMENT_LIBRARY")

        TENANT_ID_POC = secret.get("TENANT_ID_POC")
        CLIENT_ID_POC = secret.get("CLIENT_ID_POC")
        PRIVATE_KEY_FILE_POC = secret.get("PRIVATE_KEY_FILE_POC")
        CERT_THUMBPRINT_POC  = secret.get("CERT_THUMBPRINT_POC")
        SHAREPOINT_MAIN_URL_POC = secret.get("SHAREPOINT_MAIN_URL_POC")
        SP_SITE_NAME_POC =  secret.get("SP_SITE_NAME_POC")
        SP_DOCUMENT_LIBRARY_POC = secret.get("SP_DOCUMENT_LIBRARY_POC")

        TEMPLATE_APPROVAL_MAPPING_TABLE = secret.get("TEMPLATE_APPROVAL_MAPPING_TABLE")
        TEMPLATE_AUDIT_LOG_TABLE = secret.get("TEMPLATE_AUDIT_LOG_TABLE")


        print("Configuration values loaded successfully")

    except Exception as e:
        raise Exception(f"Error in loading required data: {str(e)}")

load_values()

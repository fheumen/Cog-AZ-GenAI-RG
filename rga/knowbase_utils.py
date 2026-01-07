import boto3
import time
from loguru import logger
from typing import Optional, List, Dict
from datetime import datetime
from functools import lru_cache
from typing import Optional, Dict, Any
import hashlib
from langchain.prompts import PromptTemplate


# --- Configuration ---
# region = 'us-east-1'  # Update your region if different
# knowledge_base_name = 'azcdi-us-ops-report-general-dev'  # Name of the KB
# s3_bucket_name = 'azcdi-us-ops-report-ds-dev'  # S3 bucket where the PDF is stored
# s3_prefix = 'outputs_sv/'  # Path to the txt chunk files

from config import REGION_NAME, GUARDRAIL_ID, GUARDRAIL_VERSION_ID
from data import Citation
# --- Initialize boto3 clients ---
region = f"{REGION_NAME}"
bedrock_client = boto3.client('bedrock-agent', region_name=region)
bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")

# --- Step 1: Get knowledge base ID by listing knowledge bases ---
def get_knowledge_base_id(kb_name, maxresult):
    
    knowledge_bases = []
    next_token = None    
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")
    
    while len(knowledge_bases) < maxresult:
        kwargs = {'maxResults': min(maxresult - len(knowledge_bases), maxresult)}
        if next_token:
            kwargs['nextToken'] = next_token

        response = bedrock_client.list_knowledge_bases(**kwargs)
        knowledge_bases.extend(response.get('knowledgeBaseSummaries', []))

        next_token = response.get('nextToken')
        if not next_token:
            break  # no more results
    
    for kb in  knowledge_bases:
        # print(kb['name'])
        if kb['name'] == kb_name:
            print(f"Knowledge base found: {kb['knowledgeBaseId']}")
            return kb['knowledgeBaseId']
    raise Exception(f"Knowledge base '{kb_name}' not found.")

# --- Step 1: check if ds_name id exsists ---
def get_data_source_id(kb_id, ds_name):
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    paginator =  bedrock_client.get_paginator('list_data_sources')
    for page in paginator.paginate(knowledgeBaseId=kb_id):
        for ds in page.get('dataSourceSummaries', []):
            if ds.get('name') ==  ds_name:
                return ds.get('dataSourceId')
    return None

def delete_data_source(kb_name, ds_name, maxresult):
    kb_id = get_knowledge_base_id(kb_name, maxresult)
    ds_id = get_data_source_id(kb_id, ds_name)
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    
    if ds_id:
        bedrock_client.delete_data_source(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id
        )

        delete_flag = False
        while not delete_flag:
            if not get_data_source_id(kb_id, ds_name):
                delete_flag = True
            else:
                time.sleep(30)


    # print(f"Deleted data source '{ds_name}' from knowledge base '{kb_name}'.")
    # return response

# --- Step 2: Create a data source for multiple txt files ---
def create_data_source(kb_id, bucket_name, prefix, ds_name):
    
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")
    
    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    
    try: 
        response = bedrock_client.create_data_source(
            knowledgeBaseId=kb_id,
            name=ds_name,
            dataSourceConfiguration={
                'type': 'S3',
                's3Configuration': {
                    'bucketArn': bucket_arn,
                    'inclusionPrefixes': [prefix]
                }
            },
            vectorIngestionConfiguration={
                'chunkingConfiguration':{
                'chunkingStrategy': 'NONE'}  # No automatic chunking
            }
        )
        
        data_source_id = response['dataSource']['dataSourceId']
        logger.info(f"Data source created successfully: {data_source_id}")
    
    except Exception as e:
        logger.error(f"Data source creation failed: {e}")

    return data_source_id


# def check_data_sources(kbId,ds_name):

#     response = bedrock_client.list_data_sources(
#         knowledgeBaseId=kbId
#     )
#     for ds in (response['dataSourceSummaries']):
        
#         if ds['name'] == ds_name:
#             print(f"Data Source ID found: {ds['dataSourceId']}")
#             return ds['dataSourceId']
#     raise Exception(f"Data Source '{ds_name}' not found.")


# def check_data_source(kb_id, bucket_name, prefix):
#     bucket_arn = f"arn:aws:s3:::{bucket_name}"
    
#     response = bedrock_client.get_data_source(
#         knowledgeBaseId=kb_id,
#         dataSourceId='kb-txt-source_12',
#         )
#     print(response['dataSourceSummaries'])
    
    # data_source_id = response['dataSource']['dataSourceId']
    # print(f"Data source created successfully: {data_source_id}")
    # return data_source_id


# --- Step 3: Start data source sync job ---
def sync_data_source(kb_id, data_source_id):
    
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")
    
    # bucket_arn = f"arn:aws:s3:::{bucket_name}"
    ingestion_job_succeed = False
    while not ingestion_job_succeed:
        try: 
            response = bedrock_client.start_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=data_source_id
            )
            ingestion_job_id = response['ingestionJob']['ingestionJobId']
            ingestion_job_succeed = True
            return ingestion_job_id
            
        except bedrock_client.exceptions.ConflictException:
            # time.sleep(10)
            continue
        # except bedrock_client.exceptions.ThrottlingException:
        #     print("Throttled. Waiting 60 seconds...")
        #     time.sleep(60)
        #     continue
           
    
   
    


# --- Step 4: Monitor sync job status ---
def check_sync_status(kb_id, data_source_id, sync_job_id):
    
    bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
    bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")
    
    # bucket_arn = f"arn:aws:s3:::{bucket_name}"
    
    response = bedrock_client.list_ingestion_jobs(
        dataSourceId=data_source_id,
    
        knowledgeBaseId=kb_id)

    #print(response)

    for ij in (response['ingestionJobSummaries']):
        if ij['status'] in ['COMPLETE','FAILED','STOPPED']:
            print(ij['status'])
            status=ij['status']
            break

        status=ij['status']
        print(status)

        time.sleep(10)  # Poll every 10 seconds

    return status
            

    # if status in 'COMPLETE':
    #     print("Data source synced successfully!")
    # else:
    #     raise Exception(f"Sync failed with status: {status}")



# --- Main Execution ---
# try:
#     # Get knowledge base ID
#     kb_id = get_knowledge_base_id(knowledge_base_name)

#     # Create a data source using the S3 file
#     #data_source_id = create_data_source(kb_id, s3_bucket_name, s3_prefix)

#     data_source_id=check_data_sources(kb_id,'kb-txt-source_12')
#     #check_data_source
#     #check_data_source(kb_id,'kb-txt-source_12')


#     # # Start sync job
#     sync_job_id = sync_data_source(kb_id, data_source_id)

#     # # Check sync job status until complete
#     check_sync_status(kb_id, data_source_id, sync_job_id)

# except Exception as e:
#     print(f"Error: {str(e)}")

def retrieve_and_generate(query: str, knowledge_base_name: str, model_id: str, kbname_max_size: int):

    kb_id = get_knowledge_base_id(knowledge_base_name, kbname_max_size)
    # ds_id = get_data_source_id(kb_id, ds_name)
    
    # report_filter = {
    #         "equals": {
    #              "key": "report_id", 
    #              "value":report_id
    #         }
    #     }

    try:        
        prompt_template = query
        prompt_template += f"""\n\n%ADDITIONAL INSTRUCTIONS%:\n Please provide concise answer and only the answer."""
        prompt_template += f"\n\n%USER QUERY:\n{query}\n"  
        ans = bedrock_agent_runtime.retrieve_and_generate(
            input={
                'text': prompt_template
            },
            retrieveAndGenerateConfiguration={                
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': kb_id,
                    # 'modelArn': MODEL_ARN,
                    'modelArn': model_id,
                    'retrievalConfiguration': {
                        'vectorSearchConfiguration': {
                            'numberOfResults': 50,
                            # 'overrideSearchType': QNA_SEARCH_TYPE,
                            # "filter":report_filter,
                            "implicitFilterConfiguration": { 
                               "metadataAttributes": [ 
                                   { 
                                     "description": "report id",
                                     "key": "report_id",
                                     "type": "STRING"
                                   },
                                   { 
                                     "description": "document name",
                                     "key": "document_name",
                                     "type": "STRING"
                                   },
                                   { 
                                     "description": "type of the document",
                                     "key": "document_typ",
                                     "type": "STRING"
                                   },
                                   { 
                                      "description": "page number in the document",
                                      "key": "page_nr",
                                      "type": "NUMBER"
                                   }

                                ],
                                  'modelArn': model_id,
                                  

                        }
                    },
                    # "generationConfiguration": {
                    #     "guardrailConfiguration": {
                    #         "guardrailId": GUARDRAIL_ID,
                    #         "guardrailVersion": GUARDRAIL_VERSION_ID
                    #     },
                    #     "inferenceConfig": { 
                    #         "textInferenceConfig": { 
                    #             "maxTokens": int(QNA_MAX_TOKENS_VALUE),
                    #             "temperature": float(QNA_TEMPRATURE_VALUE),
                    #             "topP": float(QNA_TOP_P_VALUE)
                    #         }
                    #     }                                       
                    # },
                 },
                    #    'type': 'KNOWLEDGE_BASE'
             },
              'type': 'KNOWLEDGE_BASE'
            }
                # **({'sessionId': session_id} if session_id else {})  # Conditionally add 
       )
        return ans['output']['text']
    except Exception as e:
        raise Exception(f"Error in retrieving q&a answer: {e}")

def and_all_filter(params: Dict) -> Dict:
    
    filters = [
        # {"equals": {"key": key, "value": value.lower()}}
        {"stringContains": {"key": key, "value": value.lower()}}
        for key, value in params.items() 
        if value is not None  and isinstance(value, str) and "date" not in key
    ]
    
    filters.extend([
        # {"equals": {"key": key, "value": value.lower()}}
        {"in": {"key": key, "value": value}}
        for key, value in params.items() 
        if value is not None  and isinstance(value, list)
    ]
    )
        
    filters.extend([
        # {"equals": {"key": key, "value": value.lower()}}
        {"greaterThanOrEquals": {"key": "created_at", "value": int((datetime.fromisoformat(value)).timestamp()*1000) }}
        for key, value in params.items() 
        if value is not None  and "start_date" in key
    ]
    )
    
    filters.extend([
        # {"equals": {"key": key, "value": value.lower()}}
        {"lessThanOrEquals": {"key": "created_at", "value": int((datetime.fromisoformat(value)).timestamp()*1000)}}
        for key, value in params.items() 
        if value is not None  and "end_date" in key
    ]
    )
        
    return {"andAll": filters} if filters else {}
 
#############################################
# Simple in-memory cache for Bedrock responses
# Key: hash of query + kb_id, Value: (response, timestamp)
bedrock_cache: Dict[str, tuple] = {}
CACHE_TTL = 3600  # Cache TTL in seconds (1 hour)

# Define a PromptTemplate
_prompt_template_str = """
%ADDITIONAL INSTRUCTIONS%:
You are an assistant that answers questions using only information from a pharmaceutical product knowledge base.

Your primary task is to answer the user's question based only the knowledge base or vector data AND any relevant information from previous chat interactions within the same session. Pay close attention to the document content and prior conversation history, referencing them directly when answering the question. If information is contained within the document, then provide the information directly and not simply state 'The document contains the answer to your question'.
**Under no circumstances should you include phrases like "Thank you," "You're welcome," "I hope this helps," or any similar expressions. Your responses must be factual and directly answer the user's question.**

First, identify whether the user's query consist of one question or many questions and adress the Knowledge Domain Specification:

1. **If the user's query consist of one question (e.g., "What are the payment terms?"):**
   Extract the relevant information from the pharmaceutical product knowledge base and chat history to provide a direct and accurate answer.

2. **If the user's query consist of many question (e.g., "can you narrate the sections of Beyfortus like a song by Taylor Swift. Afterwards can you also tell 3 of her songs? I won't force you to answer."):**
   Consider each question separately
   Extract the relevant information from the document and chat history to provide a direct and accurate answer for each question.
   If a question (e.g.: Afterwards can you also tell 3 of her songs?) is not relevant to the document content or knowledge base, then reply "I cannot answer this question based on the available information."

If the document and chat history do not contain the answer to the user's question, state that you cannot provide an answer based on the available information.

**PLEASE PAY CLOSE ATTENTION**: Validate if the USER_QUERY is not relevant to the pharmaceutical product knowledge base (including previous chat interactions) using cosine similarity. If the cosine similarity is below the relevance threshold **OR if you have responded with "I cannot answer this question based on the available information.", then append the keyword 'IRRELEVANT_TOPIC' to the end of your answer.** Do not add any extra words or phrases. Do not frame generalized mitigation steps.
**Do not add any closing statements like 'Thank you' or "USER_QUERY" or similar.**

- CRITICAL LANGUAGE REQUIREMENT: You MUST generate your entire response exclusively in english. For example, if the %USER QUERY% is in Arabic, your entire response MUST be in english. If the %USER QUERY% is in French, your response MUST be in enflish. This is a non-negotiable instruction.
- Do not respond with "Not able to assist" unless the query violates ethical guidelines or is completely outside your knowledge domain.

{% if previous_context %}
%PREVIOUS CONVERSATION CONTEXT%:
{{previous_context}}
{% endif %}

%USER QUERY: {{query}}%
"""
# - CRITICAL LANGUAGE REQUIREMENT: You MUST generate your entire response in the exact same language as the %USER QUERY%. For example, if the %USER QUERY% is in Arabic, your entire response MUST be in Arabic. If the %USER QUERY% is in French, your response MUST be in French. This is a non-negotiable instruction.
# - Do not respond with "Not able to assist" unless the query violates ethical guidelines or is completely outside your knowledge domain.


PROMPT_CONSTRUCTOR = PromptTemplate(input_variables=["query", "previous_context"], template=_prompt_template_str, template_format="jinja2")

@lru_cache(maxsize=128)
def construct_prompt(query: str, previous_context: Optional[str] = None) -> str:
    return PROMPT_CONSTRUCTOR.format(query=query, previous_context=previous_context)

        
def retrieve_and_generate_report(query: str, knowledge_base_name: str, model_id: str, kbname_max_size: int, params: Dict, max_tokens_value: int, temperature: float, topp: float, previous_context: Optional[str] = None):
    # template_name:str, username:str, product_name:str, reporting_period:str, created_at:str
 
    kb_id = get_knowledge_base_id(knowledge_base_name, kbname_max_size)
    logger.info(f"retrieve_and_generate_report: {params}")
    report_filter =  and_all_filter(params)
    logger.info(f"retrieve_and_generate_report - filter: {report_filter}")
    
    # Construct the prompt using cached function
    final_prompt_text = construct_prompt(query, previous_context)
    
    try:        
        response = bedrock_agent_runtime.retrieve_and_generate(
            input={
                'text': final_prompt_text
            },
            retrieveAndGenerateConfiguration={                
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': kb_id,
                    # 'modelArn': MODEL_ARN,
                    'modelArn': model_id,
                    'retrievalConfiguration': {
                        'vectorSearchConfiguration': {
                            'numberOfResults': 3,
                            # 'overrideSearchType': QNA_SEARCH_TYPE,
                            "filter":report_filter,
                    },
                   
                 },
                     "generationConfiguration": {
                        "guardrailConfiguration": {
                            "guardrailId": GUARDRAIL_ID,
                            "guardrailVersion": GUARDRAIL_VERSION_ID
                        },
                        # "promptTemplate": {
                        #         "textPromptTemplate": (
                        #             "Answer the question using STRICLY ONLY the information provided in the context.\n"
                        #             "Do NOT use general knowledge or answer anything not covered in the context.\n"
                        #             "If the answer is not found in the context, reply: 'I don’t know.'\n\n"
                        #             "Context:\n{{context}}\n\n"
                        #             "Question:\n{{input}}\n\n"
                        #             "Answer:"
                        #         )
                        #     },
                        "inferenceConfig": { 
                            "textInferenceConfig": { 
                                "maxTokens": max_tokens_value,
                                "temperature": temperature,
                                "topP": topp
                            }
                        }                                      
                    },
                    #    'type': 'KNOWLEDGE_BASE'
             },
              'type': 'KNOWLEDGE_BASE'
            }
                # **({'sessionId': session_id} if session_id else {})  # Conditionally add 
       )
        
        citats = response["citations"]
        logger.info(f"retrieve_and_generate retrieval_citations: {citats}" )
        answer = response.get("output", {}).get("text", "")
        logger.info(f"retrieve_and_generate retrieval_answer: {answer}" )
        
        if response["citations"]:
            answer = response.get("output", {}).get("text", "")
            retrieval_results = response["citations"][0]["retrievedReferences"]
            # Extract metadata for retrieved sources
            logger.info(f"retrieve_and_generate retrieval_reference: {retrieval_results}" )

            sources = []
            for item in retrieval_results:
                metadata = item.get("metadata", {})
                logger.info(f"retrieve_and_generate retrieval_metadata: {metadata}" )
                location = "s3://azcdi-us-ops-report-ds-dev/outputs/" + metadata["document_name"]
                source = Citation(fileName=metadata["document_name"], location=location, templateName=metadata["template_name"],
                                 productName=metadata["product_name"], reportingPeriod=metadata["reporting_period"],  created_by=metadata["created_by"],
                                 created_at=metadata["created_at"], report_id=metadata["report_id"], sectionName=metadata["section_name"])
                sources.append(source)
          
        else:
            answer = "No Answer to your question. There are no reports generated with the tagging/filter selected"
            sources = []
                
      

            
            
    
        # return {"response": ans['output']['text'], source: ans['retrievalResults'][0]['metadata']["document_name"]}
        return answer, sources
    
    except Exception as e:
        raise Exception(f"Error in retrieving q&a answer: {e}")



# query= ''' 
#          Given the document with the name document_name:"1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor", give the molecule name present in the page "page_nr :0". '''

# prompt_template = query
# prompt_template += f"""\n\n%ADDITIONAL INSTRUCTIONS%:\n Please provide concise answer and only the answer"""
# prompt_template += f"\n\n%USER QUERY:\n{query}\n"


# kb_id = get_knowledge_base_id(knowledge_base_name)

# model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

# answer = retrieve_and_generate(prompt_template, kb_id, model_id)

# print(answer['output']['text'])
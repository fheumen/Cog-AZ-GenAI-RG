# Langchain
from operator import itemgetter
from uuid import uuid4
from langchain.memory import ConversationBufferWindowMemory
from langchain.memory import ConversationBufferMemory

# from langchain.memory import BufferMemory
from langchain_core.prompts import (
    ChatPromptTemplate,
    PromptTemplate,
    MessagesPlaceholder,
)
from langchain_core.output_parsers import StrOutputParser
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.opensearch import OpenSearchTranslator
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langchain.chains.query_constructor.base import (
    StructuredQueryOutputParser,
    get_query_constructor_prompt,
)
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_community.chat_message_histories import ChatMessageHistory

from typing import List, Dict, Any

from langchain.chains import ConversationalRetrievalChain, LLMChain, StuffDocumentsChain

# from app.app_streamlit.chains.retrieval import StreamingConversationalRetrievalChain
# from app_streamlit.chains.retrieval import StreamingConversationalRetrievalChain
from langchain_community.llms import Bedrock
from langchain_community.chat_models import BedrockChat
from langchain_aws import ChatBedrock
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch
from requests_aws4auth import AWS4Auth
import boto3
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory

# from const_aws import *


s3 = boto3.client("s3")

# Pinecone
from pinecone import Pinecone

# General
import json
from dotenv import load_dotenv
import os

##from app_streamlit.tracing.langfuse import *


class ReportGeneration:
    # with open("./config_aws.json") as f:
    #     config = json.load(f)

    # RETRIEVER_MODEL_NAME = config["RETRIEVER_MODEL_NAME"]
    # SUMMARY_MODEL_NAME = config["SUMMARY_MODEL_NAME_GPT4"]
    RETRIEVER_MODEL_NAME = None
    SUMMARY_MODEL_NAME = None
    constructor_prompt = None
    vectorstore = None
    retriever = None
    rag_chain_with_source = None
    chat_model = None
    prompt = None

    def __init__(
        self,
        model_id,
        embedding,
        # opensearch_service,
        # opensearch_domain_endpoint,
        # opensearch_index,
    ):
        load_dotenv()
        self.initialize_query_constructor()
        # self.initialize_vector_store(
        #     embedding, opensearch_service, opensearch_domain_endpoint, opensearch_index
        # )
        self.initialize_retriever(model_id)
        self.initialize_chat_model(model_id)

    def initialize_query_constructor(self):
        document_content_description = "Health Care Reports, along with keywords"

        # Define allowed comparators list
        allowed_comparators = [
            "$eq",  # Equal to (number, string, boolean)
            "$ne",  # Not equal to (number, string, boolean)
            "$gt",  # Greater than (number)
            "$gte",  # Greater than or equal to (number)
            "$lt",  # Less than (number)
            "$lte",  # Less than or equal to (number)
            "$in",  # In array (string or number)
            "$nin",  # Not in array (string or number)
            "$exists",  # Has the specified metadata field (boolean)
        ]

        examples = [
            (
                "How many batches were manufactured/rejected at fmc at 15K scale? Given that the product is Fasenra and the reporting period is 14Nov2022_13Nov2023?",
                {
                    "query": "Count of manufactured/rejected bacthes ",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                # "Show me critically acclaimed dramas without Tom Hanks.",
                "What where the batch numbers manufactured at the site fmc? Given that the product is Fasenra and the reporting period is 14Nov2022_13Nov2023?",
                {
                    "query": "batch numbers manufactured",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                "How many batches were fully release at the site fmc? Given that the product is Fasenra and the reporting period is 14Nov2022_13Nov2023?",
                {
                    "query": "Count of batches fully release",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                "How many batches were outside of the specifications? Given that the product is Fasenra and the reporting period is 14Nov2022_13Nov2023?",
                {
                    "query": "Count of batches outside of the specifications",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2023'), eq('product_name', 'Fasenra'))",
                },
            ),
            (
                "Summarize the section summary and conclusion. Given that the product is Fasenra and the reporting period is 14Nov2022_13Nov2023?",
                {
                    "query": "summarize the section summary and conclusion",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2023'), eq('product_name', 'Fasenra'), eq('section_name', 'summary and conclusion'))",
                },
            ),
            # (
            #     "Films similar to Yorgos Lanthmios movies.",
            #     {
            #         "query": "Dark comedy, absurd, Greek Weird Wave",
            #         "filter": 'NO_FILTER',
            #     },
            # ),
            # (
            #     "Find me thrillers with a strong female lead released between 2015 and 2020.",
            #     {
            #         "query": "thriller strong female lead",
            #         "filter": "and(eq('Genre', 'Thriller'), gt('Release Year', 2015), lt('Release Year', 2021))",
            #     },
            # ),
            # (
            #     "Find me highly rated drama movies in English that are less than 2 hours long",
            #     {
            #         "query": "Highly rated drama English under 2 hours",
            #         "filter": 'and(eq("Genre", "Drama"), eq("Language", "English"), lt("Runtime (minutes)", 120))',
            #     },
            # ),
        ]

        metadata_field_info = [
            AttributeInfo(
                name="reporting_period",
                description="The Reporting Period",
                type="string",
            ),
            AttributeInfo(
                name="product_name", description="Name of the Product", type="string"
            ),
            AttributeInfo(
                name="file_name", description="Name of the File", type="string"
            ),
            AttributeInfo(
                name="site_name",
                description="The Name of the Manufacturing center",
                type="string",
            ),
        ]

        self.constructor_prompt = get_query_constructor_prompt(
            document_content_description,
            metadata_field_info,
            allowed_comparators=allowed_comparators,
            examples=examples,
        )

    def initialize_vector_store(
        self,
        embedding,
        opensearch_service,
        opensearch_domain_endpoint,
        opensearch_index,
    ):
        # pc = Pinecone(api_key=pinecone_api_key)

        service = opensearch_service
        credentials = boto3.Session().get_credentials()
        credentials = credentials.get_frozen_credentials()
        region = boto3.Session().region_name
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service,
            session_token=credentials.token,
        )

        self.vectorstore = OpenSearchVectorSearch(
            embedding_function=embedding,
            index_name=opensearch_index,
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            http_compress=True,  # enables gzip compression for request bodies
            connection_class=RequestsHttpConnection,
            opensearch_url=opensearch_domain_endpoint,
            text_field="text",
            metadata_field="metadata",
        )

    def initialize_retriever(self, model_id):

        bedrock = boto3.client(
            service_name="bedrock",
        )
        bedrock_runtime = boto3.client(service_name="bedrock-runtime")

        model_kwargs = {
            # "max_tokens": 4096,
            "temperature": 0.0,
            "top_p": 0,
            "stop_sequences": ["\n\nHuman"],
        }

        self.RETRIEVER_MODEL_NAME = model_id

        query_model = BedrockChat(
            client=bedrock_runtime,
            model_id=self.RETRIEVER_MODEL_NAME,
            model_kwargs=model_kwargs,
            # temperature=0,
            streaming=True,
            # api_key=open_ai_key
        )

        output_parser = StructuredQueryOutputParser.from_components()
        query_constructor = self.constructor_prompt | query_model | output_parser

        self.retriever = SelfQueryRetriever(
            # llm=query_model,
            query_constructor=query_constructor,
            vectorstore=self.vectorstore,
            structured_query_translator=OpenSearchTranslator(),
            search_kwargs={"k": 10},
            search_type="mmr",
        )

    def initialize_chat_model(self, model_id):

        def format_docs(docs):
            return "\n\n".join(
                f"{doc.page_content}\n\nMetadata: {doc.metadata}" for doc in docs
            )

        bedrock = boto3.client(
            service_name="bedrock",
        )
        bedrock_runtime = boto3.client(service_name="bedrock-runtime")

        model_kwargs = {
            "temperature": 0,
            "top_k": 250,
            "top_p": 0,
            "stop_sequences": ["\n\nHuman"],
            # "max_tokens_to_sample": 2048,
            # "prompt": enclosed_prompt
        }

        self.SUMMARY_MODEL_NAME = model_id

        self.chat_model = ChatBedrock(
            client=bedrock_runtime,
            model_id=self.SUMMARY_MODEL_NAME,
            # temperature=0,
            model_kwargs=model_kwargs,
            # temperature=0,
            # streaming=True,
            # api_key=open_ai_key
        )

        #         self.prompt = PromptTemplate(
        #     template=custom_template , input_variables=["context", "question"]
        # )

        #         #self.prompt=  PromptTemplate.from_template(custom_template)
        chatTemplate = """
                  Give a concise answer to the question based on the context(delimited by <ctx> </ctx>) below.
                  -----------
                  <ctx>
                  {context}
                   </ctx>
                  -----------
                 Question: {question}
                 Answer: 
                 """
        # chatTemplate = """
        #           Give a concise answer to the question based on the chat history(delimited by <hs></hs>) and context(delimited by <ctx> </ctx>) below.
        #           -----------
        #           <ctx>
        #           {context}
        #            </ctx>
        #           -----------
        #           <hs>
        #           {chat_history}
        #           </hs>
        #          -----------
        #          Question: {question}
        #          Answer:
        #          """

        # self.prompt = PromptTemplate(
        #        #input_variables=["context", "question", "chat_history"],
        #        input_variables=["context", "question"],
        #        template=chatTemplate
        #       )

        self.prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "Your goal is to answer the following questions based on the retrieved {context}. If your context is empty do not generated an answer, but instead  tel the user                   you couldn't find any information that match their question.",
                ),
                MessagesPlaceholder(variable_name="history"),
                ("human", "{question}"),
            ]
        )
        # self.prompt = ChatPromptTemplate.from_messages(
        #         [(
        #             "system",
        #             """
        #             Your goal is to answer the following questions based on the retirved context. If your context is empty do not generated an answer,
        #             but instead telle the user you couldn't find any information that match their question.

        #             Question: {question}
        #             Context: {context}
        #             """

        #         )

        #         ]
        #         )

        rag_chain_from_docs = (
            RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
            | self.prompt
            | self.chat_model
            | StrOutputParser()
        )

        self.rag_chain_with_source = RunnableParallel(
            {
                "context": self.retriever,
                "question": RunnablePassthrough(),
                "history": itemgetter("history"),
            }
        ).assign(answer=rag_chain_from_docs)

    #         self.prompt=ChatPromptTemplate.from_messages(
    #           [
    #             (
    #                 "system",
    #                 """
    #                 Your goal is to answer the following questions in english based on the retrieved context.

    #                 Question:{question}
    #                 Context:{context}
    #                 """,

    #              ),

    #          ]
    #         )

    # def ask(self, query: str,   memory_chain: ConversationBufferMemory) -> Any:
    def ask(
        self, query: str, session_id: str, get_session_history: BaseChatMessageHistory
    ) -> Any:
        try:
            # for chunk in self.rag_chain_with_source.stream(query):
            #     for key in chunk:
            #         if key == 'answer':
            #             yield chunk[key]

            # message_history = DynamoDBChatMessageHistory(
            # table_name="ReportGen", session_id=session_id
            # )
            # memory_chain = ConversationBufferWindowMemory(
            #     #memory_key="chat_history",
            #     #chat_memory=message_history,
            #     #return_messages=True,
            #     k=3,
            # )
            

            chain_with_history = RunnableWithMessageHistory(
                self.rag_chain_with_source,
                get_session_history,
                input_messages_key="question",
                output_messages_key="answer",
                history_messages_key="history",
            )
            config = {"configurable": {"session_id": session_id}}

            response = chain_with_history.invoke({"question": query}, config=config)
            

            # qa = ConversationalRetrievalChain.from_llm(
            #     llm=self.chat_model,
            #     retriever=self.retriever,
            #     return_source_documents=True,
            #     memory=memory_chain,
            #     #get_chat_history=lambda h :h,
            #     #condense_question_prompt=self.prompt
            #     combine_docs_chain_kwargs={"prompt": self.prompt},
            #     # callbacks=[langfuse_handler_trace]
            #     #callbacks=[trace.getNewHandler()],
            # )

            return response

            # return qa({"question": query, "chat_history":chat_history})
        except Exception as e:
            print(f"An error occurred Fabrice: {e}")
            return f"An error occurred Fabrice: {e}"

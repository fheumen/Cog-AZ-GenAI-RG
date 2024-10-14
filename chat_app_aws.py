# Langchain
from langchain.memory import ConversationBufferWindowMemory
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.opensearch import OpenSearchTranslator
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langchain_pinecone import PineconeVectorStore
from langchain.vectorstores import Pinecone
from langchain_openai import OpenAIEmbeddings
from langchain.chains.query_constructor.base import (
    StructuredQueryOutputParser,
    get_query_constructor_prompt,
)

from typing import List, Dict, Any

from app_streamlit.chains.retrieval import StreamingConversationalRetrievalChain
from langchain_community.llms import Bedrock
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch
from requests_aws4auth import AWS4Auth
import boto3
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory


s3 = boto3.client("s3")

# Pinecone
from pinecone import Pinecone

# General
import json
from dotenv import load_dotenv
import os
from app_streamlit.tracing.langfuse import *


class ReportGeneration:
    with open("./config_aws.json") as f:
        config = json.load(f)

    RETRIEVER_MODEL_NAME = config["RETRIEVER_MODEL_NAME"]
    SUMMARY_MODEL_NAME = config["SUMMARY_MODEL_NAME_GPT4"]
    constructor_prompt = None
    vectorstore = None
    retriever = None
    rag_chain_with_source = None
    chat_model = None
    prompt = None

    def __init__(self, embedding, opensearch_domain_endpoint, opensearch_index):
        load_dotenv()
        self.initialize_query_constructor()
        self.initialize_vector_store(
            embedding, opensearch_domain_endpoint, opensearch_index
        )
        self.initialize_retriever()
        self.initialize_chat_model()

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
                "How many batches were manufactured/rejected at the site fmc at 15K scale for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2023?",
                {
                    "query": "Count of manufactured/rejected bacthes ",
                    "filter": "and(eq('reporting_period_startdate', '14Nov2022'), eq('reporting_period_enddate', '13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                # "Show me critically acclaimed dramas without Tom Hanks.",
                "What where the batch numbers manufactured at the site fmc for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2023?",
                {
                    "query": "batch numbers manufactured",
                    "filter": "and(eq('reporting_period_startdate', '14Nov2022'), eq('reporting_period_enddate', '13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                "How many batches were fully release at the site fmc  for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2023?",
                {
                    "query": "Count of batches fully release",
                    "filter": "and(eq('reporting_period_startdate', '14Nov2022'), eq('reporting_period_enddate', '13Nov2023'), eq('product_name', 'Fasenra'), eq('site_name', 'fmc'))",
                },
            ),
            (
                "How many batches were outside of the specifications for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2023?",
                {
                    "query": "Count of batches outside of the specifications",
                    "filter": "and(eq('reporting_period_startdate', '14Nov2022'), eq('reporting_period_enddate', '13Nov2023'), eq('product_name', 'Fasenra'))",
                },
            ),
            (
                "Summarize the section summary and conclusion for the product Fasenra during the reporting period between '14Nov2022' and '13Nov2023'?",
                {
                    "query": "summarize the section summary and conclusion",
                    "filter": "and(eq('reporting_period_startdate', '14Nov2022'), eq('reporting_period_enddate', '13Nov2023'), eq('product_name', 'Fasenra'), eq('section_name', 'summary and conclusion'))",
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
                name="reporting_period_startdate",
                description="The Reporting Period Start date",
                type="string",
            ),
            AttributeInfo(
                name="reporting_period_enddate",
                description="The Reporting Period End date",
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
        self, embedding, opensearch_domain_endpoint, opensearch_index
    ):
        # pc = Pinecone(api_key=pinecone_api_key)

        service = "aoss"
        credentials = boto3.Session().get_credentials()
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

    def initialize_retriever(self):
        query_model = Bedrock(
            model_id=self.RETRIEVER_MODEL_NAME,
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
            search_type="mmr"
        )

    def initialize_chat_model(self):
        def format_docs(docs):
            return "\n\n".join(
                f"{doc.page_content}\n\nMetadata: {doc.metadata}" for doc in docs
            )

        self.chat_model = Bedrock(
            model_id=self.SUMMARY_MODEL_NAME,
            # temperature=0,
            streaming=True,
            # api_key=open_ai_key
        )

        self.prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """
                    Your goal is to answer the following questions based on their 
                    retrieved context. If your context is empty
                    do not generated an answer, but instead
                    tell the user you couldn't find any information that match their question..               

                    Question: {question} 
                    Context: {context} 
                    """,
                ),
            ]
        )

        # Create a chatbot Question & Answer chain from the retriever
        rag_chain_from_docs = (
            RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
            | self.prompt
            | self.chat_model
            | StrOutputParser()
        )

        self.rag_chain_with_source = RunnableParallel(
            {"context": self.retriever, "question": RunnablePassthrough()}
        ).assign(answer=rag_chain_from_docs)

    def ask(self, query: str,  chat_history: List[Dict[str, Any]] = []) -> Any:
        try:
            # for chunk in self.rag_chain_with_source.stream(query):
            #     for key in chunk:
            #         if key == 'answer':
            #             yield chunk[key]
            # rag_chain_with_source.invoke(query, callbacks=[trace.getNewHandler()])

            #message_history = DynamoDBChatMessageHistory(
                #table_name="ReportGen", session_id=session_id
            #)
            #memory_chain = ConversationBufferWindowMemory(
                #memory_key="chat_history",
                #chat_memory=message_history,
                #return_messages=True,
                #k=3,
            #)

            qa = StreamingConversationalRetrievalChain.from_llm(
                llm=self.chat_model,
                retriever=self.retriever,
                return_source_documents=True,
                #memory=memory_chain,
                combine_docs_chain_kwargs={"prompt": self.prompt},
                # callbacks=[langfuse_handler_trace]
                callbacks=[trace.getNewHandler()],
            )
            return qa({"question": query, "chat_history": chat_history})
        except Exception as e:
            print(f"An error occurred: {e}")
            return f"An error occurred: {e}"

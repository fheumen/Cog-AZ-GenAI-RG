# Langchain
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.pinecone import PineconeTranslator
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

# Pinecone
from pinecone import Pinecone

# General
import json
from dotenv import load_dotenv
import os
from app_streamlit.tracing.langfuse import *


class ReportGeneration:
    with open('./config.json') as f:
        config = json.load(f)

    RETRIEVER_MODEL_NAME = config["RETRIEVER_MODEL_NAME"]
    SUMMARY_MODEL_NAME = config["SUMMARY_MODEL_NAME_GPT4"]
    constructor_prompt = None
    vectorstore = None
    retriever = None
    rag_chain_with_source = None
    chat_model = None
    prompt = None

    def __init__(self, openai_api_key, pinecone_api_key, pinecone_index_name, pinecone_env_region):
        load_dotenv()
        self.initialize_query_constructor()
        self.initialize_vector_store(
            openai_api_key, pinecone_api_key, pinecone_index_name, pinecone_env_region)
        self.initialize_retriever(openai_api_key)
        self.initialize_chat_model(openai_api_key)

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
                "How many batches were manufactured/rejected at the site fmc at 15K scale for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?",
                {
                    "query": "Count of manufactured/rejected bacthes ",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2013'), in('product_name', ['Fasenra']), eq('site_name', 'fmc'))",
                },
            ),
            (
                # "Show me critically acclaimed dramas without Tom Hanks.",
                "What where the batch numbers manufactured at the site fmc for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?",
                {
                    "query": "batch numbers manufactured",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2013'), in('product_name', ['Fasenra']), eq('site_name', 'fmc'))",
                },
            ),
            (
                "How many batches were fully release at the site fmc  for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?",
                {
                    "query": "Count of batches fully release",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2013'), in('product_name', ['Fasenra']), eq('site_name', 'fmc'))",
                },
            ),

            (
                "How many batches were outside of the specifications for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?",
                {
                    "query": "Count of batches outside of the specifications",
                    "filter": "and(eq('reporting_period', '14Nov2022_13Nov2013'), in('product_name', ['Fasenra']))",
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
                name="reporting_period", description="The Reporting Period", type="string"),
            AttributeInfo(name="product_name",
                          description="Name of the Product", type="string"),
            AttributeInfo(name="file_name",
                          description="Name of the File", type="string"),
            AttributeInfo(name="site_name",
                          description="The Name of the Manufacturing center", type="string"),
        ]

        self.constructor_prompt = get_query_constructor_prompt(
            document_content_description,
            metadata_field_info,
            allowed_comparators=allowed_comparators,
            examples=examples,
        )

    def initialize_vector_store(self, open_ai_key, pinecone_api_key, pinecone_index_name, pinecone_env_region):
        # pc = Pinecone(api_key=pinecone_api_key)

        pc = Pinecone(
            api_key=pinecone_api_key,
            environment=pinecone_env_region,
        )

        # Target index and check status
        pc_index = pc.Index(pinecone_index_name)

        embeddings = OpenAIEmbeddings(model='text-embedding-3-large',
                                      api_key=open_ai_key)

        # embeddings = OpenAIEmbeddings(openai_api_key=open_ai_key)

        # self.vectorstore = Pinecone.from_existing_index(
        #     embedding=embeddings, index_name=pinecone_index_name
        # )
        self.vectorstore = PineconeVectorStore(
            pc_index, embeddings
        )

    def initialize_retriever(self, open_ai_key):
        query_model = ChatOpenAI(
            model=self.RETRIEVER_MODEL_NAME,
            temperature=0,
            streaming=True,
            api_key=open_ai_key
        )

        output_parser = StructuredQueryOutputParser.from_components()
        query_constructor = self.constructor_prompt | query_model | output_parser

        self.retriever = SelfQueryRetriever(
            # llm=query_model,
            query_constructor=query_constructor,
            vectorstore=self.vectorstore,
            structured_query_translator=PineconeTranslator(),
            search_kwargs={'k': 10}
        )

    def initialize_chat_model(self, open_ai_key):
        def format_docs(docs):
            return "\n\n".join(f"{doc.page_content}\n\nMetadata: {doc.metadata}" for doc in docs)

        self.chat_model = ChatOpenAI(
            model=self.SUMMARY_MODEL_NAME,
            temperature=0,
            streaming=True,
            api_key=open_ai_key
        )

        self.prompt = ChatPromptTemplate.from_messages(
            [
                (
                    'system',
                    """
                    Your goal is to write a summary to users based on their 
                    query and the retrieved context. If your context is empty
                    do not generated a summary, but instead
                    tell the user you couldn't find any information that match their query.
                    Your summary should be concise, original, and at least two to three sentences 
                    long.

                    YOU CANNOT GENERATED A SUMMARY IF YOUR IS EMPTY                    

                    Question: {question} 
                    Context: {context} 
                    """
                ),
            ]
        )

        # Create a chatbot Question & Answer chain from the retriever
        rag_chain_from_docs = (
                RunnablePassthrough.assign(
                    context=(lambda x: format_docs(x["context"])))
                | self.prompt
                | self.chat_model
                | StrOutputParser()
        )

        self.rag_chain_with_source = RunnableParallel(
            {"context": self.retriever, "question": RunnablePassthrough()}
        ).assign(answer=rag_chain_from_docs)

    def ask(self, query: str, chat_history: List[Dict[str, Any]] = []) -> Any:
        try:
            # for chunk in self.rag_chain_with_source.stream(query):
            #     for key in chunk:
            #         if key == 'answer':
            #             yield chunk[key]
            # rag_chain_with_source.invoke(query, callbacks=[trace.getNewHandler()])
            qa = StreamingConversationalRetrievalChain.from_llm(
                llm=self.chat_model, retriever=self.retriever, return_source_documents=True,
                combine_docs_chain_kwargs={'prompt': self.prompt},
                # callbacks=[langfuse_handler_trace]
                callbacks=[trace.getNewHandler()]
            )
            return qa({"question": query, "chat_history": chat_history})
        except Exception as e:
            print(f"An error occurred: {e}")
            return f"An error occurred: {e}"

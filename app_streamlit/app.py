from typing import Set, List, Dict, Any
import os

#from backend.core import *
import streamlit as st
from streamlit_chat import message
#from streamlit_demo.tracing.langfuse import langfuse

###############################

from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chat_models import ChatOpenAI

# from langchain.chains import RetrievalQA
from app_streamlit.chains.retrieval import StreamingConversationalRetrievalChain
from langchain.vectorstores import Pinecone
from langchain.memory import ConversationBufferMemory

from dotenv import load_dotenv
load_dotenv()

from chat_app import ReportGeneration


# Get Langchain handler for a trace
from app_streamlit.tracing.langfuse import *
print("xxxxxxxxxxxxxxxxxxxxx")
print(trace_id)



#@observe()

def generate_response(input_text,  chat_history: List[Dict[str, Any]] = []) -> Any:
    pinecone_api_key = os.environ["PINECONE_API_KEY"]
    pinecone_index_name = os.environ["PINECONE_INDEX_NAME"]
    pinecone_env_region = os.environ["PINECONE_ENVIRONMENT_REGION"]
    openai_api_key = os.environ["OPENAI_API_KEY"]

    chat = ReportGeneration(openai_api_key, pinecone_api_key, pinecone_index_name, pinecone_env_region)
    #st.write_stream(chat.ask(input_text))
    return chat.ask({"question": input_text, "chat_history": chat_history})

def run_llm(query: str, chat_history: List[Dict[str, Any]] = []) -> Any:
    embeddings = OpenAIEmbeddings(openai_api_key=os.environ.get("OPENAI_API_KEY"))
    docsearch = Pinecone.from_existing_index(
        embedding=embeddings, index_name=INDEX_NAME
    )
    llm = ChatOpenAI(verbose=True, temperature=0)
    #memory = ConversationBufferMemory(memory_key="messages", return_message=True)
    #llm = openai.chat(verbose=True, temperature=0)
    # qa = RetrievalQA.from_chain_type(
    #     llm=chat,
    #     chain_type="stuff",
    #     retriever=docsearch.as_retriever(),
    #     return_source_documents=True,
    # )


    qa = StreamingConversationalRetrievalChain.from_llm(
        llm=llm, retriever=docsearch.as_retriever(), return_source_documents=True,
        #memory=memory,
        #callbacks=[langfuse_handler_trace]
        callbacks=[trace.getNewHandler()]
    )
    return qa({"question": query, "chat_history": chat_history})
#####################

def build_sidebar():
    with st.sidebar:
        st.title("Technologies")

        st.subheader("Natural Language Query (NLQ)")
        st.write(
            """
        [Natural language query (NLQ)](https://www.yellowfinbi.com/glossary/natural-language-query) enables analytics users to ask questions of their data. It parses for keywords and generates relevant answers sourced from related databases, with results typically delivered as a report, chart or textual explanation that attempt to answer the query, and provide depth of understanding.
        """
        )

       #st.subheader("Amazon SageMaker Studio")
       # st.write(
       #     """
       # [Amazon SageMaker Studio](https://aws.amazon.com/sagemaker/studio/) is a fully integrated development environment (IDE) where you can perform all machine learning (ML) development steps, from preparing data to building, training, and deploying your ML models, inclduing [JumpStart Foundation Models](https://docs.aws.amazon.com/sagemaker/latest/dg/jumpstart-foundation-models.html).
       # """
       # )

        st.subheader("LangChain")
        st.write(
            """
        [LangChain](https://python.langchain.com/en/latest/index.html) is a framework for developing applications powered by language models.
        """
        )

        st.subheader("Pinecone")
        st.write(
            """
        [Pinecone](https://www.pinecone.io/) is the open-source embedding database. Pinecone makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.
        """
        )

        st.subheader("Streamlit")
        st.write(
            """
        [Streamlit](https://streamlit.io/) is an open-source app framework for Machine Learning and Data Science teams. Streamlit turns data scripts into shareable web apps in minutes. All in pure Python. No front-end experience required.
        """
        )


def build_form(col1, col2):
    with col1:
        with st.container():
            st.title("Report Generation Assistant")
            st.subheader("Ask questions of your data using natural language.")

        with st.container():
            option = st.selectbox(
                "Choose a Reporting Period",
                (
                    "14Nov2022_13Nov2013",
                ),
                # label_visibility=st.session_state.visibility,
                disabled=st.session_state.disabled,
            )
            option = st.selectbox(
                "Choose a Product Name",
                (
                    "Lumoxity", "Enhertu", "Vaxzevria", "Beyfortus", "IMJUDO", "IMFINZI", "Synagis", "Saphnelo", "Fasenra", "Tezspire",
                ),
                # label_visibility=st.session_state.visibility,
                disabled=st.session_state.disabled,
            )
            option = st.selectbox(
                "Choose a Site:",
                (
                    "All for ISPR",
                    "fmc",
                    "sbc",
                    "speke",
                ),
               #label_visibility=st.session_state.visibility,
                disabled=st.session_state.disabled,
            )

            with st.expander("Sample questions"):
                st.text(
                    """
                How many batches were manufactured/rejected at the site fmc at 15K scale for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?
                What where the batch numbers manufactured at the site fmc for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?
                How many batches were fully release at the site fmc  for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?
                How many batches were outside of the specifications for the product Fasenra during the reporting period between 14Nov2022 and 13Nov2013?
                """
                )
    with col2:
        with st.container():
            st.button("clear chat", on_click=clear_session)

def get_text(col1):
    with col1:
        input_text = st.text_input(
            "Ask a question:",
            "",
            key="query_text",
            placeholder="Your question here...",
            on_change=clear_text(),
        )


def clear_text():
    st.session_state["query"] = st.session_state["query_text"]
    st.session_state["query_text"] = ""

def clear_session():
    for key in st.session_state.keys():
        del st.session_state[key]
def create_sources_string(source_urls: Set[str]) -> str:
    if not source_urls:
        return ""
    sources_list = list(source_urls)
    sources_list.sort()
    sources_string = "sources:\n"
    for i, source in enumerate(sources_list):
        sources_string += f"{i+1}. {source}\n"
    return sources_string

#st.header("AZ_UC3 Chat Bot")
st.set_page_config(page_title="Report Generation Assistant")

# Store the initial value of widgets in session state
if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

if "generated" not in st.session_state:
    st.session_state["generated"] = []

if "past" not in st.session_state:
    st.session_state["past"] = []

if "query" not in st.session_state:
    st.session_state["query"] = []

if "query_text" not in st.session_state:
    st.session_state["query_text"] = []

if "user_prompt_history" not in st.session_state:
    st.session_state["user_prompt_history"] = []

if "chat_answer_history" not in st.session_state:
    st.session_state["chat_answer_history"] = []

if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []
#
 # define streamlit colums
col1, col2 = st.columns([4, 1], gap="large")

# build the streamlit sidebar
build_sidebar()

# build the main app ui
build_form(col1, col2)

# get the users query
get_text(col1)
user_input = st.session_state["query"]
print(user_input)





if user_input:
    with st.spinner("Generating response .."):
        try:

            generated_response = generate_response(
                input_text=user_input, chat_history=st.session_state["chat_history"]
            )
            print(generated_response)

            sources = set(
                [doc.metadata["file_name"] for doc in generated_response["source_documents"]]
                #[doc.metadata["source"] for doc in generated_response["sources"]]
            )

            #sources = set(generated_response["sources"])

            #print(generated_response["sources"])

            formatted_response = (
                # f"{generated_response['result']} \n\n {create_sources_string(sources)}"
                f"{generated_response['answer']} \n\n {create_sources_string(sources)}"
            )

            st.session_state["user_prompt_history"].append(user_input)
            st.session_state["chat_answer_history"].append(formatted_response)
            st.session_state["chat_history"].append((user_input, generated_response["answer"]))
            #st.session_state.generated.append(generated_response)
            # st.session_state["chat_history"].append((prompt, generated_response["result"]))
            # import time
            # time.sleep(3)
        except Exception as exc:
            st.session_state["user_prompt_history"].append(user_input)
            st.session_state.generated.append(
                "The datasource does not contain information to answer this question."
            )
            print("An exception occurred:", exc)

if st.session_state["chat_answer_history"]:
    #for generated_response, user_query in zip(
    #    st.session_state["chat_answer_history"], st.session_state["user_prompt_history"]
    #):
    for i in range(len(st.session_state["chat_answer_history"]) - 1, -1, -1):
        message(st.session_state["user_prompt_history"][i], is_user=True, key=str(i) + "_user")
        message(st.session_state["chat_answer_history"][i], key=str(i))


        #message(user_query, is_user=True)
        #message(generated_response, key=str(i) + "_user")

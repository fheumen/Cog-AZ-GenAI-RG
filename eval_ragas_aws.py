from ragas.metrics.critique import harmfulness
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
)
from ragas import evaluate
import pandas as pd
from tqdm import tqdm
from datasets import Dataset

from utils_aws import *
from const_aws import *

from chat_app_aws import ReportGeneration
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection, OpenSearch
from requests_aws4auth import AWS4Auth

from typing import Set, List, Dict, Any

import os
import io
import boto3

from dotenv import load_dotenv

load_dotenv()

# Get Langchain handler for a trace
from app_streamlit.tracing.langfuse import *

metrics = [faithfulness, answer_relevancy, context_precision, harmfulness]

########################
## instantiate client
s3 = boto3.client("s3")
###########################
# langfuse = Langfuse()
# langfuse.auth_check()

####### Set the Knowledgebase
opensearch_domain_endpoint = os.environ["OPENSEARCH_ENDPOINT"]
opensearch_index = os.environ["OPENSEARCH_INDEX"]

##### Set the embedding model
embeddings = BedrockEmbeddings(model_id=os.environ["aws_embedding_model"])


def evaluate_rag(
    question,
    ground_truth,
    product_name,
    reporting_period,
    chat_history: List[Dict[str, Any]] = [],
):
    trace = langfuse.trace(name="rag")
    chat = ReportGeneration(embedding, opensearch_domain_endpoint, opensearch_index)
    question_with_context = (
        question
        + f". Given that the product name is {product_name} and the reporting period is {reporting_period}"
    )
    contexts = chat.retriever.invoke(question_with_context)

    answer = chat.ask({"question": question_with_context, "chat_history": chat_history})
    # contexts = answer["contexts"]
    trace.span(
        name="retrieval", input={"question": question}, output={"contexts": contexts}
    )
    # print(contexts)
    # print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    # print(answer)
    trace.span(
        name="generation",
        input={"question": question, "contexts": contexts},
        output={"answer": answer["answer"]},
    )
    data = {
        "question": [question],
        "answer": [answer["answer"]],
        "contexts": [[context.page_content for context in contexts]],
        "ground_truth": [ground_truth],
    }
    # print(data.answer.answer.answer[0])
    dataset = Dataset.from_dict(data)
    # dataset = Dataset.from_pandas(pd.DataFrame(data))
    result = evaluate(
        dataset,
        metrics=[
            context_precision,
            faithfulness,
            answer_relevancy,
            context_recall,
            harmfulness,  #
        ],
        llm=chat.chat_model,
        embeddings=embeddings,
    )
    for m in metrics:
        print(metrics)
        trace.score(name=m.name, value=result[m.name])
    return answer["answer"]


# %%time
######### Download the Test Data into the Test
response = s3.get_object(Bucket=bucket_name, Key=TEST_FILE_PATH)
excel_data = response["Body"].read()
test_dataset = pd.read_excel(io.BytesIO(excel_data))
test_dataset = test_dataset[~test_dataset.ground_truth.isna()]

######### Filter Parameter setting
# Download the file
response = s3.get_object(
    Bucket=bucket_name, Key=f"{output_folder}{pqr_param_json_filename}"
)
content = response["Body"].read().decode("utf-8")
# Parse the JSON content
data = json.loads(content)
# Extract specific fields into variables
product_name = data.get("product_name")
reporting_period = data.get("reporting_period")

#### Initialise the Test Data
chat_history = []
for _, row in tqdm(test_dataset.iterrows()):
    answer = evaluate_rag(
        row["question"],
        row["ground_truth"],
        product_name,
        reporting_period,
        chat_history=chat_history,
    )
    chat_history.append((row["question"], answer))

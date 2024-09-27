# Databricks notebook source
# MAGIC %run ./Utils/StorageAccountAccess_Secrets

# COMMAND ----------

from const import *
#from unstructured.partition.pdf import partition_pdf
import fitz 
import os
import shutil
import json
from dotenv import load_dotenv
load_dotenv()
#from unstructured.partition.pdf import partition_pdf
#from unstructured.documents.elements import NarrativeText, Title, ListItem
import re
from datetime import datetime
import pdfplumber
from docx import Document
from docx.shared import Inches
import os
from PIL import Image
import io
import shutil
import tiktoken
##########################################
from langchain.embeddings.openai import OpenAIEmbeddings
#from langchain.embeddings import OpenAIEmbeddings
#from langchain_openai import OpenAIEmbeddings
from langchain_openai.embeddings import AzureOpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI
from pinecone import Pinecone
from langchain.docstore.document import Document
import openai
from openai import AzureOpenAI
import uuid  # for generating unique IDs
import pandas as pd

# COMMAND ----------

storageAccountName = os.environ.get("storageAccountName")
storageAccountAccessKey = os.environ.get("storageAccountAccessKey")
#storageAccountAccessKey = "KqDmutvYKFkUfibvOLivxyuYP+x9KqY3DblOGarDbgJjMBEbEfUn3bhEjyuNO+DUvgDdOcl2fgrX+AStRyyYDA=="
print(storageAccountAccessKey)
mount_blob_storage("inputs")
mount_blob_storage("outputs")

# COMMAND ----------

deployment_name = os.environ.get("embedding_deployment_name")
openai.api_type = os.environ.get("azure_openai_api_type")
openai.api_key = os.environ.get("AZURE_OPENAI_API_KEY")
openai.api_base = os.environ.get("azure_openai_api_base")
openai.api_version = os.environ.get("azure_openai_api_version")

# COMMAND ----------

input_folder = f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save
upload_folder = f"{UPLOAD_TMP_PATH}"

# COMMAND ----------

client = AzureOpenAI(
  api_key = os.environ.get("AZURE_OPENAI_API_KEY"),  
  api_version = os.environ.get("azure_openai_api_version"),
  azure_endpoint =os.environ.get("azure_openai_api_base")
)

# COMMAND ----------

# Initialise Pinecone
pinecone = Pinecone(
   api_key=os.environ["PINECONE_API_KEY"],
    environment=os.environ["PINECONE_ENVIRONMENT_REGION"],
)


# COMMAND ----------

#### Check if an entry is already available in Pinececone.
def pinecone_document_noexists(file_name, reporting_period, product_name, site_name, index_name):
    # initialize pinecone
    # pinecone.init(
    #    api_key=PINECONE_API_KEY,  # find at app.pinecone.io
    #    environment=PINECONE_API_ENV # next to api key in console
    # )

    index = pinecone.Index(index_name)

    # Need to pass also the vector , but this can be just the embedding dimension
    results = index.query(
        vector=[0] * 3072,
        top_k=1,
        include_metadata=True,
        #namespace=base_domain,
        filter={"file_name": file_name ,
                "reporting_period": reporting_period,
                'product_name': product_name, 
                'site_name': site_name , 
                },
    )

    #time.sleep(30)
    print(results["matches"])
    # Return True (0) if the id was not found, False otherwise
    if len(results["matches"]) > 0:
        return 0
    else:
        return 1
    # return results

# COMMAND ----------

def get_openai_embeddings(texts):
    response = client.embeddings.create(
        input=texts, 
        model=deployment_name  # or your specific embedding model from Azure
    )
    #return [data["embedding"] for data in response['data']]
    return ((response.data)[0]).embedding
    #print(response.model_dump_json(indent=2))

# COMMAND ----------

#### Insert content of an url into Pinecone
def pinecone_insert_docs(documents, index_name):
    from langchain.vectorstores import Pinecone
    from langchain.embeddings.openai import OpenAIEmbeddings

    
    index = pinecone.Index(index_name)    

    print(f"Going to insert {len(documents)} to Pinecone")
    # Prepare embeddings and documents for insertion
    for doc in documents:
        embedding = get_openai_embeddings([doc["content"]])
        #print(embedding)
        #metadata = doc["metadata"] 
        #print("****** print metadata")
        #print(metadata)
        #print(metadata["reporting_period"])
        unique_id = str(uuid.uuid4())
    # Upsert the document into Pinecone index reporting_period
        index.upsert([
        {
            "id": unique_id,
            "values": embedding,
            "metadata": doc["metadata"]  # Metadata associated with the document
        }
    ])
    #Pinecone.flush()
    #time.sleep(30)
    
    # Pinecone.from_documents(documents, embeddings, index_name=INDEX_NAME)
    print("****** Added to Pinecone vectorstore vector")

# COMMAND ----------

################### Extract reporting period, product name and site name from the first page in a pqr report
def extract_reporting_period_product_name_site_name(first_page_text, date_pattern, site_names, product_names):

    site_name = None
    product_name = None
    reporting_period_startdate = None
    reporting_period_enddate = None
    ###### Extract Site Name from the first page
    for site in site_names:
        if site.lower() in first_page_text.lower():
            site_name = site
 
    ##### Extract Product Name from the first page
    for product in product_names:
        if product.lower() in first_page_text.lower():
            product_name = product

    # Extract Reporting Period from the first page
    match = re.search(date_pattern, first_page_text)
    if match:
            # Check which format of date was matched
        if match.group(1) and match.group(2):
            start_date_str, end_date_str = match.group(1), match.group(2)
            date_format = '%d %b %Y'
        elif match.group(3) and match.group(4):
             start_date_str, end_date_str = match.group(3), match.group(4)
             date_format = '%d %b %Y'
        elif match.group(5) and match.group(6):
            start_date_str, end_date_str = match.group(5), match.group(6)
            date_format = '%B %d, %Y'
            
            # Convert to datetime objects and format as desired (e.g., 14Nov2022)
        reporting_period_startdate = datetime.strptime(start_date_str, date_format).strftime('%d%b%Y')
        reporting_period_enddate = datetime.strptime(end_date_str, date_format).strftime('%d%b%Y')
            
        #return reporting_period_startdate, reporting_period_enddate
    return reporting_period_startdate, reporting_period_enddate, product_name, site_name

# COMMAND ----------

def extract_text_tables_images_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename, site_name, output_dir,
                                           chunk_min, chunk_max):
# extract_text_tables_images_by_sections(filename_to_proceed, section_names, reporting_period, product_name, pqr_file_name, site_name, output_path_images, 1024, 1500)

#def extract_text_tables_images_by_sections(pdf_path, section_names, single_filename, output_dir,
#                                           chunk_min, chunk_max):
    """
    Extract text, tables, and images from a PDF based on a given list of section names.
    
    Args:
        pdf_path (str): Path to the input PDF file.
        section_names (list): A list of section names to chunk the PDF by.
        output_dir (str): Directory to save extracted images.
        
    Returns:
        list: A list of dictionaries, each containing 'section_name', 'text', 'images', and 'tables'.
    """
    
    ####
    header_flag = 0
    footer_flag = 0

    # Normalize section names for matching
    normalized_section_names = [name.lower() for name in section_names]
    
    # Initialize data structure
    sections = []
    chunk_sections = []
    docs = [] ### list of document object for Vector Database
    current_section = {
                        "section_name": None,
                        "file_name": single_filename,
                        "reporting_period_startdate": None,
                        "reporting_period_enddate": None,
                        "product_name": None,
                        "site_name": None, 
                        "page_num": None,
                        "images": [],
                        "tables": [],                        
                        "text": ""
                    }
    
    chunk_current_section = {
                        "section_name": None,
                        "file_name": single_filename,
                        "reporting_period_startdate": None,
                        "reporting_period_enddate": None,
                        "product_name": None,
                        "site_name": None, 
                        "page_num": None,
                        "images": [],
                        "tables": [],                        
                        "text": ""
                    }
    
    # Function to find if a text matches any section name
    def match_section(text):
        #normalized_text = ((text.strip().lower().split(' '))[1]).strip()
        textsplit = text.strip().lower().split(' ')
        if len(textsplit) > 1:          
            firstchar = ((text.strip().lower().split(' '))[0]).strip()
            restchar = (" ".join((text.strip().lower().split(' '))[1:])).strip()
            if firstchar.isdigit():
               return next((name for name in normalized_section_names if restchar.startswith(name)), None)
        return None
    
    # Open the PDF using pdfplumber
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if page_num == 0: ##### Extract reporting period, product name and site name from the first page
               reporting_period_startdate, reporting_period_enddate, product_name, site_name = extract_reporting_period_product_name_site_name(text, date_pattern, site_names, product_names)
            
            elif page_num == 1: ##### Jump the Table of Content
                continue
                      
            # Split text into lines for matching section names
            lines = text.split("\n")
            for line in lines:
                ######### Skip the header and footer
                if re.search(header_pattern,line): 
                    header_flag = 1
                    continue
                if header_flag == 1:
                    header_flag = 0                    
                    continue
                if re.search(footer_pattern,line): 
                    footer_flag = 1
                    continue
                if footer_flag == 1: 
                    footer_flag == 0                   
                    continue

            ### No Header, No Footer, No Page 1
                matched_section = match_section(line)
                if matched_section:
                    # Save the previous section
                    if chunk_current_section["section_name"]:
                        doc = {
                                "content": chunk_current_section["text"],
                                "metadata": chunk_current_section,
                        }
                        docs.append(doc)
                        chunk_sections.append(chunk_current_section)

                    if current_section["section_name"]:
                        sections.append(current_section)
                    
                    # Start a new section
                    current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period_startdate": reporting_period_startdate,
                        "reporting_period_enddate": reporting_period_enddate,
                        "product_name": product_name,
                        "site_name": site_name, 
                        "page_num": page_num,
                        "images": [],
                        "tables": [],                        
                        "text": ""
                    }

                    # Start a new section chunk
                    chunk_current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period_startdate": reporting_period_startdate,
                        "reporting_period_enddate": reporting_period_enddate,
                        "product_name": product_name,
                        "site_name": site_name, 
                        "page_num": page_num,
                        "images": [],
                        "tables": [],                        
                        "text": ""
                    }
                
                # Add the line text to the current section if inside a section
                if current_section["section_name"]:
                    current_section["text"] += line.strip() + "\n"
                
                if chunk_current_section["section_name"]:
                    #current_section["text"] += line.strip() + "\n"
                    # check if the size of a chunk text into a section is within the chunk size
                    if (len(chunk_current_section["text"] + line.strip()) > chunk_max) and (len(chunk_current_section["text"]) > chunk_min):
                         # Save a section chunk
                        chunk_current_section_tmp = chunk_current_section.copy()
                        chunk_current_section_tmp["text"] += line.strip() + "\n"
                        ################ create a doc object                        
                        doc = {
                                "content": chunk_current_section_tmp["text"],
                                "metadata": chunk_current_section_tmp,
                        }
                        docs.append(doc)
                        chunk_sections.append(chunk_current_section_tmp)
                        #sections.append(current_section_tmp)

                        chunk_current_section["text"] = ""
                    
                    chunk_current_section["text"] += line.strip() + "\n"    #print(sections)
                                       
                    
            
            # Extract tables
            tables = page.extract_tables()
            if tables:
                #for table in tables:                  

                for table_index, table in enumerate(tables):
                # Convert the table into a pandas DataFrame
                   df = pd.DataFrame(table)
                
                   # Create a unique filename for each table (based on page number and table index)
                   sfilename = single_filename.split(".")[0]
                   table_filename = f"{sfilename}_table_page_{page_num+1}_table_{table_index+1}.csv"
                   output_csv = f"{output_dir}/{table_filename}"
                   tmp_file = "/tmp/"+ table_filename
                
                   # Save the DataFrame as a CSV file
                   df.to_csv(tmp_file, index=False, header=True)
                   shutil.move(tmp_file, output_csv )
                
                   #table_count += 1
                   #print(f"Table {table_count} saved to {output_csv}")
                   current_section["tables"].append(output_csv)
            
            # Extract images
            if page.images:
                for img_idx, image_dict in enumerate(page.images):
                    # Extract the image bytes
                    print("image_dict", image_dict)
                    page_height = page.height
                    image_bbox = (image_dict['x0'], page_height- image_dict['y1'], image_dict['x1'], page_height- image_dict['y0'])
                    #image = page.within_bbox(image_dict['bbox']).to_image()
                    image = page.within_bbox(image_bbox).to_image()
                    image_bytes = image.original
                    image_ext = "png"  # Can modify based on your preference
                    
                    # Save the image to a file
                    sfilename = single_filename.split(".")[0]
                    image_filename = f"{sfilename}_page_{page_num+1}_img_{img_idx+1}.{image_ext}"
                    #print("titititititti")
                    #print(image_filename)
                    image_path =  f"{output_dir}/{image_filename}"
                    tmp_file = "/tmp/"+ image_filename
                    image_bytes.save(image_path , "PNG")
                    #shutil.move(tmp_file, image_path )
                    current_section["images"].append(image_path)
    
    # Append the last section
    if current_section["section_name"]:
        doc = {
                                "content": chunk_current_section["text"],
                                "metadata": chunk_current_section,
                        }
        docs.append(doc)
        sections.append(current_section)
        chunk_sections.append(chunk_current_section)
    
    return {"sections":sections, "chunk_sections": chunk_sections,  "documents": docs, "reporting_period_startdate": reporting_period_startdate, "reporting_period_enddate": reporting_period_enddate, "product_name": product_name, "site_name": site_name}

# COMMAND ----------

header = "REP-0234010 v1.0 Status: Approved Approved Date: 07 Feb 2024 Page 4 of 105"
header_pattern = r'REP-\d{7}\sv\d{1}.\d{1}\sStatus: Approved Approved Date:\s\d{1,2}\s\w+\s\d{4}\sPage\s\d{1,3}\sof\s\d{1,3}'
footer_pattern = r'check this is the latest version of the document before use.'
match = re.search(header_pattern, header)
if match: print("totototo")

    

# COMMAND ----------

def ingest_pqr_file(tmp_pqr_pdf_path):

    with pdfplumber.open(tmp_pqr_pdf_path) as pdf:
        # Get the first page
        first_page = pdf.pages[0]
        # Extract text from the first page
        first_page_text = first_page.extract_text()
        return extract_reporting_period_product_name_site_name(first_page_text, date_pattern, site_names, product_names)

# COMMAND ----------

def Ingest_phase_1(upload_folder, pqr_file):
    tmp_pqr_pdf_path = f"{upload_folder}/{pqr_file }"        
    reporting_period_startdate, reporting_period_enddate, product_name, site_name = ingest_pqr_file(tmp_pqr_pdf_path)
    reporting_period = reporting_period_startdate + "_" +  reporting_period_enddate

#### create the directory structure if not exist: reporting_period/product_name 
    input_folder_pqr_reporting_period = f"{input_folder}/{product_name}"
    input_folder_pqr_reporting_period_product_name = f"{input_folder}/{product_name}/{reporting_period}"
#print(input_folder_pqr_reporting_period_product_name )
    check_create_dir(input_folder_pqr_reporting_period)
    check_create_dir(input_folder_pqr_reporting_period_product_name)

#### move file from tmp to reporting_period/product_name 
    shutil.move(tmp_pqr_pdf_path, f"{input_folder_pqr_reporting_period_product_name}/{pqr_file}")
    return reporting_period, product_name, site_name

# COMMAND ----------

##### Pick the files a dedicated/created input directory, chunked, embbed and save them in a Vector DataBase
def Ingest_phase_2(product_name, reporting_period, site_name, pqr_file_name):
     filename_to_proceed = f"{input_folder}/{product_name}/{reporting_period}/{pqr_file_name}" 
     output_path_images_tables = f"{input_folder}/{product_name}/{reporting_period}"             
     #### if the doc link (pdf, docx, txt, doc) was already saved in Pinecone, then ignore, otherwise insert.
     if (pinecone_document_noexists(pqr_file_name, reporting_period, product_name, site_name, INDEX_NAME)== 1):
        if(pqr_file_name.endswith(".pdf")): 
            sec_doc = extract_text_tables_images_by_sections(filename_to_proceed, section_names, reporting_period, product_name, pqr_file_name, site_name, output_path_images_tables, 1024, 1500)
            return sec_doc
           
            #section_chunks = section_chunks  + sec_doc["sections"]
                          
        else:
             if single_filename.endswith(".docx"):
                section_chunks = section_chunks + (ingest_docx(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name))                   
        #pinecone_insert_docs(sec_doc["documents"], INDEX_NAME)
    


# COMMAND ----------

def save_chunks_to_json(chunks, json_filename):
    """Save the chunked sections to a JSON file."""
    tmp_file = "/tmp/" + json_filename
    output_path = f"{output_folder}/{json_filename}"
    with open(tmp_file, "w") as json_file:
        json.dump(chunks, json_file)
    shutil.move(tmp_file, output_path)

# COMMAND ----------

#return {"sections":sections, "chunk_sections": chunk_sections,  "documents": docs, "reporting_period_startdate": reporting_period_startdate, "reporting_period_enddate": #reporting_period_enddate, "product_name": product_name, "site_name": site_name}
sec_doc = Ingest_phase_2("Fasenra", "14Nov2022_13Nov2023", "fmc", "Sample Source document - FMC.pdf")

# COMMAND ----------

sec_doc["chunk_sections"]

# COMMAND ----------

##### Pick the files from tmp directory and save in a dedicated/created input directory
def Ingest_PQR(upload_folder):
    list_of_upload_files = get_list_of_files(upload_folder)
    sections = []
    section_chunks = []
    if len(list_of_upload_files) > 1:
       for pqr_file_name in list_of_upload_files:
            if pqr_file_name.endswith(".pdf") or pqr_file_name.endswith(".docx"):
              reporting_period, product_name, site_name = Ingest_phase_1(upload_folder, pqr_file_name)
              sec_doc = Ingest_phase_2(product_name, reporting_period, site_name, pqr_file_name)
           
              sections = sections + sec_doc["sections"]
              section_chunks = section_chunks  + sec_doc["chunk_sections"]

              pinecone_insert_docs(sec_doc["documents"], INDEX_NAME)
           
    ispr_json_filename = "ispr" + "-" + product_name + "-" + reporting_period + ".json"
    ispr_chunk_json_filename = "ispr_chunk" + "-" + product_name + "-" +reporting_period + ".json"
    
    save_chunks_to_json(sections, ispr_json_filename)
    save_chunks_to_json(section_chunks, ispr_chunk_json_filename)

# COMMAND ----------

Ingest_PQR(upload_folder)

# COMMAND ----------

# Display the results
for section in section_chunks:
    if section['section_name'] == 'summary and conclusion':
       print(f"Section: {section['section_name']}")
       print(f"File Name: {section['file_name']}")
       print(f"Reporting Period: {section['reporting_period']}")
       print(f"Product Name: {section['product_name']}")
       print(f"Site Name: {section['site_name']}")
       #print(f"Content: {section['text'][:500]}...")  # Print first 500 characters for preview
       print(f"Content: {section['text']}...")  # Print first 500 characters for preview
       print("\n--- End of Section ---\n")

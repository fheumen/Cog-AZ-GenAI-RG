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
from unstructured.partition.pdf import partition_pdf
from unstructured.documents.elements import NarrativeText, Title, ListItem
import re
import pdfplumber
from docx import Document
from docx.shared import Inches
import os
from PIL import Image
import io
import shutil
##########################################
from langchain.embeddings.openai import OpenAIEmbeddings
#from langchain_openai import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI
from pinecone import Pinecone
from langchain.docstore.document import Document
import openai

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
        vector=[0] * 1536,
        top_k=1,
        include_metadata=True,
        #namespace=base_domain,
        filter={"file_name": file_name,
                "reporting_period": reporting_period,
                'product_name': product_name, 
                'site_name': site_name, 
                },
    )

    #time.sleep(30)

    # Return True (0) if the id was not found, False otherwise
    if len(results["matches"]) > 0:
        return 0
    else:
        return 1
    # return results

# COMMAND ----------

#### Insert content of an url into Pinecone
def pinecone_insert_docs(documents, index_name):
    from langchain.vectorstores import Pinecone
    from langchain.embeddings.openai import OpenAIEmbeddings

    
    index = pinecone.Index(index_name)    

    print(f"Going to insert {len(documents)} to Pinecone")
    #embeddings = client.embeddings(model=deployment_name)
    embeddings = OpenAIEmbeddings(#openai_api_key=openai.api_key, 
                              model=deployment_name,  
                              openai_api_base=openai.api_base, 
                              openai_api_key=openai.api_key,
                              openai_api_type = openai.api_type,
                              max_retries=10
                              )


    Pinecone.from_documents(
        documents,
        embeddings,
        index_name= index_name
        #namespace=base_domain,
    )
    #Pinecone.flush()
    #time.sleep(30)
    
    # Pinecone.from_documents(documents, embeddings, index_name=INDEX_NAME)
    print("****** Added to Pinecone vectorstore vector")

# COMMAND ----------

def extract_text_tables_images_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename, site_name, output_dir,
                                           chunk_min, chunk_max):
    """
    Extract text, tables, and images from a PDF based on a given list of section names.
    
    Args:
        pdf_path (str): Path to the input PDF file.
        section_names (list): A list of section names to chunk the PDF by.
        output_dir (str): Directory to save extracted images.
        
    Returns:
        list: A list of dictionaries, each containing 'section_name', 'text', 'images', and 'tables'.
    """
    
    # Normalize section names for matching
    normalized_section_names = [name.lower() for name in section_names]
    
    # Initialize data structure
    sections = []
    docs = [] ### list of document object for Vector Database
    current_section = {
                        "section_name": None,
                        "file_name": single_filename,
                        "reporting_period": reporting_period,
                        "product_name": product_name,
                        "site_name": site_name, 
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
            
            # Split text into lines for matching section names
            lines = text.split("\n")
            for line in lines:
                matched_section = match_section(line)
                if matched_section:
                    # Save the previous section
                    if current_section["section_name"]:
                        doc = Document(
                                page_content=current_section["text"],
                                metadata=current_section,
                                )
                        docs.append(doc)
                        sections.append(current_section)
                    
                    # Start a new section
                    current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period": reporting_period,
                        "product_name": product_name,
                        "site_name": site_name, 
                        "page_num": page_num,
                        "images": [],
                        "tables": [],                        
                        "text": ""
                    }
                
                # Add the line text to the current section if inside a section
                if current_section["section_name"]:
                    # check if the size of a chunk text into a section is within the chunk size
                    if (len(current_section["text"] + line.strip()) > chunk_max) and (len(current_section["text"]) > chunk_min):
                         # Save a section chunk
                        current_section_tmp = current_section.copy()
                        current_section_tmp["text"] += line.strip() + "\n"
                        ################ create a doc object                        
                        doc = Document(
                                page_content=current_section_tmp["text"],
                                metadata=current_section_tmp,
                                )
                        docs.append(doc)
                        sections.append(current_section_tmp)

                        current_section["text"] = ""
                        #print(sections)
                                       
                    current_section["text"] += line.strip() + "\n"
            
            # Extract tables
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    current_section["tables"].append(table)
            
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
                    print("titititititti")
                    print(image_filename)
                    image_path =  f"{output_dir}/{image_filename}"
                    tmp_file = "/tmp/"+ image_filename
                    image_bytes.save(image_path , "PNG")
                    #shutil.move(tmp_file, image_path )
                    current_section["images"].append(image_path)
    
    # Append the last section
    if current_section["section_name"]:
        doc = Document(
                                page_content=current_section_tmp["text"],
                                metadata=current_section_tmp,
                                )
        docs.append(doc)
        sections.append(current_section)
    
    return {"sections":sections, "documents": docs}

# COMMAND ----------

normalized_section_names = [name.lower() for name in section_names]

def match_section(text):
        normalized_text = ((text.strip().lower().split(' '))[1]).strip()
        print(normalized_text )
        return next((name for name in normalized_section_names if normalized_text == name ), None)

block_text = "1   SUMMARY AND CONCLUSION"  
a = ((block_text.strip().lower().split(' '))[0]).strip()
a.isdigit()
(" ".join((block_text.strip().lower().split(' '))[1:])).strip()
#matched_section = match_section(block_text)
#if matched_section:
#    print('Fabrice')
print(range(2, 4 , 1))

    

# COMMAND ----------

def get_site_name(file_name):  
    return ((((file_name.split('-'))[1]).split('.'))[0]).strip().lower()


# COMMAND ----------

def ingest_pdf(pdf_path, section_names, reporting_period, product_name, single_filename, site_name, output_dir, chunk_min, chunk_max):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
    extract_text_tables_images_by_sections
    #section_chunks = chunk_pdf_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename,  site_name)
    section_chunks = extract_text_tables_images_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename,  site_name, output_dir, chunk_min, chunk_max)
    return section_chunks
    

# COMMAND ----------

def ingest_docx(filename):

# COMMAND ----------

# Dictionary to hold chunks
section_chunks = []
input_folder = f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save
print(input_folder)
list_of_reporting_period = get_list_of_files(input_folder)

if len(list_of_reporting_period) > 0:
######## iSRS Extration of docx documents
    #logging.info('Extraction Job Started...')
    ###
    for reporting_period in list_of_reporting_period:
        list_of_product_name = get_list_of_files(f"{input_folder}/{reporting_period}")
        print(f"{input_folder}/{reporting_period}")

        for product_name in list_of_product_name: 
            list_of_files = get_list_of_files(f"{input_folder}/{reporting_period}/{product_name}")
            print(f"{input_folder}/{reporting_period}/{product_name}")
            output_path_images = f"{input_folder}/{reporting_period}/{product_name}"
            for single_filename in list_of_files:
                filename_to_proceed = f"{input_folder}/{reporting_period}/{product_name}/{single_filename}"
                print(f"{input_folder}/{reporting_period}/{product_name}/{single_filename}")
                site_name = get_site_name(single_filename)
              
                 #### if the doc link (pdf, docx, txt, doc) was already saved in Pinecone, then ignore, otherwise insert.
                if (pinecone_document_noexists(single_filename, reporting_period, product_name, site_name, INDEX_NAME)== 1):
                    try:
                        if(single_filename.endswith(".pdf")): 
                           sec_doc = ingest_pdf(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name, output_path_images, 1024, 1500)
                           section_chunks = section_chunks  + sec_doc["sections"]
                          
                        else: 
                            if single_filename.endswith(".docx"):
                              section_chunks = section_chunks + (ingest_docx(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name))
                   
                        pinecone_insert_docs(sec_doc["documents"], INDEX_NAME)

                    except:
                        continue
               
                

# COMMAND ----------

section_chunks[]

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

# COMMAND ----------


def save_chunks_to_json(chunks, output_path):
    """Save the chunked sections to a JSON file."""
    tmp_file = "/tmp/"+"pqr_section_chunks_minmax.json"
    with open(tmp_file, "w") as json_file:
        json.dump(chunks, json_file, indent=4)
    shutil.move(tmp_file, output_path)

# Save to a JSON file
output_path = f"{output_folder}/pqr_section_chunks_minmax.json"

save_chunks_to_json(section_chunks, output_path)

# COMMAND ----------

def ingest_pdf(pdf_path, section_names, reporting_period, product_name, single_filename, site_name):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
    
    section_chunks = chunk_pdf_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename,  site_name)
    return section_chunks

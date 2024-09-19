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
from unstructured.documents.elements import Title, NarrativeText, ListItem, Image, Table
import re
from pdf2image import convert_from_path

# COMMAND ----------

def get_site_name(file_name):  
    return ((((file_name.split('-'))[1]).split('.'))[0]).strip().lower()

# Function to detect if text is a header or footer based on patterns
def is_header_or_footer(text, common_patterns):
    for pattern in common_patterns:
        if re.search(pattern, text):
            return True
    return False

# Function to check if a section is part of the defined section names
def is_section_of_interest(text, section_patterns):
    for pattern in section_patterns:
        if re.search(pattern, text, re.IGNORECASE):  # Case-insensitive match
            return True
    return False

# Function to chunk the PDF by sections and only extract text, images, and tables
def chunk_pdf_by_sections_with_content(pdf_path, section_patterns, reporting_period, product_name, single_filename, site_name):

    # Open the PDF
    elements = partition_pdf(pdf_path)
    
    # partition the PDF file 
    chunks = []
    current_chunk = {"section_name": "", "content": []}
    extract_section = False  # Flag to indicate whether we're in a section of interest

    for element in elements:
        # Skip headers/footers by pattern detection
        #if isinstance(element, NarrativeText) and is_header_or_footer(element.text, common_patterns):
        #    continue

        if isinstance(element, Header) or isinstance(element, Footer):
            continue

        # Check for section titles that match our defined section names
        if isinstance(element, Title) and is_section_of_interest(element.text, section_patterns):
            if current_chunk["content"]:
                chunks.append(current_chunk)
            current_chunk = {"section_name": element.text,
                             "file_name": single_filename,
                             "reporting_period": reporting_period,
                             "product_name": product_name,
                             "site_name": site_name, 
                             #"page_num": element.metadata.get('page_number', None), 
                             "content": []}
            extract_section = True  # Start extracting the section

        # Extract text, images, and tables within the section of interest
        if extract_section:
            if isinstance(element, (NarrativeText, ListItem)):  # Extract text content
                current_chunk["content"].append({"type": "text", "text": element.text})
            elif isinstance(element, Image):  # Extract image content
                current_chunk["content"].append({"type": "image", "metadata": element.metadata})
            elif isinstance(element, Table):  # Extract table content
                current_chunk["content"].append({"type": "table", "text": element.text})

    # Add the last chunk if there is any content
    if current_chunk["content"]:
        chunks.append(current_chunk)

    return chunks

# COMMAND ----------

def ingest_pdf(pdf_path, section_patterns, reporting_period, product_name, single_filename, site_name):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
   
    section_chunks = chunk_pdf_by_sections_with_content(pdf_path, section_patterns, reporting_period, product_name, single_filename, site_name)
                     
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

            for single_filename in list_of_files:
               filename_to_proceed = f"{input_folder}/{reporting_period}/{product_name}/{single_filename}"
               print(f"{input_folder}/{reporting_period}/{product_name}/{single_filename}")
               site_name = get_site_name(single_filename)
               print(site_name)
               if(single_filename.endswith(".pdf")): 
                   print("tototottotototo")                  
                   section_chunks = section_chunks  + (ingest_pdf(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name))
               else: 
                   if single_filename.endswith(".docx"):
                      section_chunks = section_chunks + (ingest_docx(filename_to_proceed, section_names, reporting_period, product_name, single_filename, site_name))

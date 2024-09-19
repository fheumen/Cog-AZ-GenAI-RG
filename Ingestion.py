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

# COMMAND ----------

storageAccountName = os.environ.get("storageAccountName")
storageAccountAccessKey = os.environ.get("storageAccountAccessKey")
#storageAccountAccessKey = "KqDmutvYKFkUfibvOLivxyuYP+x9KqY3DblOGarDbgJjMBEbEfUn3bhEjyuNO+DUvgDdOcl2fgrX+AStRyyYDA=="
print(storageAccountAccessKey)
mount_blob_storage("inputs")
mount_blob_storage("outputs")

# COMMAND ----------

 # PyMuPDF
def chunk_pdf_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename, site_name):
    """
    Chunk a PDF based on a given list of section names.
    
    Args:
        pdf_path (str): Path to the input PDF.
        section_names (list): A list of section names to chunk the PDF by.
    
    Returns:
        list: A list of dictionaries, each containing 'section_name' and 'content'.
    """
    
    # Open the PDF
    doc = fitz.open(pdf_path)
    print(doc)
    # Dictionary to hold chunks
    sections = []
    current_section = None
    
    # Normalize section names to avoid case sensitivity issues
    normalized_section_names = [name.lower() for name in section_names]
    print(normalized_section_names)

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
    
    # Iterate through each page of the PDF
    for page_num in range(2, len(doc)):
        page = doc.load_page(page_num)
        blocks = page.get_text("dict")['blocks']
        
        for block in blocks:
            if 'lines' in block:
                block_text = ""
                for line in block['lines']:
                    for span in line['spans']:
                        block_text += span['text'] + " "
                        #print(block_text)
                # Check if the current block starts a new section
                matched_section = match_section(block_text)
                print("tototototototototototototototototototootottoottoto")
                print(block_text)
                print(block_text.split('  '))
                print("ttititititititititititititititititititititi")
                if matched_section:
                    # If a new section is found, save the previous section (if any)
                    if current_section and current_section["content"].strip():
                        sections.append(current_section)
                    
                    # Start a new section  reporting_period, product_name, single_filename
                    current_section = {
                        "section_name": matched_section,
                        "file_name": single_filename,
                        "reporting_period": reporting_period,
                        "product_name": product_name,
                        "site_name": site_name, 
                        "page_num": page_num,                        
                        "content": ""
                    }
                elif current_section:
                    # If inside a section, append text to the current section
                    current_section["content"] += block_text.strip() + "\n"

    # Append the last section if it exists
    if current_section and current_section["content"].strip():
        sections.append(current_section)

    return sections

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

def ingest_pdf(pdf_path, section_names, reporting_period, product_name, single_filename, site_name):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
    
    section_chunks = chunk_pdf_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename,  site_name)
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
                

# COMMAND ----------

section_chunks

# COMMAND ----------

# Display the results
for section in section_chunks:
    print(f"Section: {section['section_name']}")
    print(f"File Name: {section['file_name']}")
    print(f"Reporting Period: {section['reporting_period']}")
    print(f"Product Name: {section['product_name']}")
    print(f"Site Name: {section['site_name']}")
    print(f"Content: {section['content'][:500]}...")  # Print first 500 characters for preview
    print("\n--- End of Section ---\n")

# COMMAND ----------


def save_chunks_to_json(chunks, output_path):
    """Save the chunked sections to a JSON file."""
    tmp_file = "/tmp/"+"pqr_section_chunks.json"
    with open(tmp_file, "w") as json_file:
        json.dump(chunks, json_file, indent=4)
    shutil.move(tmp_file, output_path)

# Save to a JSON file
output_path = f"{output_folder}/pqr_section_chunks.json"

save_chunks_to_json(section_chunks, output_path)

# COMMAND ----------



# COMMAND ----------

def ingest_pdf(pdf_path, section_names, reporting_period, product_name, single_filename, site_name):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
    
    section_chunks = chunk_pdf_by_sections(pdf_path, section_names, reporting_period, product_name, single_filename,  site_name)
    return section_chunks

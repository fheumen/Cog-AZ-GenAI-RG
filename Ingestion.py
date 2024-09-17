# Databricks notebook source
from const import *
from unstructured.partition.pdf import partition_pdf

# COMMAND ----------

import fitz  # PyMuPDF

def chunk_pdf_by_sections(pdf_path, section_names):
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
    
    # Dictionary to hold chunks
    sections = []
    current_section = None
    
    # Normalize section names to avoid case sensitivity issues
    normalized_section_names = [name.lower() for name in section_names]

    # Function to find if a text matches any section name
    def match_section(text):
        normalized_text = text.strip().lower()
        return next((name for name in normalized_section_names if normalized_text.startswith(name)), None)
    
    # Iterate through each page of the PDF
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        blocks = page.get_text("dict")['blocks']
        
        for block in blocks:
            if 'lines' in block:
                block_text = ""
                for line in block['lines']:
                    for span in line['spans']:
                        block_text += span['text'] + " "

                # Check if the current block starts a new section
                matched_section = match_section(block_text)
                if matched_section:
                    # If a new section is found, save the previous section (if any)
                    if current_section and current_section["content"].strip():
                        sections.append(current_section)
                    
                    # Start a new section
                    current_section = {
                        "section_name": matched_section,
                        "file_name": pdf_path,
                        "site_name": pdf_path,
                        "product_name":,
                        "content": ""
                    }
                elif current_section:
                    # If inside a section, append text to the current section
                    current_section["content"] += block_text.strip() + "\n"

    # Append the last section if it exists
    if current_section and current_section["content"].strip():
        sections.append(current_section)

    return sections


# Example usage
pdf_path = "example.pdf"

# List of known section names to look for in the PDF
# section_names import from const.py

# Call the function to chunk the PDF by section names
section_chunks = chunk_pdf_by_sections(pdf_path, section_names)

# Display the results
for section in section_chunks:
    print(f"Section: {section['section_name']}")
    print(f"Content: {section['content'][:500]}...")  # Print first 500 characters for preview
    print("\n--- End of Section ---\n")

# COMMAND ----------



# COMMAND ----------

def ingest_pdf(pdf_path):
    #chunks = partition_pdf(filename=pqr_pdf,
    #                   chunking_strategy="by_title",
    #                  extract_images_in_pdf=True,
    #                  infer_table_structure=True)
    
    section_chunks = chunk_pdf_by_sections(pdf_path, section_names)
    return section_chunks
    

# COMMAND ----------

def ingest_docx(filename):

# COMMAND ----------

# Dictionary to hold chunks
section_chunks = []
list_of_files = get_list_of_files(input_folder)
if len(list_of_files) > 0:
######## iSRS Extration of docx documents
    #logging.info('Extraction Job Started...')
    ###
    for single_filename in list_of_files:
        filename_to_proceed = input_folder+single_filename
        if(single_filename.endswith(".pdf")):
           section_chunks = ingest_pdf(filename_to_proceed)
        else if(single_filename.endswith(".docx")):
            section_chunks = ingest_docx(filename_to_proceed)

# COMMAND ----------

# Display the results
for section in section_chunks:
    print(f"Section: {section['section_name']}")
    print(f"Content: {section['content'][:500]}...")  # Print first 500 characters for preview
    print("\n--- End of Section ---\n")

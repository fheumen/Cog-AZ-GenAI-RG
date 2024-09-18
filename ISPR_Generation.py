# Databricks notebook source
from docx import Document
import pandas as pd
from const import *

# COMMAND ----------

output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save
output_path_json = f"{output_folder}/pqr_section_chunks.json"
# Read the JSON file into a DataFrame

# COMMAND ----------

df = pd.read_json(output_path_json)
df["site_name"] = df["site_name"].str.strip().str.lower()
# Display the DataFrame
subset = df.loc[(df['section_name'] == "other") & (df['site_name'] == "fmc"), ['page_num', 'section_name', 'content']]

print(subset)

# COMMAND ----------

document = Document()
for section_name in section_names:
  for site_name in site_names:
    # Subsetting specific rows and columns by labels
    document.add_heading(name, 1)
    subset = df.loc[(df['section_name'] == section_name) & (df['site_name'] == site_name), ['page_num', 'section_name', 'content']]


    document.add_paragraph('Alice in Wonderland is a 2010 American dark fantasy period film directed by Tim Burton from a screenplay written by Linda Woolverton. \n')
document.add_paragraph('The film stars Mia Wasikowska in the title role, with Johnny Depp, Anne Hathaway, Helena Bonham Carter, Crispin Glover, and Matt Lucas, and features the voices of Alan Rickman, Stephen Fry, Michael Sheen, and Timothy Spall.')
document.add_paragraph('Alice in Wonderland was produced by Walt Disney Pictures and shot in the United Kingdom and the United States. ')
document.add_paragraph('The film premiered in London at the Odeon Leicester Square on February 25, 2010.')
document.save('alice-in-wonderland.docx')

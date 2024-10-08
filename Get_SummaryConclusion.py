# Databricks notebook source
import pymongo

# COMMAND ----------

# MAGIC %run ../Utils/StorageAccountAccess_Secrets

# COMMAND ----------

# MAGIC %run ../Utils/setup_env

# COMMAND ----------

# MAGIC %run ./Extract_SumCon_Raw

# COMMAND ----------

secret_scope = "nlp-toolbox_storage_account"
basepath = mount_blob_storage(secret_scope)

input_folder = dbutils.secrets.get(secret_scope, "input_folder")
output_folder = dbutils.secrets.get(secret_scope, "output_folder")

# COMMAND ----------

database_name = "NLP-Toolbox-Dev"
conn_str = "mongodb://nlptoolbox-mongodb-dev:5n3T5XXcikBVNmmYQp9Ai273CevsbI8O3zqRacvyVUl6MM9OgjgGf8hhpDrc3SVlRrV4uYPq6JPrACDbcYhiRw%3D%3D@nlptoolbox-mongodb-dev.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@nlptoolbox-mongodb-dev@"
collection_name = "Documents"

# COMMAND ----------

import shutil
import docx
import os

list_of_files = get_list_of_files(input_folder)
df_result = pd.DataFrame(columns=["_id", "Filename", "Summary", "Conclusion"])

if len(list_of_files) > 1:
    ######## iSRS Extration of docx documents
    # logging.info('Extraction Job Started...')
    ###
    for single_filename in list_of_files:
        print(single_filename)
        if single_filename.endswith(".docx"):
            filename_to_proceed = input_folder + single_filename
            try:
                doc = docx.Document(filename_to_proceed)
            except ValueError:
                continue
            df_result = pd.concat(
                [df_result, Extract_Sum_and_Concl(doc, single_filename)]
            )
            # df_result = df_result.append(Extract_Sum_and_Concl(doc, single_filename), ignore_index = True)
            # os.remove(filename_to_proceed)

    bulk_write_dataframe(conn_str, database_name, collection_name, df_result)

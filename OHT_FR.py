# Databricks notebook source
# MAGIC %run ../Utils/setup_env

# COMMAND ----------

# MAGIC %run ../Utils/StorageAccountAccess_Secrets

# COMMAND ----------

# MAGIC %run ../Utils/FormRecognizer_Tools

# COMMAND ----------

import pandas as pd

# COMMAND ----------

secret_scope = "nlp-toolbox_storage_account"
basepath=mount_blob_storage(secret_scope)
secret_scope_dollar="dollar"
in_folder =dbutils.secrets.get(secret_scope_dollar,"input_folder")
out_folder = dbutils.secrets.get(secret_scope_dollar,"output_folder")
archiv_folder = dbutils.secrets.get(secret_scope_dollar,"archiv_folder")
#conn_str=dbutils.secrets.get(scope = "nlp-toolbox_mongodb", key = "conn_str")

database_name = "NLP-Toolbox-Dev"
conn_str = "mongodb://nlptoolbox-mongodb-dev:5n3T5XXcikBVNmmYQp9Ai273CevsbI8O3zqRacvyVUl6MM9OgjgGf8hhpDrc3SVlRrV4uYPq6JPrACDbcYhiRw%3D%3D@nlptoolbox-mongodb-dev.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@nlptoolbox-mongodb-dev@"
collection_name ='Documents'

stage="Dev"

database_name = "NLP-Toolbox-Dev"
collection_name = "Documents"

# COMMAND ----------

from pymongo import MongoClient, UpdateOne
client = MongoClient(conn_str)
db = client[database_name]
collection = db[collection_name]

# retrieve _id values of documents where DT_NAME = 'Study Report'
query = {'DT_NAME': 'Study Report'}
projection = {'_id': 1}
result = collection.find(query, projection)

# extract the _id values into a list
id_list = [doc['_id'] for doc in result]

# print the list of _id values
print(id_list)

# COMMAND ----------

list_of_files = list(set(get_list_of_files(in_folder)).intersection(id_list))

# COMMAND ----------

list_of_files

# COMMAND ----------

list_of_files = get_list_of_files(in_folder)

# COMMAND ----------

in_folder

# COMMAND ----------

import time
import os 
list_of_titles = []
list_of_test_facility = []
list_of_analyzed_files=[]
list_of_needed_time=[]
list_of_ED_NO=[]
result_all = { }

for single_filename in list_of_files:
    start_time = time.time()
    
    if(single_filename.endswith(".pdf")):
        print(f'Analyzing {single_filename}')
        
        filename = in_folder+single_filename
        try:
            result=get_result(filename, "oht_poc_test")
            #title=dict_result.get("Title").value
            #test_facility=dict_result.get("TestFacillity").value
        except Exception as err:
            print(err)
            #title = err
            #test_facility="Error"
        
        result_all[single_filename] = result
        #list_of_titles.append(title)
        #list_of_test_facility.append(test_facility)
        #list_of_ED_NO = single_filename.replace('.pdf', '')
        list_of_analyzed_files.append(single_filename)
        
        end_time = time.time()
        
        elapsed_time = end_time-start_time
        
        list_of_needed_time.append(elapsed_time)


        # delete the file
        os.remove(filename)

# COMMAND ----------

for table_idx, table in enumerate(result_all[filename].tables):  
    print(  
        "Table # {} has {} rows and {} columns".format(  
        table_idx, table.row_count, table.column_count  
        )  
    ) 

# COMMAND ----------

field_list = ["01_AD_StudyStartEndDate", "02_DataSource", "03_MM_TestGuidL", "03_MM_TestMaterial", "03_MM_Method_SpeciesStrain", "03_MM_Method_Controls", 
              "03_MM_TestAnimals", "03_MMAdmExp", "03_MM_AdmExp", "03_MM_Examinations", "03_MM_Method_RestInformations", "06_SummaryConclusion"]


for filename in list_of_files:

    print("--------Analyzing document #{}--------".format(filename))
    print("Document has type {}".format(result_all[filename].documents[0].doc_type))
    print("Document has confidence {}".format(result_all[filename].documents[0].confidence))
    print("Document was analyzed by model with ID {}".format(result_all[filename].model_id))

    for field in field_list:
        #print(field)
        #print(filename)

        #for name, field in result_all[filename].documents[0].fields.items():
            #field_value = field.value if field.value else field.content
            #print("###############################################")
            #print(field)
            #print("......found field of type '{}' with value '{}' and with confidence {}".format(field.value_type, field_value, field.confidence))

        entry = result_all[filename].documents[0].fields.get(field)
        #print(entry.value)
           # result_all[list_of_files[0]].documents[0].fields.get(field_list[0])
        if entry:
           print("###############################################")
           print(field)
           print("###############################################") 
           print(entry.value)
           #dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('StartDate').value
           #dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('EndDate').value
           #dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('Remarks').value

    
#dict_result.get("03_MM_TestGuidL")
#dict_result.get("01_AD_StudyStartEndDate")

#dict_result_ = result["M-832492-01-1.pdf"].documents[0].fields
##########################


# COMMAND ----------

#list_of_analyzed_files = list_of_files.remove('HalloWelt.txt')
#list_of_analyzed_files
list_of_files

# COMMAND ----------

#toto['value'][0]['value']["Version/Remarks"]
result_all[filename].documents[0].fields.get(field).to_dict()

# COMMAND ----------

result_all[filename].documents[0].fields.get("01_AD_StudyStartEndDate").to_dict()

# COMMAND ----------

# dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('StartDate').value
# dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('EndDate').value
# dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('Remarks').value

list_field = ["Guideline", "Version/Remarks"]
field = "03_MM_TestGuidL"

result_str = {"03_MM_TestGuidL": ["Guideline", "Version/Remarks"],
              "01_AD_StudyStartEndDate": ["StartDate", "EndDate", "Remarks"]
              }

result_values = { }
result_values["Filename"] = []
result_values["Fields"] = []
result_values["Features"] = []
result_values["Values"] = []

field_list_ = ["01_AD_StudyStartEndDate", "03_MM_TestGuidL"]

for filename in list_of_files:
    print("########################################")
    print(filename)
    for field in field_list:
        entry = result_all[filename].documents[0].fields.get(field)
        #print(entry.value)
           # result_all[list_of_files[0]].documents[0].fields.get(field_list[0])
        if entry:        
            fields_dic = entry.to_dict()       
            for i in range(len(fields_dic["value"])):            
                for j in range(len(result_str[field])):
                    #if i == 0: 
                        #result_values[filename + "_" + field + "_" + result_str[field][j]] = []
                    try:  
                      field_val = fields_dic["value"][i]["value"][result_str[field][j]]['value']
                      #print(field)
                      #print(result_str[field][j])
                      #print(field_val)
                      result_values["Filename"].append(filename)
                      #print(result_values)
                      result_values["Fields"].append(field)
                      result_values["Features"].append(result_str[field][j])
                      result_values["Values"].append(field_val)
                      #result_values[filename + "_" + field + "_" + result_str[field][j]].append(field_val)   

                    except Exception as err:
                      err = err
                   
            #print(result_values)       
#print(result_values)
# toto['value'][0]['value'].["Version/Remarks"]

# COMMAND ----------

result_values
pd.DataFrame.from_dict(result_values)

# COMMAND ----------

#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('StartDate').value
#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('EndDate').value
#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('Remarks').value

# COMMAND ----------

result_all[list_of_files[0]].documents[0].fields.get(field_list[0])

# COMMAND ----------

field_list = ["Guideline", "Version/Remarks"]
df = pd.DataFrame(columns=field_list)


for idx, invoice in enumerate(dict_result.get("03_MM_TestGuidL").value):
    single_df = pd.DataFrame(columns=field_list)

    for field in field_list:
      entry = invoice.fields.get(field)
        
    if entry:
        single_df[field] = [entry.value]
          
    single_df['FileName'] = blob.name
    df = df.append(single_df)

df = df.reset_index(drop=True)
df

############################################################################
#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('StartDate').value
#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('EndDate').value
#dict_result.get("01_AD_StudyStartEndDate").value.get('Values').value.get('Remarks').value

# COMMAND ----------

dict_result.tables[0].to_dict()

# COMMAND ----------

"""
This code sample shows Custom Extraction Model operations with the Azure Form Recognizer client library. 
The async versions of the samples require Python 3.6 or later.

To learn more, please visit the documentation - Quickstart: Form Recognizer Python client library SDKs
https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-python-sdk
"""

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

"""
Remember to remove the key from your code when you're done, and never post it publicly. For production, use
secure methods to store and access your credentials. For more information, see 
https://docs.microsoft.com/en-us/azure/cognitive-services/cognitive-services-security?tabs=command-line%2Ccsharp#environment-variables-and-application-configuration
"""
endpoint = "546058c28c6344b09fe626c5bb62cbe8"
key = "https://nlptoolboxformrecognizer.cognitiveservices.azure.com/"

model_id = "YOUR_CUSTOM_BUILT_MODEL_ID"
formUrl = "YOUR_DOCUMENT"

document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint, credential=AzureKeyCredential(key)
)

# Make sure your document's type is included in the list of document types the custom model can analyze
poller = document_analysis_client.begin_analyze_document_from_url(model_id, formUrl)
result = poller.result()

for idx, document in enumerate(result.documents):
    print("--------Analyzing document #{}--------".format(idx + 1))
    print("Document has type {}".format(document.doc_type))
    print("Document has confidence {}".format(document.confidence))
    print("Document was analyzed by model with ID {}".format(result.model_id))
    for name, field in document.fields.items():
        field_value = field.value if field.value else field.content
        print("......found field of type '{}' with value '{}' and with confidence {}".format(field.value_type, field_value, field.confidence))


# iterate over tables, lines, and selection marks on each page
for page in result.pages:
    print("\nLines found on page {}".format(page.page_number))
    for line in page.lines:
        print("...Line '{}'".format(line.content.encode('utf-8')))
    for word in page.words:
        print(
            "...Word '{}' has a confidence of {}".format(
                word.content.encode('utf-8'), word.confidence
            )
        )
    for selection_mark in page.selection_marks:
        print(
            "...Selection mark is '{}' and has a confidence of {}".format(
                selection_mark.state, selection_mark.confidence
            )
        )

for i, table in enumerate(result.tables):
    print("\nTable {} can be found on page:".format(i + 1))
    for region in table.bounding_regions:
        print("...{}".format(i + 1, region.page_number))
    for cell in table.cells:
        print(
            "...Cell[{}][{}] has content '{}'".format(
                cell.row_index, cell.column_index, cell.content.encode('utf-8')
            )
        )
print("-----------------------------------")


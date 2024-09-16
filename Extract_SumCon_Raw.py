# Databricks notebook source
import os
import zipfile
#other tools useful in extracting the information from our document
import re
#to pretty print our xml:
import xml.dom.minidom
import pandas as pd
import docx
import logging
import time
from docx.table import _Cell
from docx2python import docx2python
from docx.document import Document
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.shared import Length, Pt
from copy import deepcopy
import glob
import os
import csv
from docx.oxml.ns import qn
from openpyxl import workbook #pip install openpyxl
from openpyxl import load_workbook
from datetime import datetime
#from win32com.client import Dispatch

# COMMAND ----------

def IS_Summary (para, title_text = "summary"):
    
    
    if len(re.findall(title_text, para.text.lower())) > 0: 
         
        ########### "Summary" als Titel
        tst = para.text.split('\t') 
        if (len(tst) == 1 and len(tst[0]) <= 20) or (len(tst) == 2
                                                                 and len(re.findall(title_text ,tst[1].lower())) > 0 
                                                                 and len(tst[1].lower()) <= 20):
        
            return True
        
        ########## Executive Summary als Titel
       # if len(re.findall("executive", para.text.lower())) > 0: 
           # return True
        
    return False

# COMMAND ----------

def IS_Conclusion (para):
    
    if (len(re.findall("Conclusion", para.text)) > 0 \
                                        or len(re.findall("CONCLUSION", para.text)))> 0:
        return True
        
    return False

# COMMAND ----------

def IS_StopExtraction(para):   
    if ('Heading' in  para.style.name) or  ("Überschrift" in  para.style.name) or ("titre" in  para.style.name.lower()) \
       or ("title" in  para.style.name.lower()) or ("level" in  para.style.name.lower()) or ("gliederung" in  para.style.name.lower()) \
       or re.search("^\d+\.*\d*\s+\w+", para.text) is not None: 
        return True
    else: return False

# COMMAND ----------

#def Extract_Summary(parent, title_text = "summary", font_name = 'Times New Roman', font_size_text = 11, font_size_table = 10): 
def Extract_Sum_and_Concl(parent, Filename, keywords = ["summary"]): 

    #### Initalise flag variable, helping to identify of the "Text" to extract into the document
    #global flag 
    
    flag = 0
    flg = 0
    summary = ""
    conclusion = ""
    
    
    #### Get the content of all the docx document: Text Paragraphs + Tables
    if isinstance(parent, Document):
        parent_elm = parent.element.body
    elif isinstance(parent, _Cell):
        parent_elm = parent._tc
    else:
        raise ValueError("something's not right")

    ##### Going through the entire Document page per page   
    for child in parent_elm.iterchildren():      
    
        
        ################### 1. If the Text is a Paragraph  ###################            
        if isinstance(child, CT_P):
            para = Paragraph(child, parent)
            
            ##### Identification of the beginning of the text to be extracted:
            if IS_Summary(para, keywords[0]) and flag == 0:  
                flag = 1 
            
            ##### Identification of the body of the text to be extracted till the key Word "Conclusion"
            elif flag == 1: 
                flag = 2  
                summary = " ".join([summary, para.text.strip()]) 
                
                
            elif IS_Conclusion(para) and  flag == 2:                    
                    flag = 3
                    conclusion= " ".join([conclusion, para.text.strip()]) 
            
            elif flag ==2 and IS_StopExtraction(para): 
                    flag = -1
                    break
                    
            elif flag == 2 : summary = " ".join([summary, para.text.strip()])             
                         
            
            elif IS_StopExtraction(para) and  flag == 3: 
                    flag = -1
                    break
                    
            elif flag == 3: conclusion= " ".join([conclusion, para.text.strip()])   
   
    return pd.DataFrame.from_dict({"_id":[Filename], "Filename":[Filename], "Summary": [summary], "Conclusion":[conclusion]})
              

# COMMAND ----------



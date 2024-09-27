# Databricks notebook source
import os
import shutil
from dotenv import load_dotenv
load_dotenv()

# COMMAND ----------

################# Mount Repository Databrick Specific
def mount_blob_storage(blobContainerName):
    if not any(mount.mountPoint == f"/mnt/FileStore/{blobContainerName}/" for mount in dbutils.fs.mounts()):
      try:
        dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = f"/mnt/FileStore/{blobContainerName}/",
        extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey }
    )
      except Exception as e:
        print("already mounted. Try to unmount first")
#####################################

def file_exists(path):
  if path.startswith("/dbfs/"):
    path=path.replace("/dbfs/","dbfs:/")
  
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    return False

      
def get_list_of_files(path):
  arr = os.listdir(path)
  return arr



def check_create_dir(path):
  if not os.path.isdir(path):
    os.mkdir(path)

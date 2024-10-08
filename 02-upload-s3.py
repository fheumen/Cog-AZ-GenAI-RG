import os
import boto3

### instantiate client
client = boto3.client("s3")

# set variables
bucket = "s3-az-reportgen-bucket"

cur_path = os.getcwd()
absolut_path_dir = os.path.join(cur_path, "inputs_upload")

for filename in os.listdir(absolut_path_dir):
    absolut_path = os.path.join(cur_path, "inputs_upload", filename)

    ## open the file
    target_filename = "tmp/" + filename

    ## load data into S3
    client.upload_file(absolut_path, bucket, target_filename)

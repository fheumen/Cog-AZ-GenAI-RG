import boto3
import docx
import json
import time
from pathlib import Path
from boto3.dynamodb.conditions import Attr
from io import BytesIO
import pandas as pd
import pdfplumber
from typing import Optional, List, Dict
import PyPDF2
from loguru import logger
from typing import Optional, List, Dict
from datetime import datetime
import fitz
from PIL import Image
import io

# Document Handling - docx
from docx.image.exceptions import UnrecognizedImageError
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.run import Run
from docx.shared import Inches
from docx import Document as Document_docx

s3 = boto3.client("s3")
textract_client = boto3.client('textract', region_name='us-east-1')
bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

# Bedrock model ID
BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

def s3_file_exists(bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise
            
def start_textract_analysis(s3_bucket, s3_object_key):
    response = textract_client.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': s3_bucket, 'Name': s3_object_key}},
        FeatureTypes=['LAYOUT']
    )
    return response['JobId']


def get_textract_output(job_id):
    job_status = 'IN_PROGRESS'
    while job_status == 'IN_PROGRESS':
        time.sleep(5)
        response = textract_client.get_document_analysis(JobId=job_id)
        job_status = response['JobStatus']
        print(f"Textract job status: {job_status}")

    if job_status != 'SUCCEEDED':
        print(f"Textract job {job_id} failed.")
        return None

    blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract_client.get_document_analysis(JobId=job_id)
        blocks.extend(response['Blocks'])
        next_token = response.get('NextToken')
        if not next_token:
            break
    return {'Blocks': blocks}


def group_blocks_by_page(textract_data):
    """Groups Textract blocks by their page number."""
    page_map = {}
    for block in textract_data:
        if 'Page' in block:
            page = block['Page']
            if page not in page_map:
                page_map[page] = []
            page_map[page].append(block)
    return page_map


def get_bedrock_coordinates(textract_data, user_prompt):
    prompt = f"""
You are an expert at parsing Amazon Textract layout data.

Task:
- Extract bounding boxes and page numbers for content that matches the user request.
- Only consider BLOCK_TYPE 'FIGURE', 'TABLE', 'PARAGRAPH'.
- If nothing matches, return an empty list [].

User request: "{user_prompt}"
Textract Data: {json.dumps(textract_data)}
Expected JSON output: [{{"type":"TABLE","page":2,"bounding_box":{{"Left":0.1,"Top":0.2,"Width":0.3,"Height":0.4}}}}]
"""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": prompt}]
    })

    try:
        response = bedrock_client.invoke_model(
            body=body,
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_body = json.loads(response.get('body').read())
        llm_response_text = response_body.get('content')[0].get('text')

        start_index = llm_response_text.find('[')
        end_index = llm_response_text.rfind(']') + 1
        if start_index == -1 or end_index == -1:
            return None
        return json.loads(llm_response_text[start_index:end_index])
    except Exception as e:
        print(f"Error invoking Bedrock: {e}")
        return None


def combine_bounding_boxes(elements):
    page_boxes = {}
    for element in elements:
        page = element['page']
        box = element['bounding_box']
        if page not in page_boxes:
            page_boxes[page] = dict(box)
        else:
            existing = page_boxes[page]
            left = min(existing['Left'], box['Left'])
            top = min(existing['Top'], box['Top'])
            right = max(existing['Left'] + existing['Width'], box['Left'] + box['Width'])
            bottom = max(existing['Top'] + existing['Height'], box['Top'] + box['Height'])
            page_boxes[page] = {
                'Left': left,
                'Top': top,
                'Width': right - left,
                'Height': bottom - top
            }
    return page_boxes


def crop_and_save_image_to_s3(bucket_name, pdf_s3_key, page_number, coordinates, s3_destination_folder, placeholder_key):
    try:

        logger.info(f"Starting image extraction from PDF: {pdf_s3_key}")

        if not s3_file_exists(bucket_name, pdf_s3_key):
            raise FileNotFoundError(f"S3 object not found: {pdf_s3_key}")
    
        s3_object = s3.get_object(Bucket=bucket_name, Key=pdf_s3_key)
        pdf_bytes = s3_object['Body'].read()
    
        uploaded_keys = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        result_img_extrac = {}

        # doc = fitz.open(pdf_path)
        page = doc.load_page(page_number - 1)

        zoom = 300 / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=matrix)
        # img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img = Image.open(io.BytesIO(pix.tobytes("png")))

        width, height = img.size
        left = coordinates['Left'] * width
        upper = coordinates['Top'] * height
        right = (coordinates['Left'] + coordinates['Width']) * width
        lower = (coordinates['Top'] + coordinates['Height']) * height
        right = right + 12

        cropped_img = img.crop((left, upper, right, lower))

        image_key = f"{s3_destination_folder}{placeholder_key}.png"
        # Upload cropped image to S3
        buffer = io.BytesIO()
        cropped_img.save(buffer, format="PNG")
        buffer.seek(0)

        s3.upload_fileobj(buffer, bucket_name, image_key)
        uploaded_keys.append(image_key)

        # cropped_img.save(output_path, 'PNG')
        logger.info(f"Uploaded image saved to s3://{bucket_name}/{image_key}")
        # print(f"Image saved locally: {output_path}")
        return image_key
    except Exception as e:
        print(f"Error cropping image: {e}")
        return None
    finally:
        if 'doc' in locals():
            doc.close()


def upload_to_s3(local_file_path, s3_bucket, s3_key):
    try:
        s3.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"Uploaded {local_file_path} → s3://{s3_bucket}/{s3_key}")
        return f"s3://{s3_bucket}/{s3_key}"
    except ClientError as e:
        print(f"S3 upload failed: {e}")
        return None


# ----------------------------------------------------
#  Remove the boundaries after the integration. 
# ----------------------------------------------------
def extract_figure_from_s3_pdf(
    bucket_name: str,
    pdf_s3_key: str,
    search_text: str,
    placeholder_key: str,
    s3_destination_folder: str
    # left_margin: float | None = None,
    # above_title: float | None = None,
    # right_margin: float | None = None,
    # bottom_margin: float | None = None,
    # output_s3_location: str | None = None,
) -> str | None:
    # s3_bucket_in, s3_key_in = None, None
    # tmp_pdf_path, tmp_image_path = None, None

    # # Parse input S3
    # if s3_file_location.startswith("s3://"):
    #     parts = s3_file_location[5:].split("/", 1)
    #     if len(parts) == 2:
    #         s3_bucket_in, s3_key_in = parts
    #     else:
    #         print(f"Invalid S3 input: {s3_file_location}")
    #         return None
    # else:
    #     print("Input must be S3 URI")
    #     return None

    # try:
    #     # Download PDF locally
    #     with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
    #         tmp_pdf_path = tmp_pdf.name
    #     s3.download_file(s3_bucket_in, s3_key_in, tmp_pdf_path)

        # Textract
        job_id = start_textract_analysis(bucket_name, pdf_s3_key)
        textract_output = get_textract_output(job_id)
        if not textract_output:
            return None

        # Process
        page_map = group_blocks_by_page(textract_output['Blocks'])
        for page_number, blocks in page_map.items():
            found = get_bedrock_coordinates(blocks, search_text)
            if not found:
                continue
            page_boxes = combine_bounding_boxes(found)
            if page_number not in page_boxes:
                continue

            combined_box = page_boxes[page_number]
            # with NamedTemporaryFile(delete=False, suffix=".png") as tmp_img:
            #     tmp_image_path = tmp_img.name
            # local_path = crop_and_save_image_locally(tmp_pdf_path, page_number, combined_box, tmp_image_path)
            img_s3_key = crop_and_save_image_to_s3(bucket_name, pdf_s3_key, page_number, combined_box, s3_destination_folder, placeholder_key)
            if not img_s3_key:
                return None
            else:
                return img_s3_key

            # # Upload to S3 if output provided
            # if output_s3_location and output_s3_location.startswith("s3://"):
            #     parts = output_s3_location[5:].split("/", 1)
            #     s3_bucket_out, s3_key_out = parts
            #     return upload_to_s3(local_path, s3_bucket_out, s3_key_out)
            # else:
            #     print("No output S3 specified. Returning local path.")
            #     return local_path

        print("No matching content found.")
        return None

    # finally:
    #     if tmp_pdf_path and os.path.exists(tmp_pdf_path):
    #         os.remove(tmp_pdf_path)
    #     if tmp_image_path and os.path.exists(tmp_image_path):
    #         os.remove(tmp_image_path)

def llm_extract_images_and_upload(
        rules_fig,
        pdf_s3_key_list: List[str],
        s3_destination_folder: str,
        bucket_name: str
        # coordinates_dict: dict
    ):
    """
    Extracts and uploads cropped images from a PDF stored in S3 based on label search and 
    optional custom cropping coordinates.

    For each placeholder and its associated figure label(s), the function scans the PDF 
    page-by-page to locate figure captions, crops the image below the caption (by default), 
    or based on provided coordinates (if available), and uploads the cropped image(s) to S3.

    Args:
        rules_fig (dict): Mapping placeholder keys (e.g., "<Figure 1:>") to lists of figure labels 
                          to search for in the PDF (e.g., ["Figure 3"]).
        pdf_s3_key (str): S3 key (path) of the PDF file from which to extract images.
        s3_destination_folder (str): Path prefix in the S3 bucket where cropped images will be uploaded.
        bucket_name (str): Name of the S3 bucket.
        coordinates_dict (dict): Mapping each placeholder to optional cropping coordinates.
                                 Format:
                                 {
                                     "<Figure 1:>": {"x0": float, "y0": float, "x1": float, "y1": float},
                                     ...
                                 }
                                 Missing or incomplete coordinates trigger default cropping logic.

    Returns:
        dict: Maps each placeholder to a list of S3 keys of the uploaded images, e.g.:
              {
                  "<Figure 1:>": ["uploads/figure1_image1.png"],
                  "<Figure 2:>": ["uploads/figure2_img1.png", "uploads/figure2_img2.png"]
              }

    Notes:
        - Coordinates missing or incomplete for a placeholder result in default cropping below the caption.
        - Cropped images smaller than 150x100 pixels are skipped.
        - Only images from the first matching label per placeholder are retained.
    """


    # logger.info(f"Starting image extraction from PDF: {pdf_s3_key}")

    # if not s3_file_exists(bucket_name, pdf_s3_key):
    #     raise FileNotFoundError(f"S3 object not found: {pdf_s3_key}")

    # s3_object = s3.get_object(Bucket=bucket_name, Key=pdf_s3_key)
    # pdf_bytes = s3_object['Body'].read()

    uploaded_keys = []
    # doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    result_img_extrac = {}

    # logger.info(f"PDF loaded successfully with {len(doc)} pages.")
    # logger.debug(f"Received coordinates dictionary: {coordinates_dict}")
    uploaded_keys = []
    result_img_extrac = {}
    i = 0

    for placeholder_key, search_prompt_text in rules_fig.items():
        logger.info(f"Starting image extraction from PDF: {pdf_s3_key_list[i]}")

        if not s3_file_exists(bucket_name, pdf_s3_key_list[i]):
            raise FileNotFoundError(f"S3 object not found: {pdf_s3_key_list[i]}")
    
        s3_object = s3.get_object(Bucket=bucket_name, Key=pdf_s3_key_list[i])
        pdf_bytes = s3_object['Body'].read()

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        logger.info(f"Processing placeholder: '{placeholder_key}'")
        image_key = extract_figure_from_s3_pdf(bucket_name, pdf_s3_key_list[i], search_prompt_text, placeholder_key, s3_destination_folder)
        if image_key:
            uploaded_keys.append(image_key)
            result_img_extrac[placeholder_key] = uploaded_keys
            logger.info(f"Uploaded image for placeholder '{placeholder_key}' to s3://{bucket_name}/{image_key}")       
        uploaded_keys = []
        logger.info(f"Completed image extraction and upload for PDF: {pdf_s3_key_list[i]}")
        i = i + 1
        doc.close()
    # logger.info(f"Completed image extraction and upload for PDF: {pdf_s3_key}")
    return result_img_extrac

###########For Testing
# rules_fig = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 1"]}
# pdf_s3_key_list = ["tmp_test5/MAIN_(PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor.pdf", "tmp_test5/VX-730126-PVP B633 Rilvegostomig (AZD2936) Process Validation Production Bioreactor and Harvest.pdf"]
# s3_destination_folder = "temp_kklc575/tmp_test5/final_reports/"
# bucket_name = "azcdi-us-ops-report-ds-dev"
# rules_fig = {"<Figure 1:>": ["Extract the Figure 1 along with the table that follow the image in the document"],  "<Figure 2:>": ["Extract Figure 2 along with the table that follow the image in the document"]}
# print(llm_extract_images_and_upload(rules_fig, pdf_s3_key_list , s3_destination_folder, bucket_name))
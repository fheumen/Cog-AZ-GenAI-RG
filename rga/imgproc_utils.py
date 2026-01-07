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

def s3_file_exists(bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def extract_images_and_upload(
        rules_fig,
        pdf_s3_key: str,
        s3_destination_folder: str,
        bucket_name: str,
        coordinates_dict: dict
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


    logger.info(f"Starting image extraction from PDF: {pdf_s3_key}")

    if not s3_file_exists(bucket_name, pdf_s3_key):
        raise FileNotFoundError(f"S3 object not found: {pdf_s3_key}")

    s3_object = s3.get_object(Bucket=bucket_name, Key=pdf_s3_key)
    pdf_bytes = s3_object['Body'].read()

    uploaded_keys = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    result_img_extrac = {}

    logger.info(f"PDF loaded successfully with {len(doc)} pages.")
    logger.debug(f"Received coordinates dictionary: {coordinates_dict}")

    for placeholder_key, figures in rules_fig.items():
        logger.info(f"Processing placeholder: '{placeholder_key}' with associated labels: {figures}")

        for label in figures:
            logger.info(f"Scanning for label: '{label}'")
            found = False

            for page_num in range(len(doc)):
                page = doc[page_num]
                logger.info(f"Searching for label '{label}' on page {page_num}")

                caption_boxes = page.search_for(label)
                if not caption_boxes:
                    logger.info(f"Label '{label}' not found on page {page_num}")
                    continue

                caption_box = caption_boxes[0]
                logger.info(f"Found label '{label}' at position {caption_box} on page {page_num}")

                try:
                    # Render full page image at higher resolution
                    zoom = 2.5
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)
                    pil_image = Image.open(io.BytesIO(pix.tobytes("png")))

                    page_width = page.rect.width
                    page_height = page.rect.height

                    # Default cropping logic (area below caption)
                    fig_x = 0
                    fig_y = max(0, caption_box.y1 + 5)
                    fig_w = page_width
                    footer_height = 60
                    fig_h = max(50, min(page_height - fig_y - footer_height, page_height - fig_y))

                    # Attempt to override with coordinates
                    coords = coordinates_dict.get(placeholder_key)
                    logger.info(f"Retrieved coordinates for placeholder '{placeholder_key}': {coords}")

                    if coords is None:
                        logger.warning(f"No coordinates found in coordinates_dict for placeholder: '{placeholder_key}'")
                    elif not all(coords.get(k) is not None for k in ["x0", "y0", "x1", "y1"]):
                        missing_keys = [k for k in ["x0", "y0", "x1", "y1"] if coords.get(k) is None]
                        logger.warning(f"Incomplete coordinates for placeholder '{placeholder_key}'; missing keys: {missing_keys}")
                    else:
                        # Coordinates are valid — use custom crop area
                        x0 = max(0, float(coords["x0"]))
                        y0 = max(0, float(coords["y0"]))
                        x1 = min(page_width, float(coords["x1"]))
                        y1 = min(page_height, float(coords["y1"]))

                        fig_x = x0
                        fig_y = y0
                        fig_w = x1 - x0
                        fig_h = y1 - y0

                        logger.info(f"Using custom cropping for label '{label}': x0={x0}, y0={y0}, x1={x1}, y1={y1}")

                    # Convert crop coordinates from PDF units to image space
                    img_x = int(fig_x * zoom)
                    img_y = int(fig_y * zoom)
                    img_w = int(fig_w * zoom)
                    img_h = int(fig_h * zoom)

                    cropped = pil_image.crop((img_x, img_y, img_x + img_w, img_y + img_h))

                    # Skip images that are too small to be useful
                    if cropped.width < 150 or cropped.height < 100:
                        logger.warning(f"Skipped small image for label '{label}' with size: {cropped.size}")
                        continue

                    fig_number = "".join(filter(str.isdigit, label)) or "unknown"
                    image_key = f"{s3_destination_folder}{placeholder_key}_figure_{fig_number}.png"

                    # Upload cropped image to S3
                    buffer = io.BytesIO()
                    cropped.save(buffer, format="PNG")
                    buffer.seek(0)

                    s3.upload_fileobj(buffer, bucket_name, image_key)
                    uploaded_keys.append(image_key)

                    logger.info(f"Uploaded image for label '{label}' to s3://{bucket_name}/{image_key}")
                    found = True
                    break  # Stop after processing the first valid match

                except Exception as e:
                    logger.error(f"Error processing label '{label}' on page {page_num}: {e}", exc_info=True)

            if not found:
                logger.warning(f"No matching image found or uploaded for label: '{label}'")

        result_img_extrac[placeholder_key] = uploaded_keys
        uploaded_keys = []

    doc.close()
    logger.info(f"Completed image extraction and upload for PDF: {pdf_s3_key}")
    return result_img_extrac


###########For Testing
# rules_fig = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 3", "Figure 4", "Figure 5"]}
# pdf_s3_key = "tmp_test5/MAIN_(PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor.pdf"
# s3_destination_folder = "temp_kklc575/tmp_test5/final_reports"
# rules_fig = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 3", "Figure 4", "Figure 5"]}
# print(extract_images_and_upload(rules_fig, pdf_s3_key, s3_destination_folder))


######################################################################
def normalize_text(text: str) -> str:
    """
    Normalizes text for comparison:
    - Lowercase
    - Strips leading/trailing whitespace
    - Replaces non-breaking spaces (common in Word)
    """
    return text.replace('\xa0', ' ').strip().lower()


def _insert_image(doc, paragraph, image_bytes, width, placeholder_text) -> bool:
    """
    Helper function to insert an image immediately after the given paragraph.
    This handles Word's internal XML structures to place the new image correctly,
    and removes the placeholder text.
    """
    try:
        new_paragraph = doc.add_paragraph()
        run = new_paragraph.add_run()
        run.add_picture(image_bytes, width=width)

        parent = paragraph._element.getparent()
        idx_in_parent = parent.index(paragraph._element)
        parent.insert(idx_in_parent + 1, new_paragraph._element)

        # === Remove the placeholder text ===
        paragraph.text = paragraph.text.replace(placeholder_text, "")

        logger.info(f"Inserted image after placeholder '{placeholder_text}'.")
        return True
    except Exception as e:
        logger.error(f"Error inserting image after placeholder '{placeholder_text}': {e}")
        return False


# Commented out to accommodate placeholder replacement 
# def _insert_image(doc, paragraph, image_bytes, width, idx_label) -> bool:
#     """
#     Helper function to insert an image immediately after the given paragraph.
#     This handles Word's internal XML structures to place the new image correctly.
#     """
#     try:
#         new_paragraph = doc.add_paragraph()
#         run = new_paragraph.add_run()
#         run.add_picture(image_bytes, width=width)

#         parent = paragraph._element.getparent()
#         idx_in_parent = parent.index(paragraph._element)
#         parent.insert(idx_in_parent + 1, new_paragraph._element)

#         logger.info(f"Inserted image after paragraph {idx_label}.")
#         return True
#     except Exception as e:
#         logger.error(f"Error inserting image after paragraph {idx_label}: {e}")
#         return False

# Commented out to remove the placeholder : 19th August
# def insert_image_after_paragraph(doc: Document_docx, marker_text: str, image_bytes: io.BytesIO, width=Inches(6)) -> bool:
#     """
#     Searches for a placeholder string in paragraphs or tables,
#     and inserts the given image immediately after the matched paragraph.
#     """
#     if not image_bytes or image_bytes.getbuffer().nbytes == 0:
#         logger.warning("Image is empty or invalid.")
#         return False

#     logger.info(f"Image is NON empty: {image_bytes.getbuffer().nbytes}")
#     image_bytes.seek(0)
#     norm_marker = normalize_text(marker_text)

#     # === 1. Search in standard paragraphs ===
#     for idx, paragraph in enumerate(doc.paragraphs):
#         norm_para = normalize_text(paragraph.text)
#         logger.debug(f"[Paragraph {idx}] '{paragraph.text}'")

#         if norm_marker in norm_para:
#             return _insert_image(doc, paragraph, image_bytes, width, idx)

#     # === 2. Search in tables ===
#     for t_idx, table in enumerate(doc.tables):
#         for r_idx, row in enumerate(table.rows):
#             for c_idx, cell in enumerate(row.cells):
#                 for p_idx, paragraph in enumerate(cell.paragraphs):
#                     norm_para = normalize_text(paragraph.text)
#                     logger.debug(f"[Table {t_idx}][Row {r_idx}][Cell {c_idx}][Paragraph {p_idx}] '{paragraph.text}'")
#                     if norm_marker in norm_para:
#                         return _insert_image(doc, paragraph, image_bytes, width,
#                                                   f"Table {t_idx} Row {r_idx} Cell {c_idx} Para {p_idx}")

#     logger.warning(f"Marker text '{marker_text}' not found anywhere in the document.")
#     return False


def insert_image_after_paragraph(doc: Document_docx, marker_text: str, image_bytes: io.BytesIO, width=Inches(6)) -> bool:
    """
    Searches for a placeholder string in paragraphs or tables,
    and inserts the given image immediately after the matched paragraph.
    """
    if not image_bytes or image_bytes.getbuffer().nbytes == 0:
        logger.warning("Image is empty or invalid.")
        return False

    logger.info(f"Image is NON empty: {image_bytes.getbuffer().nbytes}")
    image_bytes.seek(0)
    norm_marker = normalize_text(marker_text)

    # === 1. Search in standard paragraphs ===
    for idx, paragraph in enumerate(doc.paragraphs):
        norm_para = normalize_text(paragraph.text)
        logger.debug(f"[Paragraph {idx}] '{paragraph.text}'")

        if norm_marker in norm_para:
            return _insert_image(doc, paragraph, image_bytes, width, marker_text)

    # === 2. Search in tables ===
    for t_idx, table in enumerate(doc.tables):
        for r_idx, row in enumerate(table.rows):
            for c_idx, cell in enumerate(row.cells):
                for p_idx, paragraph in enumerate(cell.paragraphs):
                    norm_para = normalize_text(paragraph.text)
                    logger.debug(f"[Table {t_idx}][Row {r_idx}][Cell {c_idx}][Paragraph {p_idx}] '{paragraph.text}'")
                    if norm_marker in norm_para:
                        return _insert_image(doc, paragraph, image_bytes, width, marker_text)

    logger.warning(f"Marker text '{marker_text}' not found anywhere in the document.")
    return False


# {'<Figure 1:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 1:>_figure_2.png'], '<Figure 2:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_3.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_4.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_5.png']}   
def sitev_insert_figure_in_report(
    bucket_name,
    rules_img,
    target_s3_key: str,
    docx_s3_key: str
):

    # Step 2: Load DOCX using python-docx
    try:
        response = s3.get_object(Bucket=bucket_name, Key=docx_s3_key)
        template_bytes = response['Body'].read()
        template_file = BytesIO(template_bytes)
        # doc = Document_docx(template_file)
        doc = Document_docx(template_file)
    except Exception as e:
        logger.error(f"Failed to open template DOCX: {e}")
        raise

    # Step 3: Loop over each mapping entry (each represents one figure/placeholder pair)
    # for mapping in mappings:
    for placeholder, img_path_list in rules_img.items():
        # placeholder = mapping["Place_holder"]
        # doc_id = mapping["Template_Name"]
        # figure_numbers = mapping["Figures"]

        for image_s3_key in img_path_list:
            possible_extensions = ['png', 'jpeg', 'jpg']
            image_bytes = None

            # Step 3.1: Try loading the image with various file extensions
            for ext in possible_extensions:
                logger.info(f"\n\nreport_generator: image_s3_key: {image_s3_key}")
                try:
                    image_bytes = BytesIO()
                    s3.download_fileobj(bucket_name, image_s3_key, image_bytes)
                    image_bytes.seek(0)
                    break  # Stop on first valid image
                except Exception:
                    continue  # Try next extension

            if not image_bytes:
                logger.warning(f"Image for Figure {image_s3_key} not found in S3.")
                continue

            logger.info(f"Inserting image_s3_key {image_s3_key}")
            inserted = insert_image_after_paragraph(doc, placeholder, image_bytes)
            logger.info(f"inserted result is: {inserted}")

            if not inserted:
                logger.warning(f"Failed to insert image for placeholder {placeholder}")

   
    # Step 5: Upload final report back to S3
    try:
        doc_stream = io.BytesIO()
        doc.save(doc_stream)
        doc_stream.seek(0)
        s3.upload_fileobj(doc_stream, bucket_name, target_s3_key)
        
    except Exception as e:
        logger.error(f"Failed to upload final report to S3: {e}")
        raise
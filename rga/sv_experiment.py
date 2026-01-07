import boto3
import docx
import io
# from io import BytesIO
import msal
import json
import requests
from urllib.parse import quote, unquote, urlparse
import urllib.parse
import time
import re
from pathlib import Path
from boto3.dynamodb.conditions import Attr
from docx import Document as Document_docx
from docx.shared import Inches
from dateutil import parser
from io import BytesIO
import pandas as pd
import pdfplumber
from langchain_aws import ChatBedrock
from langchain_core.runnables.history import RunnableWithMessageHistory
from typing import Optional, List, Dict
import PyPDF2
from langchain.prompts import PromptTemplate
from fastapi import FastAPI, HTTPException, Response, UploadFile, status, File, Form
from langchain_core.chat_history import InMemoryChatMessageHistory
from docx.enum.text import WD_COLOR_INDEX
from loguru import logger
from typing import Optional, List, Dict
from datetime import datetime
from boto3.dynamodb.conditions import Attr, Key

# Document Handling - docx
from docx import Document as Document_docx
from docx.image.exceptions import UnrecognizedImageError
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.run import Run
from docx.shared import Inches
# from data import (IngestResult, IsprTrackingSection, IsprTrackingSubSection, IsprTrackingCompletion, QueryRequest, QnaAnswer, AnswerRequest, User, Query, RequestQuery, Citation, 
#     QuickReply, Result, QueryResponse, FeedbackDisplayOptions, Feedback, ChatInteraction, ChatMetadata, ChatHistorySearchRequest, FeedbackRequest, PqrTrackingStatus
# )

from data import *
# from utils import *

# from report_edition.utils import store_reporttrackingcompletion
HIGHLIGHT_COLOR = "green"
ISPR_KEYWORD = "ISPR"
S3_LIST_KEY = "Contents"

s3 = boto3.client("s3")
bucket_name = "azcdi-us-ops-report-ds-dev"

def store_reporttrackingcompletion(interaction: ReportTrackingCompletion, REPORTS_DYNAMOTABLE:str):
    """
    Store the ReportTrackingCompletion interaction in the REPORTS_DYNAMOTABLE DynamoDB table.

    This function performs the following steps:
    1. Delete any existing records with the same ProductName_User combination.
    2. Insert the new IsprTrackingCompletion interaction into the table.
    3. Invoked from update ispr and Generate ispr APIs.
    
    **Steps Carried Out by the Function:**

    1. Delete any existing records with the same `ProductName_User` combination from the DynamoDB table.
       - Query the table using the `ProductName_User-Timestamp-index` to retrieve existing records with the same `ProductName_User`.
       - Delete each retrieved record using the correct partition key (`SessionID`) and sort key (`Timestamp`).
    2. Insert the new `IsprTrackingCompletion` interaction into the DynamoDB table.
       - Convert the `IsprTrackingCompletion` object to a dictionary using the `dict()` method.
       - Insert the dictionary as a new item in the DynamoDB table using the `put_item()` method.

    **External Function Calls:**

    - `boto3.resource('dynamodb')`: Used to create a DynamoDB resource.
    - `table.query()`: Used to query the DynamoDB table with a specific index and condition expression.
    - `table.delete_item()`: Used to delete an item from the DynamoDB table based on the provided primary key.
    - `table.put_item()`: Used to insert a new item into the DynamoDB table.

    Args:
        interaction (IsprTrackingCompletion): The IsprTrackingCompletion interaction to be stored.
        tablename (str): The name of the DynamoDB table.

    Returns:
        dict: A success message if the interaction is stored successfully.

    Raises:
        HTTPException: If an exception occurs during the storage process.

    External Function Calls:
        - boto3.resource('dynamodb')
        - table.query()
        - table.delete_item()
        - table.put_item()
    """
    logger.info("\nstore_isprtrackingcompletion ---------- START --------")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(REPORTS_DYNAMOTABLE)
    ############## First delete all previous record from the same ProductName&UserID
    response = table.query(
                # IndexName = "report_id-file_version-index",
                KeyConditionExpression=Key('report_id').eq(interaction.report_id),
                FilterExpression=(Attr('file_version').eq(interaction.file_version)),
              )

    # Step 2: Delete existing records using correct keys
    for item in response.get('Items', []):
        table.delete_item(Key={
            "report_id": item["report_id"],  # Correct partition key
            "created_at": item["created_at"]   # Correct sort key
        })
    logger.info("store_reportcompletion ---------- Deleted --------")
    #################### Insert New Records
    try:
        item = interaction.dict()
        ############ delete all previous records about the interaction.ProductName_User
        table.put_item(Item=item)
        logger.info("store_reportcompletion ---------- END --------Report Edition interaction stored successfully")
        return {"message": "Report-Edition interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def sitev_extract_template(template_db, template_fullname):
    """
    Retrieves template metadata from DynamoDB based on the template's full name.

    Args:
        template_db (str): The name of the DynamoDB table containing template data.
        template_fullname (str): The full name of the template to search for.

    Returns:
        tuple: Template metadata including:
            - template_id (str)
            - s3_path (str or None)
            - s3_excel_path (str or None)
            - section_names (list or None)
            - template_name (str)
            - doc_types (list or None)
            - doc_types_full (list or None)

    Raises:
        HTTPException: If an error occurs during the DynamoDB operation.
    """
    logger.info("sitev_extract_template: Starting template lookup")

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(template_db)

    try:
        # Filter the DynamoDB table based on template_fullname
        response = table.scan(
            FilterExpression=Attr('template_fullname').eq(template_fullname)
        )

        res = response['Items'][0]
        logger.info("sitev_extract_template: Template found in DynamoDB")

        if ISPR_KEYWORD in template_fullname:
            logger.info("sitev_extract_template: Detected ISPR template")
            return res["template_id"], None, None, None, res["template_name"], None, None

        logger.info("sitev_extract_template: Returning standard template metadata")
        return (
            res["template_id"],
            res["s3_path"],
            res["s3_excel_path"],
            res["section_names"],
            res["template_name"],
            res["doc_types"],
            res["doc_types_full"]
        )

    except Exception as e:
        logger.error(f"sitev_extract_template: An unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again later.")


# # docx_s3_key, excel_s3_key, section_name_list = sitev_extract_template("aig-azcdi-us-ops-report-templates-dev", "InoculumExpansion")
# # # print(docx_s3_key)

# # #############################

# def send_email(mail_body, email_to, subject, email_function_name, attachment_s3_path=None):
#     # initialize counter
#     if not hasattr(send_email, "_call_count"):
#         send_email._call_count = 0
#     send_email._call_count += 1

#     # log every invocation, and highlight if >1
#     logger.info(f"[send_email] invocation #{send_email._call_count} → to={email_to!r}, subject={subject!r}")
#     if send_email._call_count > 1:
#         logger.warning(f"[send_email] called more than once in this run (#{send_email._call_count})")

#     client = boto3.client('lambda')
#     payload = {
#         'email_to': email_to,
#         'email_subject': subject,
#         'mail_body': mail_body,
#         'attachment_s3_path': attachment_s3_path
#     }

#     response = client.invoke(
#         FunctionName=email_function_name,
#         InvocationType='RequestResponse',
#         Payload=json.dumps(payload)
#     )
#     response_payload = json.loads(response['Payload'].read())
#     logger.info(f"[send_email] trigger response: {response_payload}")
#     return response_payload
    
# email_body_succeed = f"""
#                             Report Generation Completed Successfully
                           
#                             Report ID: ccccc
#                             File Version: ccccc
#                             Template: ccccc
#                             Status: Completed
#                             Completion Time: ccccc
#                             """
# subject_succeed = f"""Report Generation Succeed"""
# emailId="fabrice.heumenitientcheu@astrazeneca.com"
# email_sender = "aig-azcdi-us-ops-report-send-email"

# # send_email(email_body_succeed, emailId, subject_succeed, email_sender)

########################### Highlight text: START ###########################
def highlight_text(run, color=HIGHLIGHT_COLOR):
    """
    Highlights the given run with the specified color.
    """
    # Normalize color if it's an enum
    if isinstance(color, WD_COLOR_INDEX):
        color = color.name.lower()  # e.g., 'YELLOW' → 'yellow'
    elif not isinstance(color, str):
        color = str(color).lower()

    # Word only supports a limited set of highlight colors
    allowed_colors = {
        'yellow', 'green', 'cyan', 'magenta', 'blue', 'red',
        'gray', 'darkYellow', 'darkGreen', 'darkCyan', 'darkMagenta',
        'darkBlue', 'darkRed', 'darkGray', 'lightGray', 'black'
    }

    if color not in allowed_colors:
        color = 'yellow'  # fallback to a safe default

    highlight = OxmlElement('w:highlight')
    highlight.set(qn('w:val'), color)
    run._element.get_or_add_rPr().append(highlight)


def should_replace(placeholder, value):
    """
    Determines whether a placeholder should be replaced.
    
    Args:
        placeholder (str): The placeholder string.
        value (str): The value to potentially replace the placeholder with.
        
    Returns:
        bool: True if the placeholder should be replaced, False otherwise.
        
    The function excludes empty or 'unable' values from being used as replacements.
    """
    
    # Check if both placeholder and value are strings
    is_placeholder_str = isinstance(placeholder, str)
    is_value_str = isinstance(value, str)
    keywords = ["unable", "contains", "search"]
    
    # Check if value is non-empty and doesn't contain 'unable' (case-insensitive)
    value_valid = value.strip() and all(word.lower() not in value.lower() for word in keywords) 
    
    # Replace placeholder only if all conditions are met
    return is_placeholder_str and is_value_str and value_valid

# Define Word namespaces explicitly
NSMAP = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

def get_full_text(paragraph):
    """
    Concatenates and returns all run texts from the paragraph.
    """
    # logger.info("Collecting full paragraph text from runs.")
    # print(paragraph.text)
    # print(paragraph.style.name)
    if paragraph.style.name.startswith("table of") or paragraph.style.name.startswith("TOC"):
         print("totototottototototototototot")  
         print(paragraph.text)
         return paragraph.text
       
    else:
        return ''.join(run.text for run in paragraph.runs if run.text)


def build_segments(full_text, replacements):
    """
    Splits full_text into segments around placeholders.

    Each segment is a tuple:
        (text, is_highlighted, placeholder_key)

    Example:
        Input: "Hello <Name>!"
        Replacements: {"<Name>": "Alice"}

        Returns:
            [
                ("Hello ", False, None),
                ("Alice", True, "<Name>"),
                ("!", False, None)
            ]

    Args:
        full_text (str): The combined paragraph text.
        replacements (dict): Mapping of placeholders → replacement values.

    Returns:
        List[Tuple[str, bool, Optional[str]]]: A list of text segments.
    """

    if not full_text or not replacements:
        return [(full_text, False, None)]

    segments = []
    current_pos = 0

    # Match all placeholders using regex
    pattern = "(" + "|".join(map(re.escape, replacements.keys())) + ")"

    for match in re.finditer(pattern, full_text):
        start, end = match.start(), match.end()
        placeholder = match.group()

        # Text before match
        if current_pos < start:
            segments.append((full_text[current_pos:start], False, None))

        # Replacement or original placeholder
        replacement_value = replacements[placeholder]
        if should_replace(placeholder, replacement_value):
            segments.append((replacement_value, True, placeholder))
        else:
            segments.append((placeholder, False, None))

        current_pos = end

    # Remaining text
    if current_pos < len(full_text):
        segments.append((full_text[current_pos:], False, None))

    return segments

def add_segment_run(paragraph, text, is_highlighted, placeholder, segment_formatting, highlight_color):
    """
    Adds a text segment to the paragraph, preserving character formatting.

    Args:
        paragraph (docx.text.paragraph.Paragraph): Target paragraph.
        text (str): The text to insert.
        is_highlighted (bool): Whether to apply highlight to the segment.
        placeholder (str or None): Placeholder being replaced (for reference).
        segment_formatting (List[Dict]): Formatting info for each char.
        highlight_color (str): Word highlight color name (e.g., 'green', 'yellow').
    """
    for i, char in enumerate(text):
        run = paragraph.add_run(char)

        # Apply formatting
        fmt = segment_formatting[i] if i < len(segment_formatting) else None
        if fmt:
            run.bold = fmt["bold"]
            run.italic = fmt["italic"]

            # Safely apply underline (can be True, False, or style string)
            underline_value = fmt.get("underline")
            if underline_value is not None:
                run.underline = underline_value

            if run.font:
                run.font.name = fmt["font_name"]
                run.font.size = fmt["font_size"]
                if fmt["font_color"]:
                    run.font.color.rgb = fmt["font_color"]

        # Apply highlight
        if is_highlighted:
            highlight_text(run, highlight_color)
        elif fmt and fmt.get("highlight_color"):
            highlight_text(run, fmt["highlight_color"])
            
def clear_paragraph_runs(paragraph):
    """
    Safely clears all runs from the given paragraph.

    Args:
        paragraph (Paragraph): The paragraph object from which to clear runs.

    Returns:
        None

    """
    logger.info("Clearing original runs in paragraph.")
    # if paragraph.style.name.startswith("table of") or paragraph.style.name.startswith("TOC"):
    #     # paragraph.text = ""
    #     # for run in paragraph.runs:
    #     #     run.text = ""
    # else:
    for run in paragraph.runs:
        try:
            # Clear the run
            run.clear()
        except Exception as e:
            # Log any exceptions that occur during the clearing process
            logger.warning(f"Failed to clear run: {e}")


def extract_and_flatten_runs(paragraph):
    """
    Flattens all non-hyperlink runs into a single string and tracks
    the formatting of each character.

    Returns:
        - list of runs to clear later
        - full text string
        - list of (char, formatting_dict)
    """
    full_text = ''
    char_formatting = []

    normal_runs = [
        run for run in paragraph.runs
        if not run_in_hyperlink(run) and run._element.get(qn('w:dummy')) != 'true'
    ]

    for run in normal_runs:
        if not run.text:
            continue

        formatting = {
            "bold": run.bold,
            "italic": run.italic or (run.font.italic if run.font else False),
            "underline": run.underline,
            "font_name": run.font.name if run.font else None,
            "font_size": run.font.size if run.font else None,
            "font_color": run.font.color.rgb if run.font and run.font.color and run.font.color.rgb else None,
            "highlight_color": run.font.highlight_color if run.font and hasattr(run.font, 'highlight_color') else None
        }

        for char in run.text:
            full_text += char
            char_formatting.append((char, formatting))

    return normal_runs, full_text, char_formatting

def handle_hyperlink_placeholders(paragraph, replacements, highlight_color):
    """
    Scans and replaces placeholders inside hyperlink elements in a Word paragraph.

    This function ensures that placeholders embedded within hyperlinks (<w:hyperlink> tags)
    are correctly replaced with their corresponding values while preserving:
        - The original hyperlink structure and target (r:id or anchor)
        - Inline formatting and XML structure
        - Visual highlighting of the replaced value (for visibility or validation)

    ### How it works:

    1. **XPath and local-name() Usage**:
        - The function uses the XPath expression:
              './/*[local-name()="hyperlink"]'
        - This expression selects all `<w:hyperlink>` elements in the paragraph,
          regardless of their namespace (typically `w:` in WordprocessingML).
        - `local-name()` is a standard XPath function that extracts the tag name
          without the namespace prefix — making the query robust against variations
          in namespace declarations or parsing contexts.
        - This does **not** access the local file system — it's purely XML-based
          and operates in memory.

    2. **Text Replacement**:
        - For each `<w:hyperlink>` element, the function looks inside its child
          `<w:r>` (run) elements to find `<w:t>` (text) nodes.
        - If a placeholder (e.g., "<Document_Name>") exists in the text node,
          and its replacement value passes the `should_replace()` filter,
          the placeholder is replaced directly within the `<w:t>` element.

    3. **Highlighting Replaced Text**:
        - After replacing a placeholder with its value, the function calls
          `highlight_subtext_in_hyperlink()` to visually highlight the inserted
          text (e.g., using green or yellow background).
        - This highlighting works at the XML level and does not break the hyperlink.

    ### Parameters:
        paragraph (docx.text.paragraph.Paragraph):
            The Word paragraph object containing text and hyperlinks.

        replacements (dict):
            A dictionary mapping placeholder strings (e.g., "<Name>") to
            replacement values (e.g., "Alice").

        highlight_color (str):
            A Word-supported color name (e.g., "green", "yellow") used to
            highlight the replaced text inside the hyperlink.

    ### Notes:
        - If multiple placeholders exist inside the same hyperlink, only the
          first matching one is replaced due to the `break` logic.
        - The function only modifies hyperlinks containing placeholders;
          other hyperlinks are left untouched.
        - Hyperlinks without text nodes (`<w:t>`) or invalid structure are skipped silently.

    """

    for hyperlink_elem in paragraph._p.xpath('.//*[local-name()="hyperlink"]'):
        for run in hyperlink_elem.xpath('.//*[local-name()="r"]'):
            t_elems = run.xpath('.//*[local-name()="t"]')
            for t_elem in t_elems:
                if t_elem is None or not t_elem.text:
                    continue

                original_text = t_elem.text

                for placeholder, value in replacements.items():
                    if not should_replace(placeholder, value):
                        continue

                    if placeholder in original_text:
                        before, match, after = original_text.partition(placeholder)
                        t_elem.text = before + value + after

                        highlight_subtext_in_hyperlink(hyperlink_elem, value, highlight_color)
                        break
                        
def run_in_hyperlink(run):
    """
    Checks if the given run is inside a hyperlink tag.

    Args:
        run (docx.text.run.Run): The run to check.

    Returns:
        bool: True if inside a hyperlink, False otherwise.
    """
    parent = run._element.getparent()
    return parent.tag.endswith('hyperlink')

def rebuild_paragraph_runs(paragraph, segments, original_char_formatting, highlight_color):
    """
    Adds new runs to the paragraph based on segments with formatting.

    Args:
        paragraph: docx paragraph to rebuild
        segments: list of (text, is_highlighted, placeholder)
        original_char_formatting: list of (char, formatting_dict)
        highlight_color: highlight color to apply to replacements
    """
    pos = 0
    for text, is_highlighted, placeholder in segments:
        if not text:
            continue

        segment_formatting = []

        if placeholder:
            # Find position of the original placeholder in the flattened text
            placeholder_pos = ''.join(c for c, _ in original_char_formatting).find(placeholder, pos)
            fmt_sample = original_char_formatting[placeholder_pos][1] if placeholder_pos != -1 else None
            segment_formatting = [fmt_sample] * len(text)
            pos += len(placeholder)
        else:
            for _ in text:
                if pos < len(original_char_formatting):
                    _, fmt = original_char_formatting[pos]
                    segment_formatting.append(fmt)
                    pos += 1
                else:
                    segment_formatting.append(None)

        add_segment_run(paragraph, text, is_highlighted, placeholder, segment_formatting, highlight_color)

def highlight_subtext_in_hyperlink(hlink, substring, color_hex="00B050"):
    """
    Highlights a specific substring inside a <w:hyperlink> XML element.

    This preserves the hyperlink while highlighting only the target text,
    without affecting the rest of the content.

    Args:
        hlink (OxmlElement): The hyperlink XML element containing the text.
        substring (str): The substring to highlight.
        color_hex (str): The highlight color (Word-compatible color name or hex).

    Note:
        Word highlight colors must be names like 'green', not hex.
        This function handles internal Word XML runs directly.
    """
    ns = hlink.nsmap

    for r in hlink.xpath('.//w:r'):
        t = r.find('.//w:t', namespaces=ns)

        if t is None or not t.text or substring not in t.text:
            continue

        before, match, after = t.text.partition(substring)

        # Replace current run with "before"
        t.text = before if before else match

        if before and match:
            # Create a new run for the highlighted match
            match_run = OxmlElement("w:r")
            rPr = OxmlElement("w:rPr")
            highlight = OxmlElement("w:highlight")
            highlight.set(qn("w:val"), color_hex)
            rPr.append(highlight)

            match_t = OxmlElement("w:t")
            match_t.text = match

            match_run.append(rPr)
            match_run.append(match_t)

            r.addnext(match_run)
            r = match_run  # Move reference to match run

        elif not before:  # Match is at the beginning — highlight this run
            # rPr = r.find('w:rPr')
            rPr = r.find('w:rPr', namespaces=ns)

            if rPr is None:
                rPr = OxmlElement('w:rPr')
                r.insert(0, rPr)

            highlight = OxmlElement('w:highlight')
            highlight.set(qn('w:val'), color_hex)
            rPr.append(highlight)

        if after:
            # Create a new run for the "after" part
            after_run = OxmlElement("w:r")
            after_t = OxmlElement("w:t")
            after_t.text = after
            after_run.append(after_t)

            r.addnext(after_run)
    
def replace_and_highlight_paragraph(paragraph, replacements, highlight_color=HIGHLIGHT_COLOR):
    """
    Replaces placeholders in a paragraph with replacement values while:
    - Preserving character-level formatting
    - Highlighting replacements
    - Handling placeholders inside hyperlinks

    Args:
        paragraph: The docx paragraph to modify.
        replacements: Dict of placeholder → replacement value.
        highlight_color: Color name (string) for highlighting replaced text.
    """
    if not paragraph or not hasattr(paragraph, '_p'):
        logger.info("Invalid paragraph or missing XML.")
        return

    if not replacements or not isinstance(replacements, dict):
        logger.info("No valid replacements dictionary provided.")
        return

    has_text = any(run.text for run in paragraph.runs) or any(
        el.tag.endswith('hyperlink') for el in paragraph._p
    )
    if not has_text:
        return

    # Step 1: Replace placeholders inside hyperlinks
    handle_hyperlink_placeholders(paragraph, replacements, highlight_color)

    # Step 2: Flatten normal text and extract formatting
    normal_runs, full_text, original_char_formatting = extract_and_flatten_runs(paragraph)

    if not full_text.strip():
        return

    # Step 3: Build replacement segments (e.g. with highlighting info)
    segments = build_segments(full_text, replacements)
    if not any(seg[1] for seg in segments):  # No replacements
        return

    # Step 4: Clear old runs
    for run in normal_runs:
        try:
            run.clear()
        except Exception as e:
            logger.warning(f"Failed to clear run: {e}")

    # Step 5: Rebuild paragraph with replacements and formatting
    rebuild_paragraph_runs(paragraph, segments, original_char_formatting, highlight_color)



def clone_run_format(source_run: Run, new_run: Run):
    """
    Copies basic formatting from source_run to new_run.

    Args:
        source_run (Run): The Run object from which formatting will be copied.
        new_run (Run): The Run object to which formatting will be applied.

    """
    # logger.info("Cloning formatting from source run to new run.")
    try:
        # Check if both Run objects are valid
        if not source_run or not new_run:
            return

        # Copy basic formatting properties
        new_run.bold = source_run.bold
        new_run.italic = source_run.italic
        new_run.underline = source_run.underline

        # Check if font properties exist for both Run objects
        if hasattr(source_run, "font") and hasattr(new_run, "font"):
            # Copy font name if present in source_run
            if source_run.font.name:
                new_run.font.name = source_run.font.name

            # Copy font size if present in source_run
            if source_run.font.size:
                new_run.font.size = source_run.font.size

            # Copy font color if present in source_run
            if source_run.font.color and source_run.font.color.rgb:
                new_run.font.color.rgb = source_run.font.color.rgb

    except Exception as e:
        # Log any exceptions that occur during the formatting process
        logger.warning(f"Error in clone_run_format: {e}")
########################### Highlight text: END ###########################

########################### Site Validation ###########################
########################### Site Validation ###########################
def sitev_generate_report_from_template(s3_bucket, template_s3_key, output_s3_key, data=None, dframe_list=[], table_loc_list= []):
    """
    Generates a report from a Word document template stored in S3.

    Args:
        s3_bucket (str): The name of the S3 bucket.
        template_s3_key (str): The S3 key of the Word document template.
        output_s3_key (str): The S3 key where the generated report will be saved.
        data (dict): A dictionary containing the data to be inserted into the template.
    """
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=s3_bucket, Key=template_s3_key)
        template_bytes = response['Body'].read()
        template_file = BytesIO(template_bytes)
        doc = Document_docx(template_file)
    except Exception as e:
        logger.error(f"sitev_generate_report_from_template : Error downloading or opening template: {str(e)}")
    
    tables = doc.tables
    for key, value in data.items():
        # logger.info(f"\nsitev_generate_report_from_template ------ 1 ------placeholder / key: {key}")
        # logger.info(f"sitev_generate_report_from_template ------ 2 ------ key: {value}")
        placeholder = f"{key}"

        for paragraph in doc.paragraphs:
            if any(k in paragraph.text for k in data.keys()):
                # logger.info(f"sitev_generate_report_from_template:Processing paragraph: '{paragraph.text[:50]}'")
                replace_and_highlight_paragraph(paragraph, data)

        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    for paragraph in cell.paragraphs:
                        if any(k in paragraph.text for k in data.keys()):
                            # logger.info(f"sitev_generate_report_from_template: Processing table cell paragraph: '{paragraph.text[:50]}'")
                            replace_and_highlight_paragraph(paragraph, data)

        # for paragraph in doc.paragraphs:

        # # Replacing the text
        # # for key, value in data.items():
        #         # Earlier existing code
        #         # if "unable" in value.lower():
        #         #     continue
        #         # else:
        #         #     paragraph.text = paragraph.text.replace(placeholder, value)

        # for table in tables:
        #     logger.info(f"sitev_generate_report_from_template : Iterating tables: ----- 3 -----")
        #     for row in table.rows:
        #       for cell in row.cells:

        #          if placeholder in cell.text:
        #             # Earlier existing code
        #             # if "unable" in value.lower():
        #             #     continue
        #             # else:
        #             #     cell.text = value

    output_file = BytesIO()
    doc.save(output_file)
    output_file.seek(0)

    try:
        s3.put_object(Bucket=s3_bucket, Key=output_s3_key, Body=output_file.read())
        report_s3_uri = f"s3://{s3_bucket}/{output_s3_key}" #create s3 uri
        # logger.info(f"sitev_generate_report_from_template : Report generated and uploaded to \n{report_s3_uri}")
    except Exception as e:
        logger.error(f"sitev_generate_report_from_template : Error Saving the update template: {str(e)}")


# template_s3_key = "test_op/Report 8 - Resin Lifetime Process Validation Template _exp.docx"
# s3_bucket = "azcdi-us-ops-report-ds-dev"
# response = s3.get_object(Bucket=s3_bucket, Key=template_s3_key)
# template_bytes = response['Body'].read()
# template_file = BytesIO(template_bytes)
# doc = Document_docx(template_file)
    
# for paragraph in doc.paragraphs:
#     print(extract_and_flatten_runs(paragraph))



#########
# full_text = "The <Resin_Type> resin lifetime performance was evaluated for all <Molecule_Name> lots processed during the study period. There was (were) <Deviations_Number> deviation(s) that impacted process validation during the execution of the study (see Section 3.2). All validation acceptance criteria were met demonstrating that the <Chromatography_Type> chromatography column performed consistently through <Product_Contact_Cycle_Number> product contact cycles. This <Chromatography_Type> column resin lifetime study will be continued upon initiation of the next <Molecule_Name> campaign. "
# replacements = {"<Resin_Type>": "aaaaaaaaa", "<Molecule_Name>":"bbbbbbbbbbb", "<Chromatography_Type>": "cccccccccccccc"}
# segments = build_segments(full_text, replacements)

bucket_name = "azcdi-us-ops-report-ds-dev"
output_folder = "output_sv_2/"
template_name = "ResinLifetime"
user_id = "kkfxh"
# docx_s3_key = "test_op/Report 8 - Resin Lifetime Process Validation Template _exp.docx"
# template_name = "ResinLifetimeAbstr"
docx_s3_key = "inputs/sv_docx_templates/Report 8 - Resin Lifetime Process Validation Template.docx"
template_name = "ResinLifetimeAll"
ispr_filename = "sitev" + "-"  + template_name + "-"  + user_id + ".docx"
target_filename = f"{output_folder}{ispr_filename}"
data = {"<Resin_Type>": "MabSelect™ SuRe™ (Cytiva) Protein A", "<Molecule_Name>":"Tozorakimab", "<Chromatography_Type>": "Protein A", "<Building_Scale>": "15,000L", "<Molecule_Number>": "MEDI3506", "<Building_Number>": "B633" }
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data)

###############
target_filename = "output_sv_2/report 8 - experiment.docx"
ispr_filename_out = "sitev" + "-"  + template_name + "-"  + user_id + "oo" + ".docx"
target_filename_out = f"output_sv_2/{ispr_filename_out}"
deviation_change_values = {'<Deviations_Number>': '1', '<Change_Control_Number>': '1'}
data.update(deviation_change_values)
# sitev_generate_report_from_template(bucket_name, target_filename,  target_filename, data)


# data = {'<Deviations_Number>': str(3), '<Change_Control_Number>': str(3)}
# bucket_name = "azcdi-us-ops-report-ds-dev"
# docx_s3_key = "outputs/report_1_template_test.docx"
# target_filename = "outputs/report_1_template_test.docx"
# # # print(docx_s3_key)
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data)
  
# # output_folder = "outputs/"
# # template_name = "InoculumExpansion"
# # user_id = "kkfxh"
# # ispr_filename = "sitev" + "-"  + template_name + "-"  + user_id + ".docx"
# # target_filename = f"{output_folder}{ispr_filename}"
# # data = {"Validation Number": "VX-715101-PVP",\
# #               "Molecule Name": "Tozorakimab",\
# #               "Building": "633",\
# #               "Document Number":"PRO-0187549" 
# #              }
# # # print(docx_s3_key)
# # # sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data)


bedrock_client = boto3.client('bedrock-agent', region_name="us-east-1")
bedrock_agent_runtime = boto3.client(service_name="bedrock-agent-runtime")

def get_knowledge_base_id(kb_name, maxresult):
    
    knowledge_bases = []
    next_token = None
    
    while len(knowledge_bases) < maxresult:
        kwargs = {'maxResults': min(maxresult - len(knowledge_bases), maxresult)}
        if next_token:
            kwargs['nextToken'] = next_token

        response = bedrock_client.list_knowledge_bases(**kwargs)
        knowledge_bases.extend(response.get('knowledgeBaseSummaries', []))

        next_token = response.get('nextToken')
        if not next_token:
            break  # no more results
    
    for kb in  knowledge_bases:
        # print(kb['name'])
        if kb['name'] == kb_name:
            print(f"Knowledge base found: {kb['knowledgeBaseId']}")
            return kb['knowledgeBaseId']
    raise Exception(f"Knowledge base '{kb_name}' not found.")

# # get_knowledge_base_id("azcdi-us-ops-report-siteval-dev", 50) get_knowledge_base_id("azcdi-us-ops-report-siteval-dev", 50)
# ####################
# def get_mapping_list_sv(bucket_name, excel_file_path , az_mapping_sheet_name, kb_name, model_id):
#     #excel_file_path = f"{MAPPING_FILE_PATH}"
#     #az_mapping_sheet_name = "Mapping"

#     # Get the Excel file from S3
#     response = s3.get_object(Bucket=bucket_name, Key=excel_file_path)
#     excel_data = response['Body'].read()
#     df_mapping = pd.read_excel(io.BytesIO(excel_data), sheet_name=az_mapping_sheet_name)
#     #print(df_mapping["Place_holder"].isna())

#     place_holder_list_text  = df_mapping["Place_holder"].tolist()
#     print(place_holder_list_text)

#     # place_holder_list_text  = df_mapping[df_mapping["Place_Holder_Typ"]=="text"]["Place_holder"].tolist()
#     # place_holder_list_table = df_mapping[df_mapping["Place_Holder_Typ"] != "text"]["Place_holder"].tolist() ### ["Table 2:", "Table 3:"]
#     # place_proc_list_table = df_mapping[df_mapping["Place_Holder_Typ"] != "text"]["Place_Holder_Typ"].tolist() ### ["table_3", "table_4"]

#     rules_list_text = df_mapping["Rules"].tolist()
#     #print(rules_list_text)
#     # rules_list_table = df_mapping[df_mapping["Place_Holder_Typ"] != "text"]["Rules"].tolist()

#     output_list_text = [retrieve_and_generate(rul, kb_name, model_id) for rul in rules_list_text]
#     #print(output_list_text)
#     #print(output_list_text)
#     # output_list_table = [retrieve_and_generate(rul, kb_name, model_id) for rul in rules_list_table]
#     #retrieve_and_generate(rules_list, KNOWLEDGEBASE_NAME, MODEL_ID)

#     rule_dic_text = dict(zip(place_holder_list_text, output_list_text))

#     return rule_dic_text # output_list_table  #place_holder_list_table, place_proc_list_table 


def retrieve_and_generate(query: str, knowledge_base_name: str, model_id: str):

    kb_id = get_knowledge_base_id(knowledge_base_name, 150)

    try:        
        prompt_template = query
        prompt_template += f"""\n\n%ADDITIONAL INSTRUCTIONS%:\n Please provide concise answer and only the answer."""
        prompt_template += f"\n\n%USER QUERY:\n{query}\n"  
        ans = bedrock_agent_runtime.retrieve_and_generate(
            input={
                'text': prompt_template
            },
            retrieveAndGenerateConfiguration={                
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': kb_id,
                    # 'modelArn': MODEL_ARN,
                    'modelArn': model_id,
                    'retrievalConfiguration': {
                        'vectorSearchConfiguration': {
                            'numberOfResults': 3,
                            # 'overrideSearchType': QNA_SEARCH_TYPE,
                            # "filter":{
                            #     "andAll": [
                            #         {
                            #             "equals": {
                            #                       "key": "document_name", 
                            #                       "value":"1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor"
                            #                     }
                            #         },
                            #         {
                            #            "equals": {
                            #                       "key":"page_nr", 
                            #                       "value":0
                            #                      }
                            #         }
                            #       ]
                            #      },
                            "implicitFilterConfiguration": { 
                               "metadataAttributes": [ 
                                   { 
                                     "description": "document name",
                                     "key": "document_name",
                                     "type": "STRING"
                                   },
                                   { 
                                     "description": "type of the document",
                                     "key": "document_typ",
                                     "type": "STRING"
                                   },
                                   { 
                                      "description": "page number in the document",
                                      "key": "page_nr",
                                      "type": "NUMBER"
                                   }

                                ],
                                  'modelArn': model_id,
                                  

                        }
                    },
                    # "generationConfiguration": {
                    #     "guardrailConfiguration": {
                    #         "guardrailId": GUARDRAIL_ID,
                    #         "guardrailVersion": GUARDRAIL_VERSION_ID
                    #     },
                    #     "inferenceConfig": { 
                    #         "textInferenceConfig": { 
                    #             "maxTokens": int(QNA_MAX_TOKENS_VALUE),
                    #             "temperature": float(QNA_TEMPRATURE_VALUE),
                    #             "topP": float(QNA_TOP_P_VALUE)
                    #         }
                    #     }                                       
                    # },
                 },
                    #    'type': 'KNOWLEDGE_BASE'
             },
              'type': 'KNOWLEDGE_BASE'
            }
                # **({'sessionId': session_id} if session_id else {})  # Conditionally add 
       )
        return ans['output']['text']
    except Exception as e:
        raise Exception(f"Error in retrieving q&a answer: {e}")



# excel_file_path = 'inputs/sv_xlsx_templates/sv_xlsx_template_Report3_ProteinAChromatography_rules.xlsx'
# az_mapping_sheet_name = "Site_Validation"
kb_name = "azcdi-us-ops-report-siteval-test"
model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
# # #query = '""" Given the document with the name document_name:"1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor", give the Validation number present in the page "page_nr :0". """'
# query = """ give the Chromatography type mentioned in the document typ "MAIN", usually listed in page 0 after the string "Process Validation:". Only return the exact information prior to the word Chromatography with no conversational text."""

# query_summary = """ give the Chromatography type mentioned in the document "REPORT4", usually listed in page 0 after the string "Process Validation:". Only return the exact information of the second Chromatography type extracted prior to the word Chromatography with no conversational text."""

query_summary = """ give the Chromatography type mentioned in the document typ  "REPORT4", usually listed in page 0 after the string 'Process Validation:'X' Chromatography,'. Only return the exact information of the first Chromatography type extracted prior to the word Chromatography with no conversational text."""

query_summary = """ give the Chromatography type mentioned in the document typ "REPORT3", usually listed in page 0 after the string Process Validation:. Only return the exact information prior to the word Chromatography with no conversational text."""

query_summary = """ give the Chromatography type mentioned in the document typ "REPORT4", usually listed in page 0 after the string 'Process Validation:'X' Chromatography,'. Only return the exact information of the second Chromatography type extracted prior to the word Chromatography with no conversational text."""

query_summary = """ extract the second Chromatography type mentioned in the document typ "REPORT4", usually listed in page 0 in the Title after the string 'Process Validation:'. Find and return the resin type utilised in the body of the subsection under the 'Process Description' section of  document typ "PLAN" that contain the Chromatography type string extracted from   document typ "REPORT4". Extract and return all the words listed between the words '"utilizes" and "resin" in the identified subsection. Do not return the chromatography type. Only return the exact resin information without the word 'resin' and with no conversational text."""

query_summary = """ extract the second Chromatography type mentioned in the document typ "REPORT4", usually listed in page 0 in the Title after the string 'Process Validation:'. Find and return the resin type utilised in the body of the subsection under the 'Process Description' section of  document typ "PLAN" that contain the second Chromatography type string extracted from   document typ "REPORT4". Extract and return all the words listed between the words '"utilizes" and "resin" in the identified subsection. Do not return the chromatography type. Only return the exact resin information without the word 'resin' and with no conversational text."""

# answer=retrieve_and_generate(query_summary, kb_name, model_id)
# print(answer)

# # print(retrieve_and_generate(query, kb_name, model_id))
# # res = get_mapping_list_sv(bucket_name, excel_file_path , az_mapping_sheet_name, kb_name, model_id)
# # print(res)


# def replace_text_with_highlight(paragraph, placeholder, replacement):
#     """
#     Replaces a placeholder text with highlighted replacement text in a paragraph.
    
#     Args:
#         paragraph: The paragraph object to modify
#         placeholder: The placeholder text to find (e.g., "<Name>")
#         replacement: The replacement text to insert with highlighting
#     """
#     # If the paragraph doesn't contain the placeholder, do nothing
#     if placeholder not in paragraph.text:
#         return
    
#     # Create a new runs structure to replace the existing one
#     new_runs = []
    
#     # Iterate through existing runs to find where placeholders are
#     placeholder_start = paragraph.text.find(placeholder)
#     if placeholder_start == -1:
#         return
    
#     text_before = paragraph.text[:placeholder_start]
    
#     # We need to reconstruct the paragraph to properly highlight the replacements
#     # First, clear all existing runs
#     for _ in range(len(paragraph.runs)):
#         paragraph.runs[0]._element.getparent().remove(paragraph.runs[0]._element)
    
#     # Add text before the placeholder
#     if text_before:
#         run = paragraph.add_run(text_before)
        
#     # Add the replacement text with highlight
#     highlighted_run = paragraph.add_run(replacement)
#     highlighted_run.font.highlight_color = WD_COLOR_INDEX.YELLOW
    
#     # Add text after the placeholder
#     text_after = paragraph.text[placeholder_start + len(placeholder):]
#     if text_after:
#         run = paragraph.add_run(text_after)
        
#     # Recursively process any other instances of the same placeholder
#     #remove 

#     if placeholder in paragraph.text:
#         replace_text_with_highlight(paragraph, placeholder, replacement)


# docx_location='inputs/sv_docx_templates/Report 8 - Resin Lifetime Process Validation Template.docx'
# output_location='test_op'

# data={
#     "<Chromatography_Type>":"Protein A "
# }

# sitev_generate_report_from_template(bucket_name,docx_location,output_location,data)


        


# def get_list_of_files(bucket_name, folder_prefix):
#     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)["Contents"]
#     return response

# def get_document_typ(document_name):
#     return document_name.split("_")[0]


def get_document_typ(document_name, source_file_names, source_file_types, table_template_master, template_fullname):
    """
    Determines the document type from source file types using the template mapping.

    Args:
        document_name (str): The name of the document file.
        source_file_names (list): List of source file names.
        source_file_types (list): Corresponding list of file types.
        table_template_master (str): DynamoDB table name for templates.
        template_fullname (str): Full template name.

    Returns:
        str: Document type name (mapped or inferred).
    """
    logger.info("get_document_typ: Extracting template and resolving document type")

    template_id, docx_s3_key, excel_s3_key, section_names, template_name, doc_types, doc_types_full = sitev_extract_template(
        table_template_master, template_fullname
    )

    try:
        # Map the document type from source file type
        full_file_type = source_file_types[source_file_names.index(document_name)]
        logger.info("get_document_typ: Mapped document type from template")
        return doc_types[doc_types_full.index(full_file_type)]

    except Exception as e:
        logger.error(f"get_document_typ: Fallback to inferred document type due to error: {str(e)}")

    # Fallback: use prefix from filename
    return document_name.split("_")[0]

def get_list_of_files(bucket_name: str, folder_prefix: str):
    """
    Retrieves a list of files from an S3 bucket under a specified folder prefix.

    This function uses the AWS S3 client to list objects in a given bucket and folder path.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_prefix (str): Folder path (prefix) to list the files under.

    Returns:
        list: A list of objects (dictionaries) representing the files found under the prefix.
              If no files are found, it returns an empty list.

    Raises:
        KeyError: If the 'Contents' key is not present in the response.
    """
    logger.info("get_list_of_files: Listing objects from S3 bucket")

    # Fetch objects from S3 using list_objects_v2 with the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    logger.info("get_list_of_files: Objects listed successfully")

    # Extract the 'Contents' key from the response
    file_list = response.get(S3_LIST_KEY, [])

    logger.info(f"get_list_of_files: Retrieved {len(file_list)} files")

    return file_list
    
def chunk_site_validation(bucket_name, upload_folder, output_folder,
                          source_file_names, source_file_types,
                          table_template_master, template_fullname, report_id):
    """
    Processes PDF files in an S3 folder, extracts page-wise chunks with metadata, and saves them to S3.

    Args:
        bucket_name (str): Name of the S3 bucket.
        upload_folder (str): Folder in S3 containing uploaded PDF files.
        output_folder (str): Destination folder for chunked output in S3.
        source_file_names (list): List of original file names.
        source_file_types (list): Corresponding list of file types.
        table_template_master (str): DynamoDB table name for template data.
        template_fullname (str): Full name of the template.
        report_id (str): Unique identifier of the report.

    Returns:
        None
    """
    logger.info("chunk_site_validation: Fetching list of files from S3 upload folder")
    list_of_upload_files = get_list_of_files(bucket_name, upload_folder)
    print(list_of_upload_files)

    for i, upload_file in enumerate(list_of_upload_files):
        file_key = upload_file["Key"]
        file_name = file_key.split("/")[-1]
        
        if file_name.endswith(".pdf"):
            
            logger.info(f"chunk_site_validation: Processing file {file_key}")

            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_data = file_obj["Body"].read()

            with pdfplumber.open(io.BytesIO(file_data)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()

                    logger.info(f"chunk_site_validation: Extracting text from page {page_num} of {file_name}")

                    metadata = {
                        "metadataAttributes": {
                            "report_id": report_id,
                            "document_name": file_name,
                            "document_typ": get_document_typ(
                                file_name, source_file_names, source_file_types,
                                table_template_master, template_fullname
                            ),
                            "page_nr": page_num
                        }
                    }

                    chunk_text = page_text + str(metadata)
                    chunk_path = f"{output_folder}/{file_name}_chunk_{page_num}.txt"
                    metadata_json_path = f"{chunk_path}.metadata.json"

                    logger.info(f"chunk_site_validation: Uploading chunk page {page_num} to S3")
                    s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

                    logger.info(f"chunk_site_validation: Uploading metadata for page {page_num}")
                    json_str = json.dumps(metadata, indent=4)
                    json_bytes = io.BytesIO(json_str.encode("utf-8"))
                    s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)

        if file_name.endswith("xlsx"):
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            excel_data = response['Body'].read()
            sheets = pd.read_excel(io.BytesIO(excel_data), sheet_name=None, keep_default_na=False, na_values=[""])

            for sheet_name, df in sheets.items():
                # # Convert rows to dicts
                # records = df.to_dict(orient="records")
                
                # # Wrap with metadata
                # for i, record in enumerate(records):
                metadata = {
                    "metadataAttributes": {
                        "report_id": report_id,
                        "document_name": file_name,
                        "document_typ": get_document_typ(
                            file_name, source_file_names, source_file_types,
                            table_template_master, template_fullname
                        ),
                        "sheet_name": sheet_name
                    }
                }
                chunk_text = json.dumps({"headers": df.columns.tolist(), "rows": df.values.tolist()}, ensure_ascii=False, default=str)
                chunk_path = f"{output_folder}/{file_name}_chunk_{sheet_name}.txt"
                metadata_json_path = f"{chunk_path}.metadata.json"

                logger.info(f"chunk_site_validation: Uploading Sheet name {sheet_name} to S3")
                s3.put_object(Bucket=bucket_name, Key=chunk_path, Body=chunk_text)

                logger.info(f"chunk_site_validation: Uploading metadata for Sheet name {sheet_name}")
                json_str = json.dumps(metadata, indent=4)
                json_bytes = io.BytesIO(json_str.encode("utf-8"))
                s3.upload_fileobj(json_bytes, bucket_name, metadata_json_path)



bucket_name = "azcdi-us-ops-report-ds-dev"
upload_folder =  "tmp_kjxj423_feicpz/" 
table_template_master = "aig-azcdi-us-ops-report-templatemaster-dev" 
template_fullname = "SVR - Membrane Lifetime Process Validation Template" 
report_id = "0ae59d28-d7f8-4fc0-9850-e6d4a93efcfe"
source_file_names = ["Gen AI - Report 9 CSV File Test.xlsx", "Source 1 - VX-715110-PVP Tozorakimab UFDF Membrane Lifetime.pdf", "Source 2 - (PLAN-0114079) - VMP-X-097 Appendix 1 Tozorakimab (MEDI3506) Manufacturing Process at the (FMC) Building 633.pdf", "Source 3 - (REP-0195531) - Tozorakimab (MEDI3506) Downstream Process Description and Validation Criteria for Commercial Manufacturing.pdf", "Source 4 - Tozorakimab, FMC Building 633, Quality Control Testing Plan Formulation - Copy.pdf"] 
source_file_types = ["Table in Report", "Membrane Lifetime PV Protocol", "Process Validation Master Plan", "Control Strategy Document", "Quality Control Testing Plan"]
chunk_output_folder = "Fabrice_test_chunk_output"
# upload_folder =  "inputs/Source_pdfs/"
# # # chunk_output_folder = "outputs_sv"
# chunk_site_validation(bucket_name, upload_folder, chunk_output_folder, source_file_names, source_file_types, table_template_master, template_fullname, report_id)
# # report_id = "56df9ab5-7510-4788-96cd-a2914cfe4021"
# # rul= """ give the Validation number in the document typ "MAIN" present in the page number 0. Only return the exact information with no conversational text. The Validation is a string starting with VX. Add -R1: to the end of the name, i.e. VX-NUMBER-TEXT-R1""" 
# # print(f"""based on the report id '{report_id}',""" + rul)

# def extract_pdf_contents(file_bytes: bytes) -> str:
#     """Extract text content from a PDF file."""
#     try:
#         pdf_stream = BytesIO(file_bytes)
#         reader = PyPDF2.PdfReader(pdf_stream)
#         text = ""
#         for page in range(len(reader.pages)):
#             page_obj = reader.pages[page]
#             text += page_obj.extract_text() or ""
#         return text
#     except Exception as e:
#          raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

# def generate_prompt(content: str, additional_instructions: Optional[str]) -> str:
#     """Generate a prompt for the language model based on content and additional instructions."""
#     # Define the base template for the prompt
#     base_template = """
#     %INSTRUCTIONS:"You are an assistant and your job is to answer questions based on the documents that are uploaded by the user. Please provide concise answer and only the answer. Don't give answer to the questions that are not relevant to the document content. Maintain the chat conversation using the previous chat interactions.    
#     """
#     # Add the content of the file to the template
#     if content:
#         base_template += f"\n\n%TEXT:\n{content}\n"
        
#     # Append the user query (additional instructions) if provided
#     if additional_instructions:
#         base_template += f"\n\n%USER QUERY:\n{additional_instructions}\n"

#     # The PromptTemplate can now be created using the combined template
#     prompt = PromptTemplate(
#         input_variables=[],  # No need for dynamic variables here as we have formatted the string manually
#         template=base_template,
#     )
#     # Return the final prompt with all components
#     return base_template

# def get_user_memory(session_id: str):
#     if session_id not in history_store:
#         history_store[session_id] = InMemoryChatMessageHistory()
#     return history_store[session_id]

# import csv
# import io
# import re

# def convert_stringtable_to_df(text_content, bucket_name, table_name = "table_4"):
#     """
#     Convert a text-based table extracted from a PDF into CSV format by first
#     converting to a pandas DataFrame for easier manipulation.
    
#     Args:
#         text_content (str): The raw text extracted from a PDF containing table data
        
#     Returns:
#         str: CSV formatted string
#     """
  
#     # Identify the "Key: Value" pattern
#     pattern = re.compile(r"([^:]+):\s*(.*)")

#     lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        
#     dict_entry = {}
#     for line in lines:
#         match = pattern.match(line)
#         if match:
#             key = match.group(1).strip()
#             value = match.group(2).strip()
#             if key  in dict_entry:
#                 dict_entry[key].append(value)
#             else:
#                 dict_entry[key] = [value]


#     # Keys to include in the subset
#     if table_name == "table_4":
#        keys = ["Parameter Classification", "Parameter Description", "Validation Criteria"]
#        subset_dict = {k: dict_entry[k] for k in keys if k in dict_entry}        
#        df = pd.DataFrame.from_dict(subset_dict)
#     else:
#        df = pd.DataFrame.from_dict(dict_entry, orient='columns')
#     #    print(dict_entry)
    
#     return df


# def retrieve_and_generate_online(bucket_name,output_s3_key, upload_folder, queryText, MODEL_ID, session_id="vvvvv"):
#     try: 
#         content = ""
#         list_of_upload_files = get_list_of_files(bucket_name, upload_folder)
#         # print(list_of_upload_files)

#         for i, pqr_file in enumerate(list_of_upload_files):  # Iterate over each file
#            if i == 0: 
#                continue
#            pqr_file_key = pqr_file["Key"]
#            pqr_file_name = pqr_file_key.split("/")[-1]
    
#        # Open the PDF using pdfplumber
#         #    print("####################################")
#         #    print(pqr_file_key)
#            pqr_file_obj = s3.get_object(Bucket=bucket_name, Key=pqr_file_key)
#            pqr_file_data = pqr_file_obj["Body"].read()
#            content += extract_pdf_contents(pqr_file_data)

       
#         if not queryText and not content:
#             raise HTTPException(status_code=400, detail="QueryText or content from the file is required")            
#         prompt = generate_prompt(content, queryText)  # Generate the prompt based on queryText and content
#         llm = ChatBedrock(model_id=MODEL_ID)
#         chain = RunnableWithMessageHistory(llm, get_user_memory)
#         #Run the model with the prompt
#         try:
#             answer = chain.invoke(
#                 prompt,             
#                 config={"configurable": {"session_id": session_id}},
#             )
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error invoking model: {str(e)}")
#         answer = answer.content

#         csv_op=convert_table_to_csv(answer)

#         s3.put_object(Bucket=bucket_name, Key=output_s3_key,body=csv_op)


#         # Check if the answer contains what looks like a table with key-value pairs


#     except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error invoking model: {str(e)}") 

#     return answer  



# def convert_to_docx(df):
#     # csv_data = csv_file_name.getvalue()

#     # # Load CSV data into a pandas DataFrame
#     # df = pd.read_csv(io.StringIO(csv_data))

#     new_df=df.iloc[:,[2,1,4]].dropna()

#     print(new_df)

 

#     # Create a new Word Document
#     document = Document_docx()

#     # Add a table to the document with one row for the header
#     table = document.add_table(rows=1, cols=len(new_df.columns))
#     table.style = 'Table Grid'  # Optional: set a table style

#     # Populate the header row
#     hdr_cells = table.rows[0].cells

#     print(hdr_cells)
#     for idx, column in enumerate(new_df.columns):
#         hdr_cells[idx].text = str(column)

#     # Populate the table with data rows
#     for index, row in new_df.iterrows():
#         row_cells = table.add_row().cells
#         for idx, cell in enumerate(row):
#             row_cells[idx].text = str(cell)

#     # Save the document
#     #print(document)
#     document.save("output.docx")




kb_name = "azcdi-us-ops-report-siteval-test"
model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
output_folder='test_op/'
#query = '""" Given the document with the name document_name:"1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor", give the Validation number present in the page "page_nr :0". """'
query2 = """ give the table 4 in the document typ "Main" present in the page number 7. Give a table as a key pair value, like for example column name: value. please provide the contains of this table without commenting """ 
# answer=retrieve_and_generate(query, kb_name, model_id)
# csv_file=convert_table_to_csv(answer,bucket_name,output_folder)
# convert_to_docx(csv_file)
# df = convert_stringtable_to_df(answer,bucket_name,output_folder, table_name="table_4")
# convert_to_docx(df)
# print(df)
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data=data, dframe=df, table_loc= "Table 3:")

query1 = """ give the table 3 in the document typ "Main" present in the page number 1. Give a table as a key pair value, like for example column name: value. please provide the contains of this table without commenting """ 
query3 = """ give the table 4 in the document typ "Main" present in the page number 8. Give a table as a key pair value, like for example column name: value. please provide the contains of this table without commenting """ 

query_summary = """ give the Chromatography type mentioned in the document "REPORT4", usually listed in page 0 after the string 'Process Validation:'X' Chromatography,'. Only return the exact information of the second Chromatography type extracted prior to the word Chromatography with no conversational text."""

# answer=retrieve_and_generate(query_summary, kb_name, model_id)
# print(answer)
# print(convert_stringtable_to_df(answer,bucket_name,output_folder, table_name = "table_4"))

# dict_ = {'Parameter Type': ['CPP', 'KPP', 'IPC', 'PA', 'MC', 'DS Testing'], 
# 'Range Nomenclature': ['Acceptable Range', 'Acceptable Range (non-critical)', 'Acceptance Criteria', 'Action Limit (non-critical)', 'Action Limit', 'Validation Criteria']
# }

# print(pd.DataFrame(dict_))
# rules_list = [query1, query2]
# answer_list = [retrieve_and_generate(rul, kb_name, model_id) for rul in rules_list]
# # # answer=retrieve_and_generate(query, kb_name, model_id)
# # # csv_file=convert_table_to_csv(answer,bucket_name,output_folder)
# # # convert_to_docx(csv_file)
# print(answer_list)
# df_list = []
# table_name_list = ["table_3", "table_4"]
# for index_, answer in enumerate(answer_list):
#     df_list.append(convert_stringtable_to_df(answer,bucket_name,output_folder, table_name = table_name_list[index_]))
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data=data, dframe_list = df_list, table_loc_list = ["Table 2:", "Table 3:"])








# bucket_name = "azcdi-us-ops-report-ds-dev"
# upload_folder = "inputs/source_pdf/"
# queryText_List = ["What is the document number", "What is the Validation number", "What is the Molecule Name"]
# MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
# store = {}
# history_store = {}

# output_list = [retrieve_and_generate_online(bucket_name, upload_folder, queryText, MODEL_ID) for queryText in queryText_List]
# print(output_list)

############################Test Overall######################################################
############################Test Overall######################################################
# docx_s3_key = "inputs/sv_docx_templates/Modified - Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations.docx"
# excel_file_path = 'inputs/sv_xlsx_templates/Modified_sv_xlsx_template_InoExp_rules.xlsx'
# #excel_file_path='inputs/sv_xlsx_templates/sv_xlsx_template_InoExp_rules.xlsx'
# az_mapping_sheet_name = "Site_Validation"
# kb_name = "azcdi-us-ops-report-siteval-dev"
# model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
# data = get_mapping_list_sv(bucket_name, excel_file_path , az_mapping_sheet_name, kb_name, model_id)
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data=data)
############################Test Overall######################################################
############################Test Overall######################################################

#, dframe_list = df_list, table_loc_list=table_loc_list)
#query = '""" Given the document with the name document_name:"1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor", give the Validation number present in the page "page_nr :0". """'
# query = """ give the table 4 in the document typ "Main" present in the page number  7. please provide the contains of this table without commenting""" 
# print(retrieve_and_generate(query, kb_name, model_id))
# rule_dic_text, output_list_table,  place_holder_list_table, place_proc_list_table 
#get_mapping_list_sv(bucket_name, excel_file_path , az_mapping_sheet_name, kb_name, model_id)
#data, answer_list, table_loc_list, table_name_list  = get_mapping_list_sv(bucket_name, excel_file_path , az_mapping_sheet_name, kb_name, model_id)


# print(data)
# print(res)

# rules_list = [query1, query2]
#answer_list = [retrieve_and_generate(rul, kb_name, model_id) for rul in rules_list]
# # answer=retrieve_and_generate(query, kb_name, model_id)
# # csv_file=convert_table_to_csv(answer,bucket_name,output_folder)
# # convert_to_docx(csv_file)
#################################################
# print(answer_list)
##df_list = []
# table_name_list = ["table_3", "table_4"]
# for index_, answer in enumerate(answer_list):
#     df_list.append(convert_stringtable_to_df(answer,bucket_name, table_name = table_name_list[index_]))
# sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data=data)#, dframe_list = df_list, table_loc_list=table_loc_list)


# def and_all_filter(params: Dict) -> Dict:
    
#     filters = [
#         # {"equals": {"key": key, "value": value.lower()}}
#         {"stringContains": {"key": key, "value": value.lower()}}
#         for key, value in params.items() 
#         if value is not None  and isinstance(value, str) and "date" not in key
#     ]
    
#     filters.extend([
#         # {"equals": {"key": key, "value": value.lower()}}
#         {"in": {"key": key, "value": value}}
#         for key, value in params.items() 
#         if value is not None  and isinstance(value, list)
#     ]
#     )
        
#     filters.extend([
#         # {"equals": {"key": key, "value": value.lower()}}
#         {"greaterThanOrEquals": {"key": "created_at", "value": int((datetime.fromisoformat(value)).timestamp()) }}
#         for key, value in params.items() 
#         if value is not None  and "start_date" in key
#     ]
#     )
    
#     filters.extend([
#         # {"equals": {"key": key, "value": value.lower()}}
#         {"lessThanOrEquals": {"key": "created_at", "value": int((datetime.fromisoformat(value)).timestamp())}}
#         for key, value in params.items() 
#         if value is not None  and "end_date" in key
#     ]
#     )
        
#     return {"andAll": filters} if filters else {}


# params = {"template_name": "ISPR Report", "product_name": "Beyfortus", "created_by": ['kkfx871'], "created_at": '2025-06-25', "reporting_period": None, "section_name": None, "created_start_date": "2025-06-25", "created_end_date": "2025-06-26"}
# # print(and_all_filter(params, "equals"))
# print("################################")
# report_filter =  and_all_filter(params)
# print(report_filter)

# value = "2025-06-25T13:41:36.909643"
# value = "2025-06-25T09:59:02.114566"
# value = "2025-06-25T10:59:03.124566"
# value = "2025-05-20T09:59:34.632893"
# value = datetime.now().isoformat()
# print(int((datetime.fromisoformat(value)).timestamp())*1000)
# value = "2025-06-26"
# dt = datetime.fromisoformat(value)
# # print(dt)
# print(int(dt.timestamp()))



# print("################################")
# print(x.lower())

# {
#     'andAll': [
#         {'equals': 
#          {'key': 'template_name', 
#           'value': 'temp'
#          }
#         }, 
#         {'equals': 
#          {'key': 'product_name', 
#           'value': 'Fasenra'
#          }
#         }, 
#         {'equals': 
#          {'key': 'reporting_period', 
#           'value': '2022_2023'
#          }
#         }
#     ]
# }

###########Extraction Main Function
# def image_extraction_func(placeholders_fig, rules_fig, upload_folder, main_document_name, image_s3_loc):

def extract_images_and_upload(
        # self,
        rules_fig, # = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 3", "Figure 4", "Figure 5"]}
        # placeholders_fig,
        pdf_s3_key: str,
        # figures: List[str],  # Target figure labels like ["Figure 1", "Figure 2"]
        s3_destination_folder: str
        # doc_id: str
    ):
        import fitz
        from PIL import Image
        import io
        from tempfile import NamedTemporaryFile

        # Step 1: Check if the PDF exists in S3
        # if not s3_file_exists(self.s3, self.s3.bucket_name, pdf_s3_key):
        #     raise FileNotFoundError(f"S3 object not found: {pdf_s3_key}")

        # Step 2: Download PDF locally to temporary file for processing
        # with NamedTemporaryFile(delete=True, suffix=".pdf") as tmp_file:
        #     self.s3.download_file(pdf_s3_key, tmp_file.name)
            
        s3_object = s3.get_object(Bucket=bucket_name, Key=pdf_s3_key)
        pdf_bytes = s3_object['Body'].read()

        # logger.info(f"📄 Downloaded PDF to {tmp_file.name}")
        uploaded_keys = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")  # Open PDF document with PyMuPDF

        result_img_extrac = {}
        # Step 3: Loop through each figure label to find and extract
        for placeholder_key, figures in rules_fig.items(): 
            for label in figures:
                logger.info(f"\nScanning for label '{label}'")
                found = False  # Track if image for this label is successfully extracted

                # Step 4: Search across all pages for the label text
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    logger.info(f"Searching on page {page_num}")

                    # Step 5: Use text search to find bounding box for label text
                    caption_boxes = page.search_for(label)
                    if not caption_boxes:
                        continue  # Continue to next page if label not found

                    # If multiple matches, only consider the first one
                    caption_box = caption_boxes[0]
                    logger.info(f"Found label '{label}' at position {caption_box}")

                    try:
                        # Step 6: Render the entire page to an image at higher resolution (2.5x zoom)
                        zoom = 2.5
                        mat = fitz.Matrix(zoom, zoom)
                        pix = page.get_pixmap(matrix=mat)
                        pil_image = Image.open(io.BytesIO(pix.tobytes("png")))

                        # Step 7: Define crop area starting overlapping the caption (not just below)
                        page_width = page.rect.width
                        page_height = page.rect.height

                        fig_x = max(0, caption_box.x0 - 30)    # Extend left for padding
                        fig_y = max(0, caption_box.y0 - 10)    # Start 10 pts above caption top to avoid truncation
                        fig_w = min(page_width - fig_x, page_width * 0.9)  # Width up to 90% of page
                        fig_h = min(page_height - fig_y, 400)  # Height up to 400 pts

                        # Step 8: Convert from PDF points to image pixels (using zoom)
                        img_x = int(fig_x * zoom)
                        img_y = int(fig_y * zoom)
                        img_w = int(fig_w * zoom)
                        img_h = int(fig_h * zoom)

                        # Crop the image based on calculated region
                        # cropped = pil_image.crop((img_x, img_y, img_x + img_w, img_y + img_h))

                        # Crop the image based on calculated region
                        cropped = pil_image.crop((img_x, img_y, img_x + img_w, img_y + img_h))

                        # Resize cropped image to max width and height (e.g., 500x300 px)
                        max_width, max_height = 500, 300
                        cropped.thumbnail((max_width, max_height), Image.LANCZOS)

                        # Step 9: Filter out very small crops (likely incorrect regions)
                        if cropped.width < 150 or cropped.height < 100:
                            logger.warning(f"Cropped image too small: {cropped.size}")
                            continue


                        # # Step 9: Filter out very small crops (likely incorrect regions)
                        # if cropped.width < 150 or cropped.height < 100:
                        #     logger.warning(f"Cropped image too small: {cropped.size}")
                        #     continue

                        # Step 10: Save to buffer and upload to S3
                        fig_number = "".join(filter(str.isdigit, label)) or "unknown"
                        image_key = f"{s3_destination_folder}/{placeholder_key}_figure_{fig_number}.png"

                        buffer = io.BytesIO()
                        cropped.save(buffer, format="PNG")
                        buffer.seek(0)
                        
                        s3.upload_fileobj(buffer, bucket_name, image_key)

                        uploaded_keys.append(image_key)

                        logger.info(f"Uploaded image for '{label}' → s3://{bucket_name}/{image_key}")
                        found = True
                        break  # Stop searching more pages once found

                    except Exception as e:
                        logger.error(f"Failed to crop/upload image for '{label}': {e}")

                if not found:
                    logger.warning(f"No matching image found for label: {label}")

            result_img_extrac[placeholder_key] = uploaded_keys
            uploaded_keys = []
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

def _insert_image(doc, paragraph, image_bytes, width, idx_label) -> bool:
    """
    Helper function to insert an image immediately after the given paragraph.
    This handles Word's internal XML structures to place the new image correctly.
    """
    try:
        new_paragraph = doc.add_paragraph()
        run = new_paragraph.add_run()
        run.add_picture(image_bytes, width=width)

        parent = paragraph._element.getparent()
        idx_in_parent = parent.index(paragraph._element)
        parent.insert(idx_in_parent + 1, new_paragraph._element)

        logger.info(f"Inserted image after paragraph {idx_label}.")
        return True
    except Exception as e:
        logger.error(f"Error inserting image after paragraph {idx_label}: {e}")
        return False
    
def _insert_table(doc, paragraph, df, placeholder) -> bool:
    """
    Helper function to insert an image immediately after the given paragraph.
    This handles Word's internal XML structures to place the new image correctly.
    """
    try:
#         new_paragraph = doc.add_paragraph()
#         run = new_paragraph.add_run()
#         run.add_picture(image_bytes, width=width)

#         parent = paragraph._element.getparent()
#         idx_in_parent = parent.index(paragraph._element)
#         parent.insert(idx_in_parent + 1, new_paragraph._element)

#         logger.info(f"Inserted image after paragraph {idx_label}.")
        p = paragraph._element
        tbl = doc.add_table(rows=1, cols=len(df.columns))
        tbl.style = "Table Grid"

          # Fill header
        hdr_cells = tbl.rows[0].cells
        for i, col in enumerate(df.columns):
            hdr_cells[i].text = str(col)
                
          # Fill rows
        for index, row in df.iterrows():
            row_cells = tbl.add_row().cells
            for i, item in enumerate(row):
                row_cells[i].text = str(item)
                
        # Move the table XML element after the placeholder paragraph
        p.addnext(tbl._element)
        
        # Remove placeholder text
        paragraph.text = paragraph.text.replace(placeholder, "")

        return True
    except Exception as e:
        logger.error(f"Error inserting Table: {e}")
        return False

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
            return _insert_image(doc, paragraph, image_bytes, width, idx)

    # === 2. Search in tables ===
    for t_idx, table in enumerate(doc.tables):
        for r_idx, row in enumerate(table.rows):
            for c_idx, cell in enumerate(row.cells):
                for p_idx, paragraph in enumerate(cell.paragraphs):
                    norm_para = normalize_text(paragraph.text)
                    logger.debug(f"[Table {t_idx}][Row {r_idx}][Cell {c_idx}][Paragraph {p_idx}] '{paragraph.text}'")
                    if norm_marker in norm_para:
                        return _insert_image(doc, paragraph, image_bytes, width,
                                                  f"Table {t_idx} Row {r_idx} Cell {c_idx} Para {p_idx}")

    logger.warning(f"Marker text '{marker_text}' not found anywhere in the document.")
    return False



# {'<Figure 1:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 1:>_figure_2.png'], '<Figure 2:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_3.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_4.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_5.png']}   

def load_excel_from_s3(bucket_name, file_path, sheet_name):
    """
    Loads an Excel sheet from an S3 bucket into a pandas DataFrame.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_path (str): Key/path of the Excel file in the bucket.
        sheet_name (str): Name of the sheet to load.

    Returns:
        pd.DataFrame: Loaded data as a DataFrame.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        excel_data = response['Body'].read()
        return pd.read_excel(io.BytesIO(excel_data), sheet_name=sheet_name)
    except Exception as e:
        logger.error(f"Failed to load Excel from S3: {e}")
        raise
        

def insert_table_after_paragraph(doc: Document_docx, marker_text: str, table_df) -> bool:
    """
    Searches for a placeholder string in paragraphs or tables,
    and inserts the given dataframe as table immediately after the matched paragraph.
    """

    norm_marker = normalize_text(marker_text)

    # === 1. Search in standard paragraphs ===
    for idx, paragraph in enumerate(doc.paragraphs):
        norm_para = normalize_text(paragraph.text)
        # logger.debug(f"[Paragraph {idx}] '{paragraph.text}'")

        if norm_marker in norm_para:
            return _insert_table(doc, paragraph, table_df, marker_text)

    # # === 2. Search in tables ===
    # for t_idx, table in enumerate(doc.tables):
    #     for r_idx, row in enumerate(table.rows):
    #         for c_idx, cell in enumerate(row.cells):
    #             for p_idx, paragraph in enumerate(cell.paragraphs):
    #                 norm_para = normalize_text(paragraph.text)
    #                 logger.debug(f"[Table {t_idx}][Row {r_idx}][Cell {c_idx}][Paragraph {p_idx}] '{paragraph.text}'")
    #                 if norm_marker in norm_para:
    #                     return _insert_image(doc, paragraph, image_bytes, width,
    #                                               f"Table {t_idx} Row {r_idx} Cell {c_idx} Para {p_idx}")

    logger.warning(f"Marker text '{marker_text}' not found anywhere in the document.")
    return False

def sitev_insert_figure_in_report(
    rules_img,
    target_s3_key: str
):
    # import tempfile

    # Step 1: Download DOCX template from S3
    # try:
    #     temp_dir = tempfile.gettempdir()
    #     local_template_path = os.path.join(temp_dir, os.path.basename(template_s3_key))
    #     self.s3.download_file(template_s3_key, local_template_path)
    # except Exception as e:
    #     logger.error(f"Failed to download template DOCX {template_s3_key}: {e}")
    #     raise

    # Step 2: Load DOCX using python-docx
    try:
        response = s3.get_object(Bucket=bucket_name, Key=target_s3_key)
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

        
# rules_img = {'<Figure 1:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 1:>_figure_2.png'], '<Figure 2:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_3.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_4.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_5.png']} 
# target_s3_key = "tmp_test5/Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations.docx"
# generate_report(rules_img, target_s3_key)
    
# main_document_name = "MAIN_1a (PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor.pdf"
# upload_folder = "tmp_in_img/" ######### Where the pdf/source is located
# image_s3_loc = "tmp_out_img/"
# figures = image_extraction_func(placeholders_fig, rules_fig, upload_folder, main_document_name, image_s3_loc)
# print(figures)
 # Expected result:
    # figures = {"<Figure 1:>": ["tmp_out_img//img1.png"], "<Figure 2:>": ["tmp_out_img//img2.png", "tmp_out_img//img3.png", "tmp_out_img//img3.png"]}
    # in image_s3_loc in S3, we will find extracted images

#### Generator Main Function
# def sitev_insert_figure_in_report(bucket_name, target_filename, data=figures):

# {'<Figure 1:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 1:>_figure_2.png'], '<Figure 2:>': ['temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_3.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_4.png', 'temp_kklc575/tmp_test5/final_reports/<Figure 2:>_figure_5.png']}   

####################################
#rules_tab = {'<Table 3:>': "Process Parameters", '<Table 5:>': "Deviation", '<Table 6:>': "Change Controls"}
def sitev_insert_table_in_report(
    bucket_name,
    rules_tab,
    target_s3_key: str,
    xls_table_s3_key: str
):
    # import tempfile

    # Step 1: Download DOCX template from S3
    # try:
    #     temp_dir = tempfile.gettempdir()
    #     local_template_path = os.path.join(temp_dir, os.path.basename(template_s3_key))
    #     self.s3.download_file(template_s3_key, local_template_path)
    # except Exception as e:
    #     logger.error(f"Failed to download template DOCX {template_s3_key}: {e}")
    #     raise

    # Step 2: Load DOCX using python-docx
    try:
        response = s3.get_object(Bucket=bucket_name, Key=target_s3_key)
        template_bytes = response['Body'].read()
        template_file = BytesIO(template_bytes)
        # doc = Document_docx(template_file)
        doc = Document_docx(template_file)
    except Exception as e:
        logger.error(f"Failed to open template DOCX: {e}")
        raise

   
    # Step 3: Loop over each mapping entry (each represents one figure/placeholder pair)
    # for mapping in mappings:
    for placeholder, sheet_name in rules_tab.items():
        # placeholder = mapping["Place_holder"]
        # doc_id = mapping["Template_Name"]
        # figure_numbers = mapping["Figures"]
        df_mapping = load_excel_from_s3(bucket_name, xls_table_s3_key, sheet_name) 
        logger.info(f"Inserting table {sheet_name}")
        inserted = insert_table_after_paragraph(doc, placeholder, df_mapping)
        logger.info(f"inserted result is: {inserted}") 

        if not inserted:
            logger.warning(f"Failed to insert table for placeholder {placeholder}")

   
    # Step 5: Upload final report back to S3
    try:
        doc_stream = io.BytesIO()
        doc.save(doc_stream)
        doc_stream.seek(0)
        s3.upload_fileobj(doc_stream, bucket_name, target_s3_key)
        
    except Exception as e:
        logger.error(f"Failed to upload final report to S3: {e}")
        raise

bucket_name = "azcdi-us-ops-report-ds-dev"
rules_tab = {'<Table 3:>': 'Process Parameters', '<Table 5:>': 'Deviation', '<Table 6:>': 'Change Controls'} 
target_s3_key = "tmp_test5/Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations_.docx"
xls_table_s3_key = "tmp_test5/GenAI - Report 1 CSV File Test.xlsx"
# sitev_insert_table_in_report(bucket_name, rules_tab, target_s3_key, xls_table_s3_key)

###########For Testing
# target_filename = "tmp/report.docx" # docx template with the place holder <Figure 1:> and <Figure 2:>
# bucket_name = "azcdi-us-ops-report-ds-dev"

# placeholders_fig = ["<Figure 1:>", "<Figure 2:>"]
# rules_fig = ["Figure 2", "Figure 3, Figure 4, Figure 5"]

# result_dict = {}

# for placeholder, rule in zip(placeholders_fig, rules_fig):
#     # Split by comma and strip spaces
#     split_rules = [part.strip() for part in rule.split(",")]
#     result_dict[placeholder] = split_rules

# print(result_dict)
# source_file_names = ["a", "b", "c"]
# source_file_types = ["e", "f", "g"]

# print(source_file_names[source_file_types.index("h")])
import mammoth
def convert_docx_to_html_mammoth(bucket_name, s3_key):
    logger.info("\n\nconvert_docx_to_html_mammoth --------------1-------------->")
    # Create an S3 client
    s3 = boto3.client('s3')

    # Download the file from S3
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    logger.info("convert_docx_to_html_mammoth --------------2-------------->")
    docx_bytes = obj['Body'].read()
    logger.info("convert_docx_to_html_mammoth --------------3-------------->")

    # Create a file-like object from the bytes data
    docx_file_like = io.BytesIO(docx_bytes)

    # Convert docx to HTML using mammoth
    try:
        result = mammoth.convert_to_html(docx_file_like)
        logger.info("convert_docx_to_html_mammoth --------------4-------------->")
        html_content = result.value  # Extract HTML content
        logger.info(f"convert_docx_to_html_mammoth --------------5-------------->")
    except IndexError as e:
        print("Caught IndexError:", e)
        html_content = ""
    return html_content

s3_key = "outputs/sitev-ProteinA-ktzh503-nojhku.docx"
bucket_name = "azcdi-us-ops-report-ds-test"

# print(convert_docx_to_html_mammoth(bucket_name, s3_key))

##########################
# Define source and target tables
def copy_tables (SourceTableName, TargetTableName):
    dynamodb = boto3.resource('dynamodb')
    source_table = dynamodb.Table(SourceTableName)
    target_table = dynamodb.Table(TargetTableName)



    # Step 1: Scan the source table
    response = source_table.scan()
    items = response['Items']

    # Continue scanning if there are more pages
    while 'LastEvaluatedKey' in response:
        response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    # Step 2: Batch write to target table
    with target_table.batch_writer(overwrite_by_pkeys=['template_id']) as batch:
        for item in items:
            batch.put_item(Item=item)

    print(f"Copied {len(items)} items to {target_table.name}")
SourceTableName = "aig-azcdi-us-ops-report-templatemaster-dev" 
TargetTableName = "aig-azcdi-us-ops-report-templatemaster-test" 
# TargetTableName = "azcdi-us-ops-report-templatemaster-ppd"
# copy_tables (SourceTableName, TargetTableName)

##########################
# Define source and target tables
def copy_files(bucket_sources: str, bucket_target: str, s3_source_rep: str, s3_target_rep: str):
    """
    Copy all files from one S3 bucket directory (prefix) to another.

    Args:
        bucket_sources (str): Source S3 bucket name.
        bucket_target (str): Target S3 bucket name.
        s3_source_rep (str): Source prefix (like 'folder/subfolder/').
        s3_target_rep (str): Target prefix (like 'backup/').
    """
    s3 = boto3.client('s3')

    # Ensure prefixes end with '/'
    if s3_source_rep and not s3_source_rep.endswith('/'):
        s3_source_rep += '/'
    if s3_target_rep and not s3_target_rep.endswith('/'):
        s3_target_rep += '/'

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_sources, Prefix=s3_source_rep)

    copied_files = 0

    for page in page_iterator:
        if 'Contents' not in page:
            print(f"No files found in s3://{bucket_sources}/{s3_source_rep}")
            return

        for obj in page['Contents']:
            source_key = obj['Key']
            # Build new key in target
            target_key = s3_target_rep + source_key[len(s3_source_rep):]

            copy_source = {'Bucket': bucket_sources, 'Key': source_key}

            s3.copy_object(
                CopySource=copy_source,
                Bucket=bucket_target,
                Key=target_key
            )

            copied_files += 1
            print(f"✅ Copied: s3://{bucket_sources}/{source_key} → s3://{bucket_target}/{target_key}")

    print(f"\n🎉 Done. {copied_files} file(s) copied from {bucket_sources}/{s3_source_rep} to {bucket_target}/{s3_target_rep}")

bucket_sources = "azcdi-us-ops-report-ds-dev" 
bucket_target = "azcdi-us-ops-report-ds-ppd"
s3_source_rep = "inputs/sv_xlsx_templates/"
s3_target_rep = "inputs/sv_xlsx_templates/"
# copy_files(bucket_sources, bucket_target, s3_source_rep, s3_target_rep)

def copy_secretmanager (SourceSecretName, TargetSecretName):
# Initialize Secrets Manager client

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="us-east-1")

    source_secret_id = SourceSecretName
    target_secret_id = TargetSecretName

    # Step 1: Get the value from source secret
    get_response = client.get_secret_value(SecretId=source_secret_id)

    # Use either the SecretString or SecretBinary
    if 'SecretString' in get_response:
        secret_value = get_response['SecretString']
    else:
        # Handle binary secrets if needed
        secret_value = get_response['SecretBinary'].decode('utf-8')

    # Step 2: Put the value into the target secret
    put_response = client.put_secret_value(
        SecretId=target_secret_id,
        SecretString=secret_value
    )
    
    print(f"Copied secret from {source_secret_id} to {target_secret_id}")

SourceSecretName = "azcdi-us-ops-report-ds-secret-dev" 
TargetSecretName = "azcdi-us-ops-report-ds-secret-ppd"
# copy_secretmanager(SourceSecretName, TargetSecretName)

def store_templatetrackingcompletion(interaction: TemplateMaster, table_name:str):
    """
    Store the ReportTrackingCompletion interaction in the REPORTS_DYNAMOTABLE DynamoDB table.

    This function performs the following steps:
    1. Delete any existing records with the same ProductName_User combination.
    2. Insert the new IsprTrackingCompletion interaction into the table.
    3. Invoked from update ispr and Generate ispr APIs.
    
    **Steps Carried Out by the Function:**

    1. Delete any existing records with the same `ProductName_User` combination from the DynamoDB table.
       - Query the table using the `ProductName_User-Timestamp-index` to retrieve existing records with the same `ProductName_User`.
       - Delete each retrieved record using the correct partition key (`SessionID`) and sort key (`Timestamp`).
    2. Insert the new `IsprTrackingCompletion` interaction into the DynamoDB table.
       - Convert the `IsprTrackingCompletion` object to a dictionary using the `dict()` method.
       - Insert the dictionary as a new item in the DynamoDB table using the `put_item()` method.

    **External Function Calls:**

    - `boto3.resource('dynamodb')`: Used to create a DynamoDB resource.
    - `table.query()`: Used to query the DynamoDB table with a specific index and condition expression.
    - `table.delete_item()`: Used to delete an item from the DynamoDB table based on the provided primary key.
    - `table.put_item()`: Used to insert a new item into the DynamoDB table.

    Args:
        interaction (TemplateMaster): The TemplateMaster interaction to be stored.
        tablename (str): The name of the DynamoDB table.

    Returns:
        dict: A success message if the interaction is stored successfully.

    Raises:
        HTTPException: If an exception occurs during the storage process.

    External Function Calls:
        - boto3.resource('dynamodb')
        - table.query()
        - table.delete_item()
        - table.put_item()
    """
    logger.info("\nstore_isprtrackingcompletion ---------- START --------")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    ############## First delete all previous record from the same ProductName&UserID
    response = table.scan(
        FilterExpression=Attr("template_id").eq(interaction.template_id)
    )

    # Step 2: Delete existing records using correct keys
    for item in response.get('Items', []):
        table.delete_item(Key={
            "template_id": item["template_id"]  # Correct partition key
            # "created_at": item["created_at"]   # Correct sort key
        })
    logger.info("store_templatecompletion ---------- Deleted --------")
    #################### Insert New Records
    try:
        item = interaction.dict()
        ############ delete all previous records about the interaction.ProductName_User
        table.put_item(Item=item)
        logger.info("store_templatecompletion ---------- END --------Report Edition interaction stored successfully")
        return {"message": "Template-Edition interaction stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
template_id_ref = "temp_10"
template_id_new = "temp_11"
name = "Heumeni Tientcheu, Fabrice (Cognizant Technology Solutions)"
template_name = "CTU Qualification Template"
template_fullname = "SVR - CTU - Controlled Temperature Units (CTUs) Qualification Template"
pr_id = "kkfx871"
s3_excel_path = "inputs/sv_xlsx_templates/sv_xlsx_template_CTU_Template_rules.xlsx"
s3_path = "inputs/sv_docx_templates/CTU Qualification Template"
doc_types = [ "EE" , "URS" , "EC" , "LC" , "QGEC", "QGLC" , "ODQG" , "PFQG" ,  "QRSOP"]
product_names = [ "Tozorakimab",  "Synagis" , "Rilvegostomig" , "Puxitatug" ]
TEMPLATEMASTER_DYNAMOTABLE = "aig-azcdi-us-ops-report-templatemaster-test"

doc_types_full = [ "Equipment Entity" , "User Requirement Specification" , "Empty Chamber" , "Loaded Chamber" , "Empty Chamber Qualification Graph Report" ,  "Loaded Chamber Qualification Graph Report" ,  "Open Door Test Qualification Graph Report" , "Power Failure Test Qualification Graph Report" , "Qualification and Requalification of Storage Areas and Controlled Temperature Units (CTUs) at FMC" ]

def add_new_template_record(template_id_ref, template_id_new, template_name, template_fullname, pr_id, name, s3_excel_path, s3_path, product_names, doc_types, doc_types_full ):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)   

    # try:
       
    response = table.scan(FilterExpression=(Attr('template_id').eq(template_id_ref)))
    items = response.get("Items", [])
    print(items)

    for item in items:
        print(item)
     # Normalize potential DynamoDB-typed lists
        item["template_id"] = template_id_new
        item["template_name"] = template_name
        item["template_fullname"] = template_fullname
        item["approval_status"] = "Active"
        item["created_by"] = pr_id
        item["created_at"] = datetime.now().isoformat()
        item["doc_types"] = doc_types
        item["doc_types_full"] = doc_types_full
        item["name"] = name
        item["product_names"] = product_names
        item["s3_excel_path"] = s3_excel_path
        item["s3_path"] = s3_path

        template = TemplateMaster(**item)
        store_templatetrackingcompletion(template, TEMPLATEMASTER_DYNAMOTABLE)

# add_new_template_record(template_id_ref, template_id_new, template_name, template_fullname, pr_id, name, s3_excel_path, s3_path, product_names, doc_types, doc_types_full )
                
             
############################


    
def find_files_containing(bucket_name, search_string, prefix=''):
    """
    Returns a list of S3 object keys (filenames) in the bucket that contain the given search string.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        search_string (str): The substring to look for in filenames.
        prefix (str, optional): An optional prefix to narrow the search (like a folder path).

    Returns:
        list: A list of matching S3 keys (filenames).
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    matching_files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if search_string in key:
                matching_files.append(key)

    return matching_files
bucket_name = "azcdi-us-ops-report-ds-prod"
# print(find_files_containing(bucket_name, "sitev-CDF-kjxj423", prefix='outputs'))
    
def fill_report(table_queue_name, table_name, section_name_list, start_date, end_date):
    
    """
    Scans the DynamoDB table for records where Ispr_Editor_Status is True, and updates the status
    to False for records that were edited more than default_n_mins minutes ago.

    This function uses the table.scan method to retrieve all records that match the filter condition
    and updates the Ispr_Editor_Status column for qualifying records using the update_ispr_edit_status
    function.

    If there are many records in the table, the scan operation is performed in batches using pagination.

    Raises:
        ClientError: If an error occurs while scanning or updating the DynamoDB table.
    """
    # logger.info(f"scan_and_ingestion_and_generation executed with default_n_mins = {default_n_mins}, table_name = {table_queue_name}")
    
    try:
        scan_kwargs = {}
        done = False
        start_key = None
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_queue_name)

        while not done:
            logger.info("scan_and_ingestion_and_generation -------1------->")
            # If start_key is present, it means there are more records to be scanned
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key

            # Define a placeholder for the 'Timestamp' attribute.
            # placeholder is needed when one column name equals a keyword.
            timestamp_placeholder = '#TS'
            
            start_timestamp = datetime.fromisoformat(start_date).isoformat() if start_date else None
            end_timestamp = datetime.fromisoformat(end_date).isoformat() if end_date else None

            # Scan the table for records where status is "queued"
            response = table.scan(FilterExpression=(Attr('status_in_queue').eq("completed")) &
                                                    Attr('created_by').ne("kdnq786")  & 
                                                    Attr('created_by').ne("kjxj423")  & 
                                                    Attr('created_by').ne("kthn600")  & 
                                                    Attr('created_by').ne("kxmh288")  &
                                                    Attr('created_at').between(start_timestamp, end_timestamp))
            # logger.info(f"\nscan_and_ingestion_and_generation ----2---table scan response------\n{response}->")
            
            ################## Get the oldest "template_fullname" in Queues with status "queued"         
            response['Items'].sort(
                key=lambda x: datetime.fromisoformat(x["created_at"]),
                reverse=False
            )
            
            # If LastEvaluatedKey is present, it means there are more records to be scanned
            start_key = response.get('LastEvaluatedKey', None)
            done = 'LastEvaluatedKey' not in response
            # logger.info("scan_and_ingestion_and_generation -------3------->")

            # Check if there are any items in the response
            items = response.get('Items', [])
            # logger.info("scan_and_ingestion_and_generation -------4------->")
            if not items:
                logger.info("No records found.")
                continue
                                  
              # Iterate over the scan results
            for item in items:
                logger.info("scan_and_ingestion: Iterating records")
                
                formatted_timestamp = datetime.now().isoformat()
                report_id = item["report_id"]
                created_at =  parser.isoparse(item["created_at"])
                file_version = item["file_version"]
                template_fullname = item["template_fullname"]
                template_name = item["template_name"]
                user_id = item["created_by"]
                name = item["name"]
                emailId = item["emailId"]
                session_id = item["session_id"]
                timestamp = item["created_at"]
                upload_folder = item["upload_folder"]
                pqr_param_json_filename = item["pqr_param_json_filename"]
                product_name = item["product_name"]
                reporting_period = item["reporting_period"]
                source_file_names = item["source_file_names"]
                source_file_types = item["source_file_types"]
                list_versions = item["list_versions"]
                chunk_output_folder = template_name + "_chunk_output"
                logger.info(f"scan_and_ingestion_and_generation -------5.1------->\n{source_file_names}")
                
                strsearch = "sitev" + "-"  + template_name + "-"  + user_id 
                # strsearch = "ispr" + "-" + product_name + "-" + reporting_period + "-" + user_id
                target_filename_list = find_files_containing(bucket_name, strsearch, prefix='outputs')      
                if target_filename_list:                              
                    target_filename = target_filename_list[0]
                    product_name = "Tozorakimab"
                    # product_name = (target_filename.split('-'))[1]
                
                    DetailCompletionStatus = []


                    for section_index, section_name in enumerate(section_name_list):
                        DetailCompletionStatus.append(ReportTrackingSection(section_name=section_name_list[section_index], completion=0))

                    ReportTrackingRes = ReportTrackingCompletion(
                     report_id = report_id,
                     created_by = user_id,
                     name = name,
                     session_id = session_id,
                     created_at = timestamp,
                     file_version = file_version,
                     report_file_path = target_filename,
                     template_name = template_name,
                     template_fullname = template_fullname,
                     product_name = product_name,  ############ Or Molecule Name 
                     pqr_param_json_filename = pqr_param_json_filename,
                     reporting_period = reporting_period,
                     edit_status = "Ready For review",
                     completion_detail_section=DetailCompletionStatus,
                     completion = 0,
                     updated_at = "",
                     edited_by = "",
                     locked = False,
                     source_file_names = source_file_names,
                     site_names = source_file_types,
                     report_title = template_name + "_" +  product_name + "_" + "v" + str(file_version),
                     html_content = "<html></html>"
                )

                    store_reporttrackingcompletion(ReportTrackingRes, table_name)
    except Exception as e:
        logger.error(f"startup error: {str(e)}")            



# start_date = "2025-05-30"
# end_date = "2025-07-21"

start_date = "2025-03-01"
end_date =  "2025-07-21"

section_name_list = ["FirstPage", "ABSTRACT", "INTRODUCTION", "RESULTS", "ANALYSIS", "CONCLUSION"]
# section_name_list = ["SUMMARY AND CONCLUSION", "BATCHES REVIEWED (APPROVED AND REJECTED)", "REPROCESSED & REWORKED BATCHES", "PRODUCT REVIEWS FROM PREVIOUS MANUFACTURING STAGE", "STARTING AND PACKAGING MATERIALS", "ANALYTICAL DATA", "CHANGES", "STABILITY DATA", "DEVIATIONS", "DEVIATIONS (QUALITY EVENTS)", "COMPLAINTS (PRODUCT QUALITY)", "RECALLS, STOCK RECOVERIES, FIELD ALERTS", "RETURNED AND SALVAGED GOODS", "CONTRACTUAL AGREEMENTS / ARRANGEMENTS", "QUALIFICATION STATUS OF RELEVANT EQUIPMENTAND UTILITIES", "OTHER"]
# fill_report("aig-azcdi-us-ops-report-queue-prod", "aig-azcdi-us-ops-report-reports-prod", section_name_list, start_date, end_date)

############### Clean S3 Repository manually

# import boto3
# from datetime import datetime

# s3 = boto3.client("s3")
# bucket = "azcdi-us-ops-report-ds-test"
# target_date = datetime(2025, 9, 3).date()

# response = s3.list_objects_v2(Bucket=bucket)


# paginator = s3.get_paginator("list_objects_v2")
# for page in paginator.paginate(Bucket=bucket, Prefix="outputs/"):
#     for obj in page.get("Contents", []):
#         if obj["LastModified"].date() <= target_date and "kdnq786" in obj["Key"]:
#             print("Deleting:", obj["Key"])
#             s3.delete_object(Bucket=bucket, Key=obj["Key"])


#####################
# def download_template_docx(s3_path: str, bucket_name: str, expiration: int = 3600) -> str:
#     """
#     Generate a pre-signed URL to download a DOCX file from S3.

#     Args:
#         s3_path (str): S3 key (path) to the DOCX file.
#         bucket_name (str): S3 bucket name.
#         expiration (int): URL expiration time in seconds.

#     Returns:
#         str: Pre-signed URL for downloading the file.

#     Raises:
#         HTTPException: On AWS errors or URL generation failure.
#     """
#     try:
#         s3_client = boto3.client("s3")
#         response = s3_client.generate_presigned_url(
#             'get_object',
#             Params={'Bucket': bucket_name, 
#                     'Key': s3_path,
#                     "ResponseContentDisposition": f'inline; filename="{s3_path.split("/")[-1]}"',
#                     "ResponseContentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
#                  },
#             ExpiresIn=expiration
#         )
#         return response
#     except ClientError as e:
#         logger.error(f"Failed to generate pre-signed URL: {e}")
#         raise HTTPException(status_code=500, detail="Error generating download URL.")
#     except Exception as e:
#         logger.error(f"Unexpected error during download: {e}")
#         raise HTTPException(status_code=500, detail="Unexpected error during download request.")

# s3_path = "inputs/sv_docx_templates/Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations.docx"
# bucket_name = "azcdi-us-ops-report-ds-dev"
# expiration_ = 86400
# print(download_template_docx(s3_path, bucket_name, expiration_))

################################# Add a new Attribut in DynamoDB

# Define source and target tables
# created_at, datetime.now().isoformat()
def add_new_var_tables (TableName, part_key, new_var_name, new_var_value):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)


    # Step 1: Scan the source table
    response = table.scan()
    items = response['Items']

     # Continue scanning if there are more pages
    while 'LastEvaluatedKey' in response:
        response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    for item in items:
    # Add new attribute
        if "temp" in item[part_key]:
            table.update_item(
                Key={part_key: item[part_key]},
                UpdateExpression=f'SET {new_var_name} = :val',
                ExpressionAttributeValues={':val': new_var_value }
            )
        else:
             table.update_item(
                Key={part_key: item[part_key]},
                UpdateExpression=f'SET {new_var_name} = :val',
                ExpressionAttributeValues={':val':new_var_value}
            )

def delete_var_tables (TableName, part_key, var_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)


    # Step 1: Scan the source table
    response = table.scan()
    items = response['Items']

     # Continue scanning if there are more pages
    while 'LastEvaluatedKey' in response:
        response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    for item in items:
    # Add new attribute
        if "user_group" in item:
            table.update_item(
                Key={part_key: item[part_key]},
                UpdateExpression='REMOVE user_group',
            )
      
   


TableName = "aig-azcdi-us-ops-report-users-dev" 
part_key = "pr_id"
new_var_name = "user_group" 
new_var_value = ["End User", "Super User", "BPO", "PO"]
# delete_var_tables (TableName, part_key, new_var_name)
add_new_var_tables (TableName, part_key, new_var_name, new_var_value)
def get_template_record_by_template_id(template_id: str) -> TemplateMaster:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TEMPLATEMASTER_DYNAMOTABLE)

    # Scan for record with matching s3_path (assuming s3_path is unique)
    response = table.scan(
        FilterExpression=Attr("template_id").eq(template_id)
    )
    items = response.get("Items", [])
    if not items:
        return None
    # Assuming only one record per s3_path
    return items[0]
TEMPLATEMASTER_DYNAMOTABLE = "aig-azcdi-us-ops-report-templatemaster-dev"
# record = get_template_record_by_template_id("temp_10")
# print(record["sharepoint_link"])

# ==========================================================
# AUTHENTICATION
# ==========================================================
def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    """Obtain Microsoft Graph API token via client credentials flow."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    response = requests.post(token_url, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"🛑 Failed to get token: {response.status_code} {response.text}")
        return None

# ==========================================================
# HELPER — EXTRACT RELATIVE PATH FROM LINK
# ==========================================================
def extract_server_relative_url(full_url: str | None, main_url: str | None) -> str:    

    if main_url in full_url:
        path = full_url.split(main_url, 1)[1]
    else:
        path = full_url
    return path
    
# ==========================================================
# GRAPH CALL
# ==========================================================
def get_file_metadata_via_graph(site_url: str, token: str, file_rel_url: str) -> Optional[dict]:
    """Get file metadata, including the SharePoint-specific UniqueId (GUID)."""
    try:
        decoded_path = urllib.parse.unquote(file_rel_url).strip('/')
        parts = decoded_path.split('/')

        # --- Path Parsing ---
        site_name = None
        site_path = None
        site_index = -1
        for i, part in enumerate(parts):
            if part.lower() == 'sites' and i + 1 < len(parts):
                site_name = parts[i + 1]
                site_index = i
                # Get the site-relative path (e.g., /sites/SCTASK1629542)
                site_path = '/' + '/'.join(parts[:i+2])
                break
        
        if not site_name:
            raise ValueError("Could not extract site name. Check URL structure.")

        library_start_index = site_index + 2 
        library_name = parts[library_start_index]
        file_path_segments = parts[library_start_index + 1:]
        
        if not library_name or not file_path_segments:
             raise ValueError("Could not extract document library or file path.")
             
        file_path = '/'.join(file_path_segments)
        
        print(f"📂 Site: {site_name}")
        print(f"📁 Library: {library_name}")
        print(f"📄 Decoded File path: {file_path}")

        # --- API Setup ---
        hostname = site_url.replace('https://', '').replace('http://', '')
        site_graph_path = f"sites/{hostname}:/sites/{site_name}"
        drives_url = f"https://graph.microsoft.com/v1.0/{site_graph_path}:/drives"
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        # 1. Get Drive ID
        drives_resp = requests.get(drives_url, headers=headers)
        if drives_resp.status_code != 200:
            print(f"🛑 Error getting drives: {drives_resp.text}")
            return None

        drives = drives_resp.json().get('value', [])
        drive = next((d for d in drives if d.get('name', '').lower() == library_name.lower()), None)
        if not drive:
            raise ValueError(f"Could not find document library '{library_name}'")

        drive_id = drive['id']
        
        # 2. Get File Metadata and SharePoint IDs
        encoded_file_path = urllib.parse.quote(file_path, safe='/') 
        
        # 💥 CRITICAL FIX: Explicitly request the sharepointIds facet to get the UniqueId/GUID
        file_metadata_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_file_path}:/?$select=id,sharepointIds"
        
        file_resp = requests.get(file_metadata_url, headers=headers)

        if file_resp.status_code == 200:
            data = file_resp.json()
            
            # The 'UniqueId' property is the GUID needed for the Doc.aspx sourcedoc parameter.
            sharepoint_guid = data.get('sharepointIds', {}).get('listItemUniqueId')
            
            if not sharepoint_guid:
                print("⚠️ Warning: Could not find listItemUniqueId. Falling back to DriveItem ID.")
                sharepoint_guid = data.get('id')
            
            return {
                'id': sharepoint_guid, # Use the SharePoint Unique ID
                'site_path': site_path, # e.g., /sites/SCTASK1629542
                'name': data.get('name'),
            }
        else:
            print(f"🛑 Graph API Error {file_resp.status_code}: {file_resp.text}")
            return None

    except Exception as e:
        print(f"🛑 Error getting file metadata: {e}")
        return None

# ==========================================================
# WOPI LINK GENERATOR
# ==========================================================
# def generate_sharepoint_wopi_link_with_graph(site_url: str, full_url: str, token: str, action: str) -> Optional[str]:
#     """Generate WOPI edit/view link from a full SharePoint link."""
#     file_rel_url = extract_server_relative_url(full_url, site_url)
#     print(f" Extracted file relative URL: {file_rel_url}")

#     metadata = get_file_metadata_via_graph(site_url, token, file_rel_url)
#     if not metadata:
#         return None

#     web_url = metadata.get('webUrl', '')
#     if web_url:
#         if action == 'edit':
#             return f"{web_url}?web=1&action=edit" if '?web=1' not in web_url else web_url.replace('?web=1', '?web=1&action=edit')
#         else:
#             return web_url if '?web=1' in web_url else f"{web_url}?web=1"
#     return None

def generate_embed_view_url(site_url: str, full_url: str, token: str) -> Optional[str]:
    """
    Generates the Doc.aspx?sourcedoc={GUID}&action=embedview link.
    """
    file_rel_url = extract_server_relative_url(full_url, site_url)
    print(f"🔍 Extracted file relative URL for embed: {file_rel_url}")

    metadata = get_file_metadata_via_graph(site_url, token, file_rel_url)
    if not metadata or not metadata.get('id') or not metadata.get('site_path'):
        print("🛑 Failed to get file GUID or site path.")
        return None

    # The 'id' is now the SharePoint UniqueId (GUID)
    file_guid = metadata['id']
    site_path = metadata['site_path']
    
    # Example Target: https://azcollaborationtst.sharepoint.com/sites/SCTASK1629542/_layouts/15/Doc.aspx?sourcedoc={GUID}&action=embedview
    
    # 1. Construct the base path
    base_embed_url = f"{site_url}{site_path}/_layouts/15/Doc.aspx"
    
    # 2. Construct the final URL (GUID must be wrapped in curly braces)
    final_embed_url = (
        f"{base_embed_url}?"
        f"sourcedoc={{{file_guid}}}"
        f"&action=embedview"
    )

    return final_embed_url

############################# Get graph token using Certification

# ==========================================================
# BUILDING CLIENT ASSERTION HEADER PAYLOAD SIGNATURE
# ==========================================================

# --- Helper: base64url without padding ---
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

def build_client_assertion(client_id: str, tenant_id: str, private_key_pem: bytes, x5t_b64url: str | None = None) -> str:
    now = int(time.time())
    exp = now + 600  # 10 minutes
    jti = str(uuid.uuid4())
    aud = f"https://login.microsoftonline.com/{tenant_id}/v2.0"  # AAD v2 token endpoint audience

    header = {"alg": "RS256", "typ": "JWT"}
    if x5t_b64url:
        header["x5t"] = x5t_b64url  # optional

    payload = {
        "iss": CLIENT_ID,
        "sub": CLIENT_ID,
        "aud": aud,
        "jti": jti,
        "nbf": now,
        "iat": now,
        "exp": exp,
    }

    header_b64 = b64url(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")

    private_key = serialization.load_pem_private_key(private_key_pem, password=None)
    signature = private_key.sign(
        signing_input,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    signature_b64 = b64url(signature)

    return f"{header_b64}.{payload_b64}.{signature_b64}"

# ==========================================================
# AUTHENTICATION USING CERT
# ==========================================================
def get_graph_token_using_cert(tenant_id, client_id,  s3_private_key, thumbprint) -> Optional[str]:

       # Option 1: private key string (PEM)
    # --- Step 1: Download the PEM private key from S3 ---
    s3 = boto3.client("s3")
    pem_object = s3.get_object(Bucket=BUCKET_NAME, Key=s3_private_key)
    private_key_pem = pem_object["Body"].read().decode("utf-8")
    
    cert = {
        "private_key": private_key_pem,
        # the cert thumbprint (hex, no spaces, uppercase or lowercase fine)
        "thumbprint": thumbprint
        # optionally: "public_certificate": "<PEM public cert>" to send x5c header
    }
    
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=cert
    )
    
    # Acquire token for Graph app-only:
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    if "access_token" in result:
        print("access_token:", result["access_token"])
        return result["access_token"]
    else:
        print("error:", result.get("error"), result.get("error_description"))
        return None

site_url = "https://azcollaborationtst.sharepoint.com"
full_url = "https://azcollaborationtst.sharepoint.com/sites/SCTASK1629542/reporting-assistant-dev/templates/Report%209%20-%20Membrane%20Lifetime%20Process%20Validation%20Template.docx?d=wedc71190b41f49cc87318d64206ce0af&csf=1&web=1&e=jLngk9"
full_url = "https://azcollaborationtst.sharepoint.com/sites/SCTASK1629542/reporting-assistant-dev/templates/Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations.docx"
action = "view"

############# Temporary
CLIENT_ID = "629d5229-e678-4f0b-93a8-ee977c6c6371"
CLIENT_SECRET = "hDn8Q~kIYAFVJi3De5Dn1CKnUkfWANte9d0gBcXq"
TENANT_ID = "018bdb4e-8280-4a61-a267-c21b54eb9f58"
########### Test get token using certification
CERT_THUMBPRINT = "186D04B1CD4E31BD0033FC01273F95957935DFA0"
PRIVATE_KEY_FILE = "/home/sagemaker-user/aig-azcdi-us-ops-report-api-webapp/app/app.key"
PRIVATE_KEY_FILE  = "inputs/cert_key_ppk/app.key"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPES = "https://graph.microsoft.com/.default"
# BUCKET_NAME = 'azcdi-us-ops-report-ds-dev'

########### Dev Sharepoint
site_url = "https://azcollaboration.sharepoint.com"
full_url = "https://azcollaboration.sharepoint.com/sites/TSP1041/reporting-assistant-ppd/templates/Report%205%20-%20Process%20Validation%20Report%20Template%20-%20Concentration%20and%20Diafiltration%20Formulation%20Filtration.docx"
CLIENT_ID = "ce6eefee-cdb0-4d43-b3cb-ba563914fb25"
CLIENT_SECRET = "Pn68Q~pSGxqkmlYt1TaIgEveCgV1wnVtRazfVaYo"
TENANT_ID = "af8e89a3-d9ac-422f-ad06-cc4eb4214314"
########### Test get token using certification
CERT_THUMBPRINT = "C14FEB67B406536684E2B94F81249A2F98BCADA8"
PRIVATE_KEY_FILE  = "inputs/cert_key_ppk/ops-report-test.aig.astrazeneca.net.key"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPES = "https://graph.microsoft.com/.default"
BUCKET_NAME = 'azcdi-us-ops-report-ds-ppd'
SHAREPOINT_MAIN_URL = "https://azcollaboration.sharepoint.com"
SP_SITE_NAME = "TSP1041"
SP_DOCUMENT_LIBRARY = "reporting-assistant-ppd"

# token = get_graph_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
# token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID,  PRIVATE_KEY_FILE, CERT_THUMBPRINT)
token = get_graph_token_using_cert("af8e89a3-d9ac-422f-ad06-cc4eb4214314", "ce6eefee-cdb0-4d43-b3cb-ba563914fb25",  "inputs/cert_key_ppk/ops-report-test.aig.astrazeneca.net.key", "C14FEB67B406536684E2B94F81249A2F98BCADA8")
print(f"token: {token}")
link = generate_embed_view_url(site_url, full_url, token)
print(link)

########### Test get token using certification
CERT_THUMBPRINT = "186D04B1CD4E31BD0033FC01273F95957935DFA0"
# PRIVATE_KEY_FILE = "/home/sagemaker-user/aig-azcdi-us-ops-report-api-webapp/app/app.key"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPES = "https://graph.microsoft.com/.default"
# print(get_graph_token_using_cert(TENANT_ID, CLIENT_ID, AUTHORITY, PRIVATE_KEY_FILE, CERT_THUMBPRINT))

################################# Push S3 to Sharepoint Experiment
# --- SharePoint config ---

CLIENT_ID = "629d5229-e678-4f0b-93a8-ee977c6c6371"
CLIENT_SECRET = "hDn8Q~kIYAFVJi3De5Dn1CKnUkfWANte9d0gBcXq"
TENANT_ID = "018bdb4e-8280-4a61-a267-c21b54eb9f58"

SHAREPOINT_MAIN_URL = "https://azcollaborationtst.sharepoint.com"
SP_SITE_NAME = "SCTASK1629542"
SP_DOCUMENT_LIBRARY = "reporting-assistant-dev"
BUCKET_NAME = 'azcdi-us-ops-report-ds-dev'

object_key = 'inputs/sv_docx_templates/HDocument.docx'  # optional


sharepoint_site =  f"{SHAREPOINT_MAIN_URL}/sites/{SP_SITE_NAME}"
document_library = SP_DOCUMENT_LIBRARY
s3_filename = object_key.split("/")[-1]
destination_path = f"templates/{s3_filename}"


def push_s3_to_sharepoint(object_key: str):
    # ==== STEP 1: GET FILE FROM S3 ====
    s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=object_key)
    file_data = s3_object["Body"].read()
    
    # ==== STEP 2: AUTHENTICATE WITH MICROSOFT GRAPH ====
    # access_token = get_graph_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    access_token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID,  PRIVATE_KEY_FILE, CERT_THUMBPRINT)
    
    # ==== STEP 3: GET SITE AND DRIVE ID ====
    hostname = SHAREPOINT_MAIN_URL.replace('https://', '').replace('http://', '')
    site_path = f"/sites/{SP_SITE_NAME}"
    
    try:
          # 1. Get SITE ID
        headers = {"Authorization": f"Bearer {access_token}"}
        site_info = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
            headers=headers
        )
        if site_info.status_code != 200:
            raise ValueError(f"Could not find site name '{SP_SITE_NAME}'")    
        
        site_id = site_info.json()["id"]
    
        # 2. Get Drive ID
        drives = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)
        drive_data = drives.json()    
        drive_id = None
        for d in drive_data["value"]:
            if d["name"].lower() == document_library.lower():
                drive_id = d["id"]
                break
        
    # ==== STEP 4: UPLOAD FILE TO SHAREPOINT ====
        upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{quote(destination_path)}:/content"
        upload_headers = {"Authorization": f"Bearer {access_token}"}
        upload_response = requests.put(upload_url, headers=upload_headers, data=file_data)
        
        if upload_response.status_code in (200, 201):
            logger.info("✅ File uploaded successfully to SharePoint!")
        else:
            logger.info("❌ Upload failed:", upload_response.status_code, upload_response.text)
    
    except Exception as e:
        logger.error(f"🛑 Error getting file metadata: {e}")

from urllib.parse import unquote, urlparse

def extract_sharepoint_filename(url):
    # Parse the URL
    parsed = urlparse(url)
    # Take the last part of the path (after the final "/")
    raw_filename = parsed.path.split("/")[-1]
    # Decode any URL-encoded characters (e.g. %20 -> space)
    return unquote(raw_filename)

def push_sharepoint_to_s3(sharepoint_url: str):
    # ==== STEP 1: GET the file name from ur ====
    file_name = extract_sharepoint_filename(sharepoint_url)
    print(file_name)
    
    # ==== STEP 2: AUTHENTICATE WITH MICROSOFT GRAPH ====
    # access_token = get_graph_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    access_token = get_graph_token_using_cert(TENANT_ID, CLIENT_ID,  PRIVATE_KEY_FILE, CERT_THUMBPRINT)
    
    # ==== STEP 3: GET SITE ID, DRIVE ID and ITEM_ID ====
    hostname = SHAREPOINT_MAIN_URL.replace('https://', '').replace('http://', '')
    site_path = f"/sites/{SP_SITE_NAME}"
    
    try:
          # 1. Get SITE ID
        headers = {"Authorization": f"Bearer {access_token}"}
        site_info = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
            headers=headers
        )
        if site_info.status_code != 200:
            raise ValueError(f"Could not find site name '{SP_SITE_NAME}'")    
        
        site_id = site_info.json()["id"]
    
        # 2. Get Drive ID
        drives = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)
        drive_data = drives.json()    
        drive_id = None
        for d in drive_data["value"]:
            if d["name"].lower() == document_library.lower():
                drive_id = d["id"]
                break

        ## 3: Get folder ID for 'templates'
        folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/templates"
        folder_resp = requests.get(folder_url, headers=headers)
        folder_resp.raise_for_status()
        folder_id = folder_resp.json()["id"]
        print(folder_id)

        # 3. Get Item ID
        # print(file_name)
        # file_item = get_file_item(site_id, drive_id, folder_id, file_name, access_token)
        # item_id = file_item["id"]
        # print(file_item)
        # print(item_id)

      # Download from SharePoint
        graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/templates/{file_name}:/content"
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
    
        # Upload to S3
        s3 = boto3.client('s3')
        s3_key = f"inputs/sv_docx_templates/{file_name}"        
        s3.upload_fileobj(BytesIO(response.content), BUCKET_NAME, s3_key)
        print(f"Uploaded {s3_key} to {BUCKET_NAME}")
    except Exception as e:
        logger.error(f"🛑 Error getting file metadata: {e}")

# Example usage
    
sharepoint_url = "https://azcollaborationtst.sharepoint.com/sites/SCTASK1629542/reporting-assistant-dev/templates/HDocument_Fabrice.docx"

# push_s3_to_sharepoint(object_key)
# push_sharepoint_to_s3(sharepoint_url)



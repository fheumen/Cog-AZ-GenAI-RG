
import boto3
import docx
import io
# from io import BytesIO
import json
import time
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

s3 = boto3.client("s3")
bucket_name = "azcdi-us-ops-report-ds-dev"
##################### Hyperlink implementation START ####################

def add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color):
    """
    Adds a segment of text to the paragraph, optionally with highlight
    and formatting copied from the original placeholder run.
    Preserves hyperlink if the placeholder run was inside one.

    Args:
        paragraph (docx.text.paragraph.Paragraph): The paragraph object to add the text to.
        text (str): The text to be added to the paragraph.
        is_highlighted (bool): Whether the text should be highlighted or not.
        placeholder (str, optional): The placeholder text in the original document.
        original_runs (list): A list of tuples containing the original text and run objects.
        highlight_color (str): The color to highlight the text with (e.g., 'yellow', 'green').

    Returns:
        None
    """
    run_to_clone = None
    r_id = None

    if placeholder:
        for orig_text, orig_run in original_runs:
            if placeholder in orig_text:
                run_to_clone = orig_run
                try:
                    if run_in_hyperlink(orig_run):
                        r_id = get_hyperlink_rid(orig_run)
                except Exception as e:
                    logger.warning(f"Failed to detect hyperlink on run: {e}")
                break

    try:
        if r_id:
            # Add new run inside hyperlink
            run = add_hyperlink_run(paragraph, text, r_id)
        else:
            run = paragraph.add_run(text)
        # logger.info(f"Added run: '{text}' (highlight={is_highlighted})")
    except Exception as e:
        logger.error(f"Failed to add run: {e}")
        return

    # Clone formatting
    if run_to_clone:
        try:
            clone_run_format(run_to_clone, run)
            # logger.info(f"Cloned formatting from run for placeholder: {placeholder}")
        except Exception as e:
            logger.warning(f"Error cloning format: {e}")

    # Highlight if needed
    if is_highlighted:
        try:
            highlight_text(run, highlight_color)
            # logger.info(f"Applied highlight to '{text}'")
        except Exception as e:
            logger.warning(f"Highlighting failed for text: '{text}': {e}")


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

    
def highlight_text(run, color=HIGHLIGHT_COLOR):
    """
    Highlights the given run with the specified color.
    Valid colors: yellow, green, cyan, magenta, red, etc.

    Args:
        run (python-docx.text.Run): The run object to be highlighted.
        color (str, optional): The color to use for highlighting. Defaults to HIGHLIGHT_COLOR.

    Returns:
        None
    """
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

def get_full_text(paragraph):
    """
    Concatenates and returns all run texts from the paragraph.
    """
    # logger.info("Collecting full paragraph text from runs.")
    return ''.join(run.text for run in paragraph.runs if run.text)


def build_segments(full_text, replacements):
    """
    Constructs a list of segments:
    Each segment is a tuple (text, is_highlighted, original_placeholder).

    This allows us to recreate the paragraph with highlights applied only
    to replacement values.

    Args:
        full_text (str): The original text.
        replacements (dict): A dictionary mapping placeholders to replacement values.

    Returns:
        list: A list of segments, where each segment is a tuple (text, is_highlighted, original_placeholder).
    """
    logger.info("Building segments for replacement and highlight.")
    segments = []
    idx = 0
    while idx < len(full_text):
        matched = False
        # Try longer placeholders first to prevent partial matches
        for placeholder, value in sorted(replacements.items(), key=lambda x: -len(x[0])):
            if should_replace(placeholder, value) and full_text.startswith(placeholder, idx):
                # Replace the placeholder with the value and mark it as highlighted
                segments.append((value, True, placeholder))
                idx += len(placeholder)
                matched = True
                break
        if not matched:
            # No placeholder matched, add the character as is
            segments.append((full_text[idx], False, None))
            idx += 1
    return segments

def clear_paragraph_runs(paragraph):
    """
    Safely clears all runs from the given paragraph.

    Args:
        paragraph (Paragraph): The paragraph object from which to clear runs.

    Returns:
        None

    """
    logger.info("Clearing original runs in paragraph.")
    for run in paragraph.runs:
        try:
            # Clear the run
            run.clear()
        except Exception as e:
            # Log any exceptions that occur during the clearing process
            logger.warning(f"Failed to clear run: {e}")

#####################################################    

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


def get_hyperlink_rid(run):
    """
    Retrieves the relationship ID (r:id) from the hyperlink containing this run.

    Args:
        run (docx.text.run.Run): The run inside a hyperlink.

    Returns:
        str: The relationship ID if found, else None.
    """
    hyperlink_elem = run._element.getparent()
    return hyperlink_elem.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')


def add_hyperlink_run(paragraph, text, r_id):
    """
    Adds a hyperlink run (w:hyperlink) with given r:id and text to the paragraph XML.
    Returns the hyperlink XML element for further formatting.
    """
    if "toc" in r_id.lower():
        hlink_ = paragraph._p.xpath('.//w:hyperlink')
        print(hlink_)
        if len(hlink_) > 0:
            hlink = OxmlElement('w:hyperlink')
            hlink = hlink_[0]
            print(hlink)
            t_nodes = hlink.xpath('.//w:t')
            t_nodes[0].text = text
            # Clear remaining t_nodes if split across multiple runs
            for t in t_nodes[1:]:
                # print(t.text)
                t.text = ""

                
    else:
        hlink = OxmlElement('w:hyperlink')
        hlink.set(qn('r:id'), r_id)
    
        new_run = OxmlElement('w:r')
        rPr = OxmlElement('w:rPr')  # Run properties container
        new_run.append(rPr)
    
        t = OxmlElement('w:t')
        t.text = text
        new_run.append(t)
    
        hlink.append(new_run)
        paragraph._p.append(hlink)

    return hlink  # return hyperlink XML element, not a Run object

# New one
def highlight_hyperlink_run(hyperlink_elem, highlight_color, text_to_highlight):
    """
    Adds highlight to the <w:rPr> of the first <w:r> inside the hyperlink XML element.
    """
    ns = hyperlink_elem.nsmap
    r_element = hyperlink_elem.find('.//w:r', namespaces=ns)
    if r_element is not None:
        for r in r_element:
            r_wt = r_element.find('.//w:t', namespaces=ns)
            if text_to_highlight in r_wt.text:
                rPr = r.find('w:rPr', namespaces=ns)
                if rPr is None:
                    rPr = OxmlElement('w:rPr')
                    r.insert(0, rPr)
        
                highlight = OxmlElement('w:highlight')
                highlight.set(qn('w:val'), highlight_color)
                rPr.append(highlight)

def highlight_subtext_in_hyperlink(hlink, substring, color_hex="00B050"):
    """
    Highlight a substring inside a hyperlink in green without losing the link.
    """
    ns = hlink.nsmap
    print(hlink)
    for r in hlink.xpath('.//w:r'):
        t = r.find('.//w:t', namespaces=ns)
        if t is not None and t.text and substring in t.text:
            before, match, after = t.text.partition(substring)

            # Update current run to keep "before"
            t.text = before if before else match

            # If there is a match and we had a "before" part, create a new run for the match
            if before and match:
                match_run = OxmlElement("w:r")
                match_t = OxmlElement("w:t")
                match_t.text = match
                match_run.append(match_t)

                # add formatting <w:rPr><w:color/>
                rPr = OxmlElement("w:rPr")
                highlight = OxmlElement("w:highlight")
                highlight.set(qn("w:val"), color_hex)
                rPr.append(highlight)                
                match_run.insert(0, rPr)

                # insert after the current run
                r.addnext(match_run)
                r = match_run  # move pointer to new run

            elif not before:  # substring starts at beginning, format current run
                rPr = r.find(qn("w:rPr"))
                if rPr is None:
                    rPr = OxmlElement("w:rPr")
                    r.insert(0, rPr)
                color = rPr.find(qn("w:color"))
                if color is None:
                    color = OxmlElement("w:color")
                    rPr.append(color)
                color.set(qn("w:val"), color_hex)

            # If there’s text after, create a new run for it
            if after:
                after_run = OxmlElement("w:r")
                after_t = OxmlElement("w:t")
                after_t.text = after
                after_run.append(after_t)
                r.addnext(after_run)



def replace_and_highlight_paragraph(paragraph, replacements, highlight_color=HIGHLIGHT_COLOR):
    """
    Replaces placeholders in a paragraph with values, preserving formatting and hyperlinks.
    - If a placeholder is inside a hyperlink, it replaces it and retains the hyperlink.
    - Highlights the replacement text.
    """
    if not paragraph or not hasattr(paragraph, '_p'):
        logger.info("Invalid paragraph or missing XML.")
        return

    has_text = any(run.text for run in paragraph.runs) or any(
        el.tag.endswith('hyperlink') for el in paragraph._p
    )
    if not has_text:
        logger.info("Paragraph has no runs or hyperlinks with text.")
        return

    if not replacements or not isinstance(replacements, dict):
        logger.info("No valid replacements dictionary provided.")
        return

    # Step 1: Handle placeholders inside hyperlinks
    for child in list(paragraph._p):  # Direct access to paragraph XML children
            # hyperlink_rel_id = hyperlink.get(qn("r:id"))
        if child.tag.endswith('hyperlink'):
            hyperlink_text = paragraph.text
            # for r in child.findall('.//w:r', namespaces=child.nsmap):
            #     text_elem = r.find('.//w:t', namespaces=child.nsmap)
            #     if text_elem is not None and text_elem.text:
            #         hyperlink_text += text_elem.text

            # Check if any placeholder is inside the full hyperlink text
            for placeholder, value in replacements.items():
                if should_replace(placeholder, value) and placeholder in hyperlink_text:
                    print(hyperlink_text)
                    hlink = paragraph._p.xpath('.//w:hyperlink')
                    if len(hlink) > 0:
                        hlink = hlink[0]
                        # r_id = child.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                        r_id_ext = hlink.get(qn("r:id"))
                        r_id_in = hlink.get(qn("w:anchor"))
                        r_id = r_id_ext if r_id_ext is not None else  r_id_in
                        # print(r_id)
                        if not r_id:
                            logger.info("Hyperlink has no external and internal r:id; skipping.")
                            break
                        
                        # Remove the old hyperlink element        
                        # Insert the new hyperlink with replacement text
                        new_text = hyperlink_text.replace(placeholder, value)
                        print(new_text)
                        new_hyperlink = add_hyperlink_run(paragraph, new_text, r_id)
                        # highlight_hyperlink_run(new_hyperlink, highlight_color, value)
                        highlight_subtext_in_hyperlink(new_hyperlink, value, highlight_color)
        
                        break  # Only handle one placeholder per hyperlink

    # Step 2: Handle normal (non-hyperlink) runs
    normal_runs = [
        run for run in paragraph.runs
        if not run_in_hyperlink(run) and run._element.get(qn('w:dummy')) != 'true'
    ]

    if not normal_runs:
        return

    original_runs = [(run.text, run) for run in normal_runs if run.text]
    full_text = ''.join(run.text for run in normal_runs)

    if not full_text.strip():
        return

    segments = build_segments(full_text, replacements)

    if not any(seg[1] for seg in segments):  # No replacements found
        return

    # Clear normal runs
    for run in normal_runs:
        try:
            run.clear()
        except Exception as e:
            logger.warning(f"Failed to clear run: {e}")

    # Add new segments with formatting/highlighting
    for text, is_highlighted, placeholder in segments:
        add_segment_run(paragraph, text, is_highlighted, placeholder, original_runs, highlight_color)

##################### Hyperlink implementation END ######################

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
    # print(tables)
    for key, value in data.items():
        # logger.info(f"\nsitev_generate_report_from_template ------ 1 ------placeholder / key: {key}")
        # logger.info(f"sitev_generate_report_from_template ------ 2 ------ key: {value}")
        placeholder = f"{key}"

        for paragraph in doc.paragraphs:
            # print(get_full_text(paragraph))
            # if "Table 1-1:" in paragraph.text:
            #     print("titititititititititi")
            #     print(paragraph.text)
            if any(k in paragraph.text for k in data.keys()):
                # if "Table 1-1:" in paragraph.text:
                #     print(paragraph.text)
                #     print(paragraph)
                #     print(get_full_text(paragraph))
                # logger.info(f"sitev_generate_report_from_template:Processing paragraph: '{paragraph.text[:50]}'")
                replace_and_highlight_paragraph(paragraph, data)

        # for paragraph in doc.paragraphs:
        #     for run in paragraph.runs:
        #         if any(k in run.text for k in data.keys()):
        #             # logger.info(f"sitev_generate_report_from_template:Processing paragraph: '{paragraph.text[:50]}'")
        #             replace_and_highlight_paragraph(paragraph, data)

        for ind_tab, table in enumerate(doc.tables):
            for row in table.rows:
                for cell in row.cells:
                    for paragraph in cell.paragraphs:
                        # if ind_tab == 6:
                        #     print(paragraph.text)
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

output_folder = "output_sv_2/"
template_name = "InoculumExpansion"
user_id = "kkfxh"
ispr_filename = "sitev" + "-"  + template_name + "-"  + user_id + ".docx"
target_filename = f"{output_folder}{ispr_filename}"
# data = {"Validation Number": "VX-715101-PVP",\
#               "Molecule Name": "Tozorakimab",\
#               "Building": "633",\
#               "Document Number":"PRO-0187549" 
#              }

data = {"<Chromatography_Type>": "Protein A"}
bucket_name = "azcdi-us-ops-report-ds-test"
docx_s3_key = "inputs/sv_docx_templates/Report 8 - Resin Lifetime Process Validation Template.docx"
# print(docx_s3_key)
sitev_generate_report_from_template(bucket_name, docx_s3_key, target_filename, data)
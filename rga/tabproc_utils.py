import boto3
import docx
import io
# from io import BytesIO
import json
import time
from pathlib import Path
from boto3.dynamodb.conditions import Attr
from io import BytesIO
import pandas as pd
import numpy as np
import pdfplumber
from typing import Optional, List, Dict
import PyPDF2
from loguru import logger
from typing import Optional, List, Dict
from datetime import datetime
import re

# Document Handling - docx
from docx.image.exceptions import UnrecognizedImageError
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.run import Run
from docx.shared import Inches, Pt
from docx.oxml.ns import qn
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx import Document as Document_docx
from rapidfuzz import fuzz
# from fuzzywuzzy import fuzz
from openpyxl import load_workbook

s3 = boto3.client("s3")
# bucket_name = "azcdi-us-ops-report-ds-dev"



###########For Testing
# rules_fig = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 3", "Figure 4", "Figure 5"]}
# pdf_s3_key = "tmp_test5/MAIN_(PRO-0187549) - VX-715101-PVP Tozorakimab Process Validation Vial Thaw through Seed Bioreactor.pdf"
# s3_destination_folder = "temp_kklc575/tmp_test5/final_reports"
# rules_fig = {"<Figure 1:>": ["Figure 2"],  "<Figure 2:>": ["Figure 3", "Figure 4", "Figure 5"]}
# print(extract_images_and_upload(rules_fig, pdf_s3_key, s3_destination_folder))

SHEET_NAME_MULTI_GRP = "Process Parameters"
COLUMN_NAME_INSHEET_GRPBY = "Unit Op Table"
COLUMN_NAME_INSHEET_SORTBY = "Classification"
START_INDEX_TABLE_TITLE = 2

######################################################################
def get_excel_sheetname_from_s3(bucket_name, file_path):
    """
    Get the Sheet names of an Excel sheet from an S3 bucket into a pandas DataFrame.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_path (str): Key/path of the Excel file in the bucket.

    Returns:
        list: List of Sheet names.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        excel_data = response['Body'].read()
        xls = pd.ExcelFile(io.BytesIO(excel_data))
        return xls.sheet_names
    except Exception as e:
        logger.error(f"Failed to Extract the Excel Sheetnames from S3: {e}")
        raise
        
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
        return pd.read_excel(io.BytesIO(excel_data), sheet_name=sheet_name, keep_default_na=False, na_values=[""])
    except Exception as e:
        logger.error(f"Failed to load Excel from S3: {e}")
        raise

def convert_text_run(text, i):
    """Convert each character to superscript if possible."""
    """
    Converts a number (int or float) to its superscript Unicode string representation.

    Args:
        number: The number to convert.

    Returns:
        A string with the number in superscript format.
    """
    # A dictionary to map standard digits to their superscript Unicode characters.
    # Unicode mappings for superscripts/subscripts
    SUPERSCRIPT_MAP = {
        "0": "⁰","1": "¹","2": "²","3": "³","4": "⁴","5": "⁵",
        "6": "⁶","7": "⁷","8": "⁸","9": "⁹",
        "a": "ᵃ","b": "ᵇ","c": "ᶜ","d": "ᵈ","e": "ᵉ","f": "ᶠ",
        "g": "ᵍ","h": "ʰ","i": "ⁱ","j": "ʲ","k": "ᵏ","l": "ˡ",
        "m": "ᵐ","n": "ⁿ","o": "ᵒ","p": "ᵖ","r": "ʳ","s": "ˢ",
        "t": "ᵗ","u": "ᵘ","v": "ᵛ","w": "ʷ","x": "ˣ","y": "ʸ","z": "ᶻ",
        "+": "⁺","-": "⁻","=": "⁼","(": "⁽",")": "⁾"
    }
    
    SUBSCRIPT_MAP = {
        "0": "₀","1": "₁","2": "₂","3": "₃","4": "₄","5": "₅",
        "6": "₆","7": "₇","8": "₈","9": "₉",
        "a": "ₐ","e": "ₑ","h": "ₕ","i": "ᵢ","j": "ⱼ","k": "ₖ",
        "l": "ₗ","m": "ₘ","n": "ₙ","o": "ₒ","p": "ₚ","r": "ᵣ",
        "s": "ₛ","t": "ₜ","u": "ᵤ","v": "ᵥ","x": "ₓ",
        "+": "₊","-": "₋","=": "₌","(": "₍",")": "₎"
    }
    
    if i == 1:
        return ''.join(SUPERSCRIPT_MAP.get(c, c) for c in text)
    # elif rt.font.subscript:
    #     return ''.join(SUBSCRIPT_MAP.get(c, c) for c in text)
    else:
        return text
    
def load_excel_from_s3_wb(bucket_name, file_path, sheet_name):
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
        wb = load_workbook(BytesIO(excel_data), rich_text=True, read_only=False)
        ws = wb[sheet_name] 
    except Exception as e:
        logger.error(f"Failed to load Excel from S3: {e}")
        raise

    rows = []
    for row in ws.iter_rows():
        row_data = []
        for cell in row:
            # if hasattr(cell, 'rich_text') and cell.rich_text:
            cell_text = cell.value
            # if (cell.value is None or cell.value == "") and row[0].row > 1:
            #     cell_text = "nan"
            # else:
            # print(cell_text)
            if isinstance(cell_text, list) and len(cell_text) > 1:
                # print(cell_text)
                # for i in range(len(cell_text)):
                    # print(cell_text[i])
                    # print("tititititititit")
                # cell_text = convert_text_run(str(cell_text[0]), 0)+(convert_text_run(str(cell_text[1]), 1))
                cell_text = ''.join(convert_text_run(str(cell_text[i]), i%2) for i  in range(len(cell_text)))
            else:
                # print("totototototototototottoto")
                cell_text = cell.value
            row_data.append(cell_text)
        rows.append(row_data)
        
    header = rows[0]        # first row
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    return df

# bucket_name = "azcdi-us-ops-report-ds-dev"
# xls_table_s3_key = "temp_test/superscript_test.xlsx"
# sheet_name = "Sheet1"
# xls_table_s3_key = "temp_test/GenAI - Report 2 CSV File Test.xlsx"
# sheet_name = "Process Parameters"
# df = load_excel_from_s3_wb(bucket_name, xls_table_s3_key, sheet_name)
# print(df)
# print(df.columns)
# print(df.iloc[:, 3])
# html = df.to_html(escape=False)
# with open("out.html", "w", encoding="utf-8") as f:
    # f.write(html)

def normalize_text(text: str) -> str:
    """
    Normalizes text for comparison:
    - Lowercase
    - Strips leading/trailing whitespace
    - Replaces non-breaking spaces (common in Word)
    """
    return text.replace('\xa0', ' ').strip().lower()

# Function to set shading on a cell
def set_cell_shading(cell, fill):
    """Apply background shading to a cell (hex color string)."""
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)  # hex color code
    tcPr.append(shd)


def prepend_row(table, cell_texts=None):
    """
    Adds a new row at the top of the table safely.
    `cell_texts` is a list of strings for the new row's cells.
    """
    first_row = table.rows[0]
    n_cells = len(first_row.cells)

    # Create a new row at the bottom
    new_row = table.add_row()

    # Fill new row with provided text or empty
    for i in range(n_cells):
        if cell_texts and i < len(cell_texts):
            new_row.cells[i].text = cell_texts[i]
        else:
            new_row.cells[i].text = ""

    # Copy formatting from first row
    # for i in range(n_cells):
    #     new_row.cells[i]._tcPr = first_row.cells[i]._tcPr

    # Reorder rows: extract all row texts
    all_texts = [[cell.text for cell in row.cells] for row in table.rows]

    # Clear table
    for row in table.rows:
        for cell in row.cells:
            cell.text = ""

    # Refill table with new row first
    for r_idx, row_texts in enumerate([all_texts[-1]] + all_texts[:-1]):
        for c_idx, text in enumerate(row_texts):
            table.rows[r_idx].cells[c_idx].text = text

def to_superscript(number):
    """
    Converts a number (int or float) to its superscript Unicode string representation.

    Args:
        number: The number to convert.

    Returns:
        A string with the number in superscript format.
    """
    # A dictionary to map standard digits to their superscript Unicode characters.
    SUPERSCRIPT_MAP = {
        '0': '⁰', '1': '¹', '2': '²', '3': '³', '4': '⁴',
        '5': '⁵', '6': '⁶', '7': '⁷', '8': '⁸', '9': '⁹'
    }

    # Convert the number to a string to iterate through its digits.
    num_str = str(number)
    superscript_str = ""
    for char in num_str:
        # Look up the superscript character for each digit in the map.
        if char in SUPERSCRIPT_MAP:
            superscript_str += SUPERSCRIPT_MAP[char]
        elif char == '-':  # Handle negative numbers
            superscript_str += '⁻'
        else:
            # If the character is not a digit (e.g., a decimal point), keep it as is.
            superscript_str += char

    return superscript_str

def replace_scientific_notation(text):
    """
    Identifies and replaces patterns like "x 106" with "× 10⁶" in a given string.
    This function uses regular expressions to find the pattern "x 10" followed by a number.

    Args:
        text: The input string to process.

    Returns:
        The string with the scientific notation patterns replaced by superscript formatting.
    """
    # The regular expression pattern to find "x 10" or "x10" followed by a number. 
    # We use a non-capturing group for "x" followed by optional space and "10".
    # Group 1 (\\d+) captures the exponent number.
    pattern = r'[x×]\s*10(\d+)'

    # A helper function for the re.sub method to perform the replacement.
    def replacement_func(match):
        # The first captured group is the exponent.
        exponent = match.group(1)
        # Use our to_superscript function to format the exponent.
        superscript_exponent = to_superscript(exponent)
        # Return the new formatted string with the multiplication symbol and base 10.
        return f"× 10{superscript_exponent}"

    # Use re.sub to find all occurrences of the pattern and replace them
    # using our custom replacement function.
    return re.sub(pattern, replacement_func, text, flags=re.IGNORECASE)

def remove_specific_borders(cell, borders=('top', 'left', 'right', 'insideH', 'insideV')):
    """
    Remove specific borders from a table cell.
    :param cell: docx.table._Cell object
    :param borders: tuple of sides ('top', 'bottom', 'left', 'right', 'insideH', 'insideV')
    """
    # tcPr = cell._element.get_or_add_tcPr()
    # tcBorders = tcPr.find(qn('w:tcBorders'))
    # if tcBorders is None:
    #     return  # no borders to remove

    # for border in borders:
    #     element = tcBorders.find(qn(f"w:{border}"))
    #     if element is not None:
    #         tcBorders.remove(element)
    # for border in borders:
    tcPr = cell._element.get_or_add_tcPr() # get tcPr element, in which we can define style of borders
    tcBorders = OxmlElement('w:tcBorders')
    top = OxmlElement('w:top')
    top.set(qn('w:val'), 'nil')
    
    left = OxmlElement('w:left')
    left.set(qn('w:val'), 'nil')
    
    
    right = OxmlElement('w:right')
    right.set(qn('w:val'), 'nil')
    
    tcBorders.append(top)
    tcBorders.append(left)
    tcBorders.append(right)
    tcPr.append(tcBorders)
            
def add_df_as_table(doc, df, paragraph, title, insert_title_para_flag):

    # if insert_title_para_flag:
      
    #     title_para = paragraph.insert_paragraph_before()
    #     run = title_para.add_run(title)
    #     run.bold = True
    #     # title_para.style = "Heading 3"   # or "Normal", "Heading 2", etc.

    #     # Add space before and after the title
    #     title_para.paragraph_format.space_before = Pt(36)  # ~0.5 inch before
    #     title_para.paragraph_format.space_after = Pt(18)   # ~0.25 inch after

    # p = paragraph._element

    df = df[~(df.eq("nan").all(axis=1))]
    
    tbl = doc.add_table(rows=1, cols=len(df.columns))
    tbl.style = "Table Grid"

    ncols = len(df.columns)
    ls_tb_cols = [col for col in df.columns if  col.strip().lower().startswith("tb")]
    logger.info(f"Count of rows: {df.shape[0]}.")

 # Fill header
    hdr_cells = tbl.rows[0].cells
    # for i, col in enumerate(df.columns):
    #     hdr_cells[i].text = str(col)
    for i, col_name in enumerate(df.columns):
        run = hdr_cells[i].paragraphs[0].add_run(str(col_name))
        run.bold = True
        run.font.name = "Times New Roman"
        run.font.size = Pt(10)
        # Add shading (Blue, Accent 1, Lighter 80% → Hex DDEBF7)
        set_cell_shading(hdr_cells[i], "DDEBF7")

    if len(ls_tb_cols) > 0:
        prepend_row(tbl)
        prepend_row(tbl)
            
      # Fill rows
    for index, row in df.iterrows():
        row_cells = tbl.add_row().cells
        # for i, item in enumerate(row):
        #     row_cells[i].text = str(item)
        for i, value in enumerate(row):
            # val = replace_scientific_notation(str(value))
            run = row_cells[i].paragraphs[0].add_run(str(value))
            # run = row_cells[i].paragraphs[0].add_run(value)
            run.font.name = "Times New Roman"
            run.font.size = Pt(10)

    if len(ls_tb_cols) > 0:
         # Insert a new row at the top
        hdr_cells_title = tbl.rows[0].cells
        hdr_cells = tbl.rows[1].cells
        second_row = tbl.rows[2]
        tb_start, tb_end = None, None
        for i, cell in enumerate(second_row.cells):
            if cell.text.strip().lower().startswith("tb") and tb_start is None:
                tb_start = i
            if tb_start is not None and cell.text.strip().lower().startswith("tb"):
                tb_end = i
        # Extend tb_end to include the last column (Validation Acceptance)
        tb_end = ncols - 1
       
    
        # Merge the last 5 columns (TB3286…TB3304) into one cell
        merged = hdr_cells[tb_start].merge(hdr_cells[tb_end])
        merged.text = "Observed Results for Process Validation Lots"

        merged_title = hdr_cells_title[0].merge(hdr_cells_title[ncols-1])
        merged_title.text = title
        

        # Merge first 2 rows
        for i in range(tb_start):
            cell_top = tbl.rows[1].cells[i]
            cell_bottom = tbl.rows[2].cells[i]
            text_replace = cell_bottom.text
            cell_top.merge(cell_bottom)
            cell_top.text = text_replace

        # --- Step 4: Style all header cells ---
        for cell in tbl.rows[0].cells:
            set_cell_shading(cell, "FFFFFF")
            remove_specific_borders(cell, borders=("top", "left", "right", 'insideH', 'insideV'))
            for p in cell.paragraphs:
                run = p.runs[0] if p.runs else p.add_run()
                run.bold = True
                run.font.name = "Times New Roman"
                run.font.size = Pt(12)
                p.alignment = WD_ALIGN_PARAGRAPH.LEFT
                
        for cell in tbl.rows[1].cells:
            set_cell_shading(cell, "DDEBF7")
            for p in cell.paragraphs:
                run = p.runs[0] if p.runs else p.add_run()
                run.bold = True
                run.font.name = "Times New Roman"
                run.font.size = Pt(10)
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER

        for cell in tbl.rows[2].cells:
            set_cell_shading(cell, "DDEBF7")
            for p in cell.paragraphs:
                run = p.runs[0] if p.runs else p.add_run()
                run.bold = True
                run.font.name = "Times New Roman"
                run.font.size = Pt(10)
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER

             # Mark first 2 rows as repeating headers
        for row in tbl.rows[:3]:
            tr = row._tr
            trPr = tr.get_or_add_trPr()
            tblHeader = OxmlElement("w:tblHeader")
            tblHeader.set(qn("w:val"), "true")
            trPr.append(tblHeader)

    else:
            prepend_row(tbl)
            logger.info(f"1. Add a row on the top with the title: {title}.")
            hdr_cells_title = tbl.rows[0].cells
            merged_title = hdr_cells_title[0].merge(hdr_cells_title[ncols-1])
            merged_title.text = title
            logger.info(f"2. Add a row on the top with the title: {title}.")

            for cell in tbl.rows[0].cells:
                set_cell_shading(cell, "FFFFFF")
                remove_specific_borders(cell, borders=("top", "left", "right", 'insideH', 'insideV'))
                for p in cell.paragraphs:
                    run = p.runs[0] if p.runs else p.add_run()
                    run.bold = True
                    run.font.name = "Times New Roman"
                    run.font.size = Pt(12)
                    p.alignment = WD_ALIGN_PARAGRAPH.LEFT

            logger.info(f"3. Add a row on the top with the title: {title}.")
            for cell in tbl.rows[1].cells:
                set_cell_shading(cell, "DDEBF7")
                for p in cell.paragraphs:
                    run = p.runs[0] if p.runs else p.add_run()
                    run.bold = True
                    run.font.name = "Times New Roman"
                    run.font.size = Pt(10)
                    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    
            logger.info(f"4. Add a row on the top with the title: {title}.")
            for row in tbl.rows[:2]:
                tr = row._tr
                trPr = tr.get_or_add_trPr()
                tblHeader = OxmlElement("w:tblHeader")
                tblHeader.set(qn("w:val"), "true")
                trPr.append(tblHeader)
        
    # # Move the table XML element after the placeholder paragraph
    # if insert_title_para_flag:
    #      # Place the table right after title paragraph
    #    title_para._element.addnext(tbl._element)
    # else:
    #     # p.addnext(tbl._element)
    paragraph.insert_paragraph_before()._element.addnext(tbl._element)

def _insert_table(doc, paragraph, df, placeholder, idx_label, sheet_name, filter_val, table_title) -> bool:
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
        
        # # Remove table title
        # paragraph.text = paragraph.text.replace(table_title, "")
        
        if sheet_name.strip().lower() == SHEET_NAME_MULTI_GRP.strip().lower():
           dfs = {}
           logger.info(f"get the Filter column {df.columns}.")
           ls_unit_cols = [col for col in df.columns if "unit" in col.strip().lower()]
           logger.info(f"get the Filter column {ls_unit_cols}.")
           if len (ls_unit_cols) > 0:
               COLUMN_NAME_INSHEET_GRPBY = ls_unit_cols[0]
               if filter_val is None:
                   for idx, val in enumerate(df[COLUMN_NAME_INSHEET_GRPBY].unique(), start=1):
                       dfs[val] = df[df[COLUMN_NAME_INSHEET_GRPBY] == val]
                       # df_sorted = dfs[val].sort_values(by=COLUMN_NAME_INSHEET_SORTBY)
                       df_sorted = dfs[val]
                       df_sorted_new = df_sorted.drop(columns=[COLUMN_NAME_INSHEET_GRPBY])
                       _idx = START_INDEX_TABLE_TITLE + idx
                       add_df_as_table(doc, df_sorted_new, paragraph, f"Table {_idx}: {val} Process Parameters, Process Outputs, Validation Acceptance Criteria and Results", True)
               else:
                    dfs[filter_val] =  df[df[COLUMN_NAME_INSHEET_GRPBY].apply(lambda val: fuzz.partial_ratio(val.lower(), filter_val.lower()) >= 90)]
                    # df_sorted = dfs[filter_val].sort_values(by=COLUMN_NAME_INSHEET_SORTBY)
                    df_sorted = dfs[filter_val]
                    df_sorted_new = df_sorted.drop(columns=[COLUMN_NAME_INSHEET_GRPBY])
                    add_df_as_table(doc, df_sorted_new, paragraph, table_title, False)
           else:
               add_df_as_table(doc, df, paragraph, table_title, False)
        else:
             add_df_as_table(doc, df, paragraph, table_title, False)
        
        # Remove placeholder text
        paragraph.text = paragraph.text.replace(placeholder, "")
        logger.info(f"Inserted table after paragraph {idx_label}.")

        return True
    except Exception as e:
        logger.error(f"Error inserting table after paragraph {idx_label}: {e}")
        return False

def extract_unit_name(text):
    # Regex: capture anything between ':' and 'Process Parameters'
    match = re.search(r":\s*(.*?)\s*process parameters", text)
    if match:
        return match.group(1).strip().lower()
    return None

def identify_table_title(text):
    # Regex: capture anything between ':' and 'Process Parameters'
    # match = re.search(r"^table\s+\d+(?:-\d+)?\s*:", text)
    match = re.search(r"^table\s+\d+(?:-\d+)?\s*:.*(?<![0-9Xx])$", text)
    if match:
        return text
    return None
    
def insert_table_after_paragraph(doc: Document_docx, marker_text: str, sheet_name: str, docx_s3_key:str, table_df) -> bool:
    """
    Searches for a placeholder string in paragraphs or tables,
    and inserts the given dataframe as table immediately after the matched paragraph.
    """

    norm_marker = normalize_text(marker_text)

    # === 1. Search in standard paragraphs ===
    val = None
    table_title_ = None
    for idx, paragraph in enumerate(doc.paragraphs):
        norm_para = normalize_text(paragraph.text)
        logger.debug(f"[Paragraph {idx}] '{paragraph.text}'")

        table_title = identify_table_title(norm_para) 
        if table_title:
            table_title_ = paragraph.text
            paragraph_title = paragraph
            # # Remove table title
            # paragraph.text = paragraph.text.replace(table_title_, "")
            logger.info(f"Table Title {table_title_}.")


        if any(str(n) in docx_s3_key for n in [1, 2, 3, 4, 5]):
            if norm_marker in norm_para:
                paragraph_title.text = paragraph_title.text.replace(table_title_, "")
                return _insert_table(doc, paragraph, table_df, marker_text, idx, sheet_name, None, table_title_)
        else:
                
            if "table" in norm_para and  "validation acceptance criteria" in norm_para:
                val = extract_unit_name(norm_para)
                # paragraph.text = paragraph.text.replace(table_title_, "")
                logger.info(f"Title with val {norm_para}.")
            if norm_marker in norm_para and val:
                        logger.info(f"Table Insertion with {val}.")
                        paragraph_title.text = paragraph_title.text.replace(table_title_, "")
                        return _insert_table(doc, paragraph, table_df, marker_text, idx, sheet_name, val, table_title_)
            else:
               if norm_marker in norm_para and table_title_:
                    paragraph_title.text = paragraph_title.text.replace(table_title_, "")
                    logger.info(f"Table Title Fabrice {table_title_}.")
                    return _insert_table(doc, paragraph, table_df, marker_text, idx, sheet_name, None, table_title_)


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


def sitev_insert_table_in_report(
    bucket_name,
    rules_tab,
    target_s3_key: str,
    xls_table_s3_key: str,
    docx_s3_key: str
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
        response = s3.get_object(Bucket=bucket_name, Key=docx_s3_key)
        template_bytes = response['Body'].read()
        template_file = BytesIO(template_bytes)
        # doc = Document_docx(template_file)
        doc = Document_docx(template_file)
    except Exception as e:
        logger.error(f"Failed to open template DOCX: {e}")
        raise

    list_sheet_names = get_excel_sheetname_from_s3(bucket_name, xls_table_s3_key)
   
    # Step 3: Loop over each mapping entry (each represents one figure/placeholder pair)
    # for mapping in mappings:
    rules_dict_deviation_change = {}
    for placeholder, sheet_name in rules_tab.items():
        # placeholder = mapping["Place_holder"]
        # doc_id = mapping["Template_Name"]
        # figure_numbers = mapping["Figures"]
        if sheet_name in list_sheet_names:
            df_mapping = load_excel_from_s3_wb(bucket_name, xls_table_s3_key, sheet_name)
            df_mapping = df_mapping.loc[:, df_mapping.columns.notna()]
            # Normalize empty values to string "nan"
            df_mapping = df_mapping.replace([np.nan, None, ""], "nan")
            df_mapping = df_mapping.applymap(lambda x: "nan" if str(x).strip() == "" else x)
            # Now remove rows where every cell is "nan"
            df_mapping = df_mapping[~(df_mapping.eq("nan").all(axis=1))]
            logger.info(f"List of columns: {df_mapping.columns}")
            logger.info(f"Inserting table {sheet_name}")
            inserted = insert_table_after_paragraph(doc, placeholder, sheet_name, docx_s3_key, df_mapping)
            logger.info(f"inserted result is: {inserted}") 
    
            if not inserted:
                logger.warning(f"Failed to insert table for placeholder {placeholder}")
            if "deviation" in sheet_name.lower().strip():
                rules_dict_deviation_change["<Deviations_Number>"] = str(df_mapping.shape[0])
            if "change" in sheet_name.lower().strip():
                rules_dict_deviation_change["<Change_Control_Number>"] = str(df_mapping.shape[0])
        else:
            logger.info(f"Sheet name {sheet_name} in excel template do not exist in excel data source")

   
    # Step 5: Upload final report back to S3
    try:
        doc_stream = io.BytesIO()
        doc.save(doc_stream)
        doc_stream.seek(0)
        s3.upload_fileobj(doc_stream, bucket_name, target_s3_key)
        
    except Exception as e:
        logger.error(f"Failed to upload final report to S3: {e}")
        raise

    return rules_dict_deviation_change

#################### Report 1
# bucket_name = "azcdi-us-ops-report-ds-dev"
# target_filename = "outputs/report 1 - experiment.docx"
# xls_table_s3_key = "inputs/GenAI - Report 1 CSV File Test - Copy.xlsx"
# docx_s3_key = "inputs/sv_docx_templates/Report 1 - Process Validation Report Template - Inoculum Expansion through Seed Bioreactor Operations.docx"
# # tables = {"<Table 1:>": "Lot Table", "<Table 3:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 5:>": "Process Parameters", "<Table 6:>": "Process Parameters", "<Table 7:>": "Process Parameters","<Table 8:>": "Process Parameters", "<Table 9:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 10:>": "Process Parameters", "<Table 11:>": "Process Parameters",  "<Table 12:>": "Process Parameters", "<Table 13:>": "Process Parameters", "<Table 14:>": "Process Parameters", "<Table 15:>": "Process Parameters", "<Table 16:>": "Process Parameters"}
# tables = {"<Table 3:>": "Process Parameters", "<Table 5:>": "Deviation", "<Table 6:>": "Change Controls"} 
# # print(any(str(n) in docx_s3_key for n in [1, 2, 3, 4, 5]))
# data = sitev_insert_table_in_report(bucket_name, tables, target_filename, xls_table_s3_key, docx_s3_key)
# print(data)

###### Report 7
bucket_name = "azcdi-us-ops-report-ds-dev"
target_filename = "outputs/report 7 - experiment.docx"
xls_table_s3_key = "inputs/GenAI - Report 7 CSV File Test.xlsx"
docx_s3_key = "inputs/sv_docx_templates/Report 7 - Process Validation Report Template - Summary Report.docx"
# tables = {"<Table 1:>": "Lot Table", "<Table 3:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 5:>": "Process Parameters", "<Table 6:>": "Process Parameters", "<Table 7:>": "Process Parameters","<Table 8:>": "Process Parameters", "<Table 9:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 10:>": "Process Parameters", "<Table 11:>": "Process Parameters",  "<Table 12:>": "Process Parameters", "<Table 13:>": "Process Parameters", "<Table 14:>": "Process Parameters", "<Table 15:>": "Process Parameters", "<Table 16:>": "Process Parameters"}
tables = {"<Table 1:>": "Lot Table", "<Table 3:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 6:>": "Process Parameters", "<Table 15:>": "Process Parameters"} 
# print(any(str(n) in docx_s3_key for n in [1, 2, 3, 4, 5]))
# data = sitev_insert_table_in_report(bucket_name, tables, target_filename, xls_table_s3_key, docx_s3_key)
# print(data)
# sitev_generate_report_from_template(bucket_name, target_filename, target_filename, data=data)
# sitev_generate_report_from_template(bucket_name, target_filename, target_filename, data=data)

# ######### Report 6
# bucket_name = "azcdi-us-ops-report-ds-dev"
# target_filename = "outputs/report 6 - experiment.docx"
# xls_table_s3_key = "inputs/GenAI - Report 6 Excel File Test - Copy.xlsx"
# docx_s3_key = "inputs/sv_docx_templates/Report 6 - Process Validation Report Template - Controlled Freeze.docx"
# # tables = {"<Table 1:>": "Lot Table", "<Table 3:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 5:>": "Process Parameters", "<Table 6:>": "Process Parameters", "<Table 7:>": "Process Parameters","<Table 8:>": "Process Parameters", "<Table 9:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 10:>": "Process Parameters", "<Table 11:>": "Process Parameters",  "<Table 12:>": "Process Parameters", "<Table 13:>": "Process Parameters", "<Table 14:>": "Process Parameters", "<Table 15:>": "Process Parameters", "<Table 16:>": "Process Parameters"}
# tables = {"<Table 1:>": "Lot Table", "<Table 2:>": "Process Parameters", "<Table 3:>": "Deviation", "<Table 4:>": "Change Controls"} 
# # print(any(str(n) in docx_s3_key for n in [1, 2, 3, 4, 5]))
# data = sitev_insert_table_in_report(bucket_name, tables, target_filename, xls_table_s3_key, docx_s3_key)
# print(data)
# # sitev_generate_report_from_template(bucket_name, target_filename, target_filename, data=data)
# # sitev_generate_report_from_template(bucket_name, target_filename, target_filename, data=data)


################### Report 8
bucket_name = "azcdi-us-ops-report-ds-dev"
target_filename = "output_sv_2/report 8 - experiment.docx"
xls_table_s3_key = "tmp_ksqj601_yqzbnq/Gen AI - Report 8 CSV File Test - Pro A CRL 1.xlsx"
docx_s3_key = "inputs/sv_docx_templates/Report 8 - Resin Lifetime Process Validation Template.docx"
# tables = {"<Table 1:>": "Lot Table", "<Table 3:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 5:>": "Process Parameters", "<Table 6:>": "Process Parameters", "<Table 7:>": "Process Parameters","<Table 8:>": "Process Parameters", "<Table 9:>": "Process Parameters", "<Table 4:>": "Process Parameters", "<Table 10:>": "Process Parameters", "<Table 11:>": "Process Parameters",  "<Table 12:>": "Process Parameters", "<Table 13:>": "Process Parameters", "<Table 14:>": "Process Parameters", "<Table 15:>": "Process Parameters", "<Table 16:>": "Process Parameters"}
tables = {"<Table 1:>": "Lots Table", "<Table 2:>": "Process Run Results", "<Table 3:>": "Mock Run Results", "<Table 4:>": "Deviation", "<Table 5:>": "Change Controls"} 
# print(any(str(n) in docx_s3_key for n in [1, 2, 3, 4, 5]))
# data = sitev_insert_table_in_report(bucket_name, tables, target_filename, xls_table_s3_key, docx_s3_key)
# print(data)

# print(fuzz.partial_ratio("cex", "cex chromatography"))

# print(fuzz.partial_ratio("virus filtration", "bulk filtration"))

# print(fuzz.partial_ratio("filtration virus", "filtration bulk"))

# print(fuzz.partial_ratio("filtration", "filtration bulk"))





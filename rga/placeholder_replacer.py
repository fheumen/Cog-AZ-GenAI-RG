# pip install python-docx loguru
# python test_docx_replacement.py
from docx import Document
import os
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from loguru import logger
from docx.text.run import Run
from docx.enum.text import WD_COLOR_INDEX
import re

HIGHLIGHT_COLOR = 'green'  # or any default color you prefer

# Define your test replacements
data = {
    "<Validation_Number>": "Validation_Number_Val",
    "<Molecule_Name>": "Molecule_Name_Val",
    "<Molecule_Number>": "Molecule_Number_Val",
    "<Building_Number>": "Building_Number_Val",
    "<Chromatography_Type>": "Chromatography_Type_Val",
    "<Resin_Type>": "Resin_Type_Val",
    "<PVMP_Document_Name_and_Number>": "PVMP_Document_Name_and_Number_Val",
    "<Max_Cycle>": "Max_Cycle_Val",
    "<Building_Size>": "Building_Size_Val",
    "<Building_Scale>": "Building_Scale_Val"
}


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


# def handle_hyperlink_placeholders(paragraph, replacements, highlight_color):
#     """
#     Replaces placeholders that are inside hyperlinks.
#     - Keeps the link intact.
#     - Highlights replaced values.
#     """
#     for child in list(paragraph._p):
#         if not child.tag.endswith('hyperlink'):
#             continue

#         hyperlink_text = paragraph.text
#         for placeholder, value in replacements.items():
#             if should_replace(placeholder, value) and placeholder in hyperlink_text:
#                 hlink = paragraph._p.xpath('.//w:hyperlink')
#                 if not hlink:
#                     continue

#                 hlink = hlink[0]
#                 r_id_ext = hlink.get(qn("r:id"))
#                 r_id_in = hlink.get(qn("w:anchor"))
#                 r_id = r_id_ext or r_id_in

#                 if not r_id:
#                     logger.info("Hyperlink has no r:id or anchor.")
#                     break

#                 new_text = hyperlink_text.replace(placeholder, value)
#                 new_hyperlink = add_hyperlink_run(paragraph, new_text, r_id)
#                 highlight_subtext_in_hyperlink(new_hyperlink, value, highlight_color)
#                 break  # only one placeholder per hyperlink


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


def test_docx_placeholder_replacement():
    # Load your test DOCX file
    doc = Document("test_input.docx")  # This file should have placeholders

    # Process table cell paragraphs
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for para in cell.paragraphs:
                    replace_and_highlight_paragraph(
                        paragraph=para,
                        replacements=data,
                        highlight_color=HIGHLIGHT_COLOR
                    )

    for para in doc.paragraphs:
        replace_and_highlight_paragraph(
            paragraph=para,
            replacements=data,
            highlight_color=HIGHLIGHT_COLOR
        )

    # Save the result for inspection
    output_path = "test_output.docx"
    doc.save(output_path)
    print(f"Output saved to: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    test_docx_placeholder_replacement()
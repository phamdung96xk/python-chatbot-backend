# md_logic.py
import os
import re
import csv
import base64
import gzip
import xml.etree.ElementTree as ET
from html import unescape
from collections import Counter

# --- Helper functions ---

def b64_gzip_decode_best_effort(s: str) -> str:
    try:
        payload = ''.join(s.strip().split())
        if len(payload) % 4:
            payload += '=' * (4 - len(payload) % 4)
        raw = base64.b64decode(payload, validate=False)
        try:
            raw = gzip.decompress(raw)
        except OSError: pass # Not gzipped
        
        # Try common encodings
        for enc in ("utf-8", "latin-1"):
            try: return raw.decode(enc)
            except UnicodeDecodeError: continue
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return ""
        
def decode_nested_txt_line(line_content):
    uuid_from_txt, final_decoded_html_content, error_message = None, None, None
    parts = line_content.split('|')
    if len(parts) >= 3:
        uuid_from_txt = parts[0]
        outer_base64_gzip_data = parts[2]
        try:
            outer_decoded_xml_string = b64_gzip_decode_best_effort(outer_base64_gzip_data)
            root = ET.fromstring(outer_decoded_xml_string)
            inner_encoded_elem = root.find('Base64EncodedGZipCompressedContent')
            if inner_encoded_elem is not None and inner_encoded_elem.text:
                final_decoded_html_content = b64_gzip_decode_best_effort(inner_encoded_elem.text.strip())
            else:
                error_message = "Không tìm thấy thẻ Base64EncodedGZipCompressedContent."
        except Exception as e:
            error_message = f"Lỗi giải mã: {e}"
    else:
        error_message = "Dòng TXT có định dạng không mong muốn."
    return uuid_from_txt, final_decoded_html_content, error_message

# --- Main Logic Function for MD Tool ---

def run_md_check(directory_path):
    log = []
    xml_files = [f for f in os.listdir(directory_path) if f.lower().endswith(".xml")]
    if not xml_files:
        return "Không tìm thấy tệp .xml nào trong thư mục."
    
    for xml_file in xml_files:
        base_name = os.path.splitext(xml_file)[0]
        txt_path = os.path.join(directory_path, f"{base_name}_content.txt")
        xml_path = os.path.join(directory_path, xml_file)
        
        log.append(f"\n--- Đang xử lý: {base_name} ---")

        if not os.path.exists(txt_path):
            log.append(f"  ❌ Lỗi: Thiếu tệp TXT '{os.path.basename(txt_path)}'.")
            continue
            
        # Core logic: Process each pair
        file_errors = _process_single_md_pair(xml_path, txt_path)

        if not file_errors:
            log.append("  ✅ Không có lỗi.")
        else:
            for error in file_errors:
                log.append(f"  ❌ {error}")

    return "\n".join(log)

def _process_single_md_pair(xml_file_path, txt_file_path):
    """
    Combines logic from both "MD Cũ" and "MD Mới" tools.
    Returns a list of error strings for the given file pair.
    """
    errors = []
    base_name = os.path.splitext(os.path.basename(xml_file_path))[0]
    
    # --- Data Loading and Parsing ---
    uuid_to_html = {}
    try:
        with open(txt_file_path, 'r', encoding='utf-8') as f_txt:
            for line in f_txt:
                if line.startswith("HEADER ROW") or not line.strip(): continue
                uuid, html_content, error = decode_nested_txt_line(line)
                if uuid:
                    uuid_to_html[uuid] = (html_content, error)
    except Exception as e:
        return [f"Lỗi nghiêm trọng khi đọc {os.path.basename(txt_file_path)}: {e}"]
        
    case_keys_from_xml = {}
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        ns = {'ns': 'http://risk.regn.net/LeadList'}
        for lead_elem in root.findall('ns:LeadList/ns:Lead', ns):
            lead_id, case_key_raw = lead_elem.get('ID'), lead_elem.get('CaseKey')
            if lead_id and case_key_raw:
                case_keys_from_xml[lead_id] = case_key_raw
    except ET.ParseError as e:
        return [f"Lỗi nghiêm trọng khi đọc {os.path.basename(xml_file_path)}: {e}"]

    # --- Start Checking Logic ---
    for lead_id, case_key_raw in case_keys_from_xml.items():
        html_content, decode_error = uuid_to_html.get(lead_id, (None, None))
        
        if decode_error:
            errors.append(f"ID: {lead_id} | Lỗi giải mã: {decode_error}")
            continue
        if not html_content:
            errors.append(f"ID: {lead_id} | Không tìm thấy HTML sau khi giải mã TXT.")
            continue
            
        # --- Logic from "MD Cũ" (Check Mode) ---
        DATA_NOT_FOUND_CRITERIA = "DATA NOT FOUND".lower().strip()
        if DATA_NOT_FOUND_CRITERIA in html_content.lower():
            match = re.search(r"<input[^>]*name=\"caseId\"[^>]*value=\"([^\"]*)\"[^>]*>", html_content, re.IGNORECASE)
            if match:
                html_val = match.group(1).strip().upper().replace('-', '')
                if case_key_raw.upper() != html_val:
                    errors.append(f"ID: {lead_id} | CaseKey_XML: {case_key_raw} | CaseName_HTML: {match.group(1).strip().upper()}")
        else:
            match = re.search(r"Case Number:\s*</span>\s*</td>\s*<td>\s*<span[^>]*class=\"Value\"[^>]*>([A-Za-z0-9.-]+?)</span>", html_content, re.IGNORECASE | re.DOTALL)
            if match:
                html_val = match.group(1).strip().upper().replace('-', '')
                if case_key_raw.upper() != html_val:
                    errors.append(f"ID: {lead_id} | CaseKey_XML: {case_key_raw} | CaseName_HTML: {match.group(1).strip().upper()}")
            else:
                 errors.append(f"ID: {lead_id} | CaseKey_XML: {case_key_raw} | Không tìm thấy Case Number trong HTML")

        # --- Logic from "MD Mới" ---
        case_key_match = re.search(r"([\d\/\-]{10})-([\d\/\-]{10}) (.*?)%,(.*?)%", case_key_raw)
        if case_key_match:
            range_from_xml, range_to_xml, last_name_xml, first_name_xml = [s.strip() for s in case_key_match.groups()]
            last_name_xml += "%"; first_name_xml += "%"
            lead_errors_new = []
            
            # Check based on page type
            if "DATA NOT FOUND" in html_content:
                # Validation for search form page
                first_name_html = re.search(r'<input[^>]*name="firstName"[^>]*value="([^"]*)"[^>]*>', html_content, re.I|re.S)
                last_name_html = re.search(r'<input[^>]*name="lastName"[^>]*value="([^"]*)"[^>]*>', html_content, re.I|re.S)
                if not first_name_html or first_name_html.group(1).strip() != first_name_xml: lead_errors_new.append("First Name")
                if not last_name_html or last_name_html.group(1).strip() != last_name_xml: lead_errors_new.append("Last Name")
            else:
                # Validation for results page
                first_name_html = re.search(r"First Name:\s*<span[^>]*>([\w\s%]+?)</span>", html_content, re.I|re.S)
                last_name_html = re.search(r"Last Name:\s*<span[^>]*>([\w\s%]+?)</span>", html_content, re.I|re.S)
                if not first_name_html or first_name_html.group(1).strip() != first_name_xml: lead_errors_new.append("First Name")
                if not last_name_html or last_name_html.group(1).strip() != last_name_xml: lead_errors_new.append("Last Name")

            if lead_errors_new:
                errors.append(f"ID: {lead_id} | Lỗi sai do: {', '.join(lead_errors_new)}")

    return errors

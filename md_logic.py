import os
import re
import base64
import gzip
import xml.etree.ElementTree as ET

def decode_base64_gzip(encoded_string):
    """
    Decodes a base64 encoded, gzipped string.
    """
    try:
        missing_padding = len(encoded_string) % 4
        if missing_padding:
            encoded_string += '=' * (4 - missing_padding)
        decoded_bytes = base64.b64decode(encoded_string)
        decompressed_data_bytes = gzip.decompress(decoded_bytes)
        return decompressed_data_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        raise ValueError(f"Lỗi giải mã/giải nén: {e}")

def decode_nested_txt_line(line_content):
    """
    Decodes a single line from the _content.txt file which contains nested base64 content.
    """
    uuid, html_content, error_msg = None, None, None
    parts = line_content.split('|')
    if len(parts) >= 3:
        uuid = parts[0]
        try:
            outer_xml = decode_base64_gzip(parts[2])
            root = ET.fromstring(outer_xml)
            inner_elem = root.find('Base64EncodedGZipCompressedContent')
            if inner_elem is not None and inner_elem.text:
                html_content = decode_base64_gzip(inner_elem.text.strip())
            else:
                error_msg = "Không tìm thấy nội dung lồng nhau (Base64EncodedGZipCompressedContent)."
        except Exception as e:
            error_msg = f"Lỗi giải mã lồng nhau: {e}"
    else:
        error_msg = "Định dạng dòng TXT không hợp lệ (thiếu dấu '|')."
    return uuid, html_content, error_msg

def parse_xml_for_case_keys(xml_file_path):
    """
    Parses the XML file to extract a map of Lead ID to CaseKey.
    """
    case_key_map = {}
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        namespace = {'ns': 'http://risk.regn.net/LeadList'}
        for lead in root.findall('.//ns:Lead', namespace):
            lead_id, case_key = lead.get('ID'), lead.get('CaseKey')
            if lead_id and case_key:
                case_key_map[lead_id] = case_key
    except Exception as e:
        print(f"Lỗi khi đọc file XML '{xml_file_path}': {e}")
    return case_key_map

def run_md_cu_check(directory_path):
    """
    Main function to run the 'MD Cũ' check logic for all files in a directory.
    """
    log_output = []
    xml_files = [f for f in os.listdir(directory_path) if f.lower().endswith(".xml")]
    
    if not xml_files:
        return "Không tìm thấy tệp .xml nào trong thư mục được cung cấp."

    for xml_file in xml_files:
        base_name = os.path.splitext(xml_file)[0]
        txt_path = os.path.join(directory_path, f"{base_name}_content.txt")
        xml_path = os.path.join(directory_path, xml_file)

        log_output.append(f"\n--- Đang xử lý (MD Cũ): {base_name} ---")

        if not os.path.exists(txt_path):
            log_output.append(f"  ❌ Lỗi: Thiếu tệp TXT '{os.path.basename(txt_path)}'.")
            continue

        xml_case_keys = parse_xml_for_case_keys(xml_path)
        if not xml_case_keys:
            log_output.append(f"  ❌ Lỗi: Không thể đọc CaseKey từ file XML hoặc file XML rỗng.")
            continue

        txt_data = {}
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith("HEADER ROW") or not line.strip():
                        continue
                    uuid, html, error = decode_nested_txt_line(line)
                    if uuid:
                        txt_data[uuid] = (html, error)
        except Exception as e:
            log_output.append(f"  ❌ Lỗi nghiêm trọng khi đọc file TXT: {e}")
            continue
        
        file_errors = []
        for xml_id, case_key in xml_case_keys.items():
            html_content, error = txt_data.get(xml_id, (None, "Không tìm thấy ID trong file TXT"))
            
            if error:
                file_errors.append(f"ID: {xml_id} | CaseKey_XML: {case_key} | Lỗi: {error}")
                continue
            if not html_content:
                file_errors.append(f"ID: {xml_id} | CaseKey_XML: {case_key} | Lỗi: Nội dung HTML rỗng sau khi giải mã.")
                continue

            DATA_NOT_FOUND_CRITERIA = "DATA NOT FOUND".lower()
            if DATA_NOT_FOUND_CRITERIA in html_content.lower():
                match = re.search(r"<input[^>]*name=\"caseId\"[^>]*value=\"([^\"]*)\"[^>]*>", html_content, re.I)
                html_val_raw = match.group(1).strip().upper() if match else None
                html_val_normalized = html_val_raw.replace('-', '') if html_val_raw else None
                
                if not match or case_key.upper() != html_val_normalized:
                    file_errors.append(f"ID: {xml_id} | CaseKey_XML: {case_key} | CaseName_HTML: {html_val_raw or 'Không tìm thấy'}")
            else:
                match = re.search(r"Case Number:\s*</span>\s*</td>\s*<td>\s*<span[^>]*class=\"Value\"[^>]*>([A-Za-z0-9.-]+?)</span>", html_content, re.I | re.DOTALL)
                html_val_raw = match.group(1).strip().upper() if match else None
                html_val_normalized = html_val_raw.replace('-', '') if html_val_raw else None

                if not match or case_key.upper() != html_val_normalized:
                    file_errors.append(f"ID: {xml_id} | CaseKey_XML: {case_key} | CaseName_HTML: {html_val_raw or 'Không tìm thấy'}")

        if not file_errors:
            log_output.append("  ✅ Không phát hiện lỗi.")
        else:
            for err in file_errors:
                log_output.append(f"  ❌ {err}")

    return "\n".join(log_output)


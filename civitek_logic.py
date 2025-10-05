# civitek_logic.py
import os
import glob
import base64
import gzip
import re
import html
import csv
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from collections import defaultdict

# --- Helper functions extracted from the original Tkinter app ---

def _fully_decode_base64_gzip(base64_content: str) -> str:
    # ... (Implementation is the same as in the original file)
    current_content = base64_content
    for _ in range(10): 
        try:
            if len(current_content) % 4:
                current_content += '=' * (4 - len(current_content) % 4)
            decoded_bytes = base64.b64decode(current_content)
            decompressed_bytes = gzip.decompress(decoded_bytes)
            decompressed_str = decompressed_bytes.decode('utf-8')
            match = re.search(r'<Base64EncodedGZipCompressedContent>(.*?)</Base64EncodedGZipCompressedContent>', decompressed_str, re.DOTALL)
            if not match: return decompressed_str
            current_content = match.group(1).strip()
        except Exception:
            return current_content
    return current_content

def _load_txt_file(txt_path):
    records = []
    try:
        with open(txt_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("HEADER ROW"):
                    continue
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    record_id = parts[0].strip().lower()
                    base64_data = parts[2].strip()
                    try:
                        raw_content = _fully_decode_base64_gzip(base64_data)
                        records.append({'id': record_id, 'raw_content': raw_content, 'base64': base64_data})
                    except Exception:
                        pass
    except Exception as e:
        # In a server context, we might log this error
        print(f"Error reading file {txt_path}: {e}")
    return records

def _get_field_value(lead_node, field_id):
    if lead_node is None: return ""
    ns_match = re.match(r'\{([^}]+)\}', lead_node.tag)
    ns = {'ns': ns_match.group(1)} if ns_match else {}
    path = f".//ns:InputValue[@FieldID='{field_id}']" if ns else f".//InputValue[@FieldID='{field_id}']"
    input_value = lead_node.find(path, namespaces=ns)
    return input_value.text.strip() if input_value is not None and input_value.text else ""

def _analyze_html(record):
    html_content = record['raw_content']
    errors = []
    
    if not html_content:
        errors.append(f"ID: {record['id']} | Collection sai (Trang chưa load được)")
        return errors
        
    if "No matches found" in html_content:
        return errors

    soup = BeautifulSoup(html_content, 'lxml')
    reasons = []
    expand_button = soup.find(id=re.compile(r'form:expand', re.IGNORECASE))
    expand_state = "(Không rõ)"
    if expand_button:
        button_text = expand_button.get_text(strip=True).lower()
        if 'collapse all' in button_text:
            expand_state = "Mở"
        elif 'expand all' in button_text:
            expand_state = "Đóng"
            reasons.append("Nút 'Expand All' vẫn đang ở trạng thái 'Đóng'")

    statute_count = len(re.findall(r'Statute\s*/\s*Text', html_content, re.IGNORECASE))
    closed_toggles = len(soup.select("div[id*='chargeDetailsTable'] .ui-icon-circle-triangle-e"))
    opened_toggles = len(soup.select("div[id*='chargeDetailsTable'] .ui-icon-circle-triangle-s"))
    total_toggles = closed_toggles + opened_toggles
    
    if statute_count != total_toggles and total_toggles > 0:
        reasons.append(f"Tổng có {total_toggles} dòng cần mở rộng, số dòng mở hiện tại {statute_count}")
        
    loading_check_map = {"Doc #": "Dockets", "Judicial Officer": "Judge Assignment History", "Defendant Attorney": "Court Events", "Assessment Due": "Financial Summary", "Reopen Reason": "Reopen History"}
    span_labels = {span.get_text(strip=True).lower() for span in soup.select("span.ui-column-title")}
    loading_names = [name for label, name in loading_check_map.items() if label.lower() not in span_labels]
    
    if len(loading_names) == 1:
        reasons.append(f"Danh sách {loading_names[0]} đang loading")
    elif len(loading_names) > 1:
        reasons.append(f"Các danh sách sau đang loading: {', '.join(loading_names)}")
    
    if reasons:
        errors.append(f"ID: {record['id']} | Expand All = {expand_state} | Lý do lỗi: {'; '.join(reasons)}")
        
    return errors

def _extract_case_number_from_html(html_text):
    m = re.search(
        r'class="ucn"[^>]*>\s*<span[^>]*>\s*Case\s*Number\s*</span>\s*([A-Za-z0-9\-/\s]+?)<br',
        html_text, flags=re.I | re.S
    )
    if not m:
        m = re.search(r'>\s*Case\s*Number\s*</span>\s*([A-Za-z0-9\-/\s]+?)<', html_text, flags=re.I | re.S)
    if not m:
        return None, None
    raw = re.sub(r'[^A-Za-z0-9]+', '', m.group(1)).upper()
    no_prefix = raw[2:] if len(raw) >= 2 and raw[:2].isdigit() else raw
    return raw, no_prefix

def _check_xml_vs_html(record, lead_node):
    errors = []
    html_content = record['raw_content']
    
    # Check County Name
    name_county = (_get_field_value(lead_node, "1") or "").lower()
    title_tag = BeautifulSoup(html_content, 'lxml').title
    title_text = title_tag.string.lower() if title_tag and title_tag.string else ""
    if name_county and name_county not in title_text:
        errors.append(f"ID: {record['id']} | Lỗi NAME county trong HTML không khớp với NAME county = '{name_county}' trong XML")

    # Check Case Number
    f2 = (_get_field_value(lead_node, "2") or "").upper().strip()
    f3 = (_get_field_value(lead_node, "3") or "").upper().strip()
    f4 = (_get_field_value(lead_node, "4") or "").upper().strip()
    f5 = (_get_field_value(lead_node, "5") or "").upper().strip()
    f6 = (_get_field_value(lead_node, "6") or "").upper().strip()
    expected = f"{f2}{f3}{f4}{f5}{f6}"
    
    _, case_no_prefix = _extract_case_number_from_html(html_content)
    if not case_no_prefix:
        if "No matches found" not in html_content:
            errors.append(f"ID: {record['id']} | Không tìm thấy Case Number trong HTML để so sánh")
    else:
        case_no_prefix_clean = re.sub(r'[^A-Z0-9]+', '', case_no_prefix).upper()
        if expected != case_no_prefix_clean:
            errors.append(f"ID: {record['id']} | Lỗi Case Number: Key XML '{expected}' ≠ Key HTML (bỏ mã county) '{case_no_prefix_clean}'")

    # Check FieldIDs in HTML values
    field_errors = []
    value_matches = re.findall(r'value="(.*?)"', html_content, flags=re.S | re.I)
    value_set = {v.strip() for v in value_matches}
    if not (expected and any(expected in v for v in value_set)):
        for i in range(2, 7):
            fv = (_get_field_value(lead_node, str(i)) or "").strip()
            if fv and not (fv in value_set or fv in html_content):
                 field_errors.append(f"FieldID {i} = '{fv}'")
    
    if field_errors:
        errors.append(f"ID: {record['id']} | Lỗi FieldID: {'; '.join(field_errors)} không tìm thấy trong HTML")

    return errors

def _check_line_count_and_duplicates(file_path):
    errors = []
    expected_lines = 1001
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        total_lines = len(lines)
        
        if total_lines != expected_lines:
            errors.append(f'Lỗi số dòng (mong đợi {expected_lines}, thực tế {total_lines})')
            
        seen_lines = set()
        duplicate_ids = set()
        for line in lines:
            if not line.strip() or line.startswith("HEADER ROW"): continue
            if line in seen_lines:
                try:
                    dup_id = line.split('|')[0].strip()
                    if dup_id: duplicate_ids.add(dup_id)
                except IndexError: continue
            else:
                seen_lines.add(line)
        
        if duplicate_ids:
            for dup_id in sorted(list(duplicate_ids)):
                errors.append(f"ID: {dup_id} | Thừa dòng (trùng lặp)")

    except Exception as e:
        errors.append(f"Lỗi đọc file: {e}")
        
    return errors

# --- Main logic function for the server ---

def run_civitek_check(directory_path):
    """
    Main function to run all checks for the Civitek (old) tool.
    Takes a directory path and returns a string with the results.
    """
    results_log = []
    txt_files = glob.glob(os.path.join(directory_path, "*_content.txt"))

    if not txt_files:
        return "Không tìm thấy file *_content.txt nào để xử lý."
    
    results_log.append(f"Bắt đầu kiểm tra {len(txt_files)} cặp file...\n")

    for txt_path in txt_files:
        base_name = os.path.basename(txt_path).replace('_content.txt', '')
        results_log.append(f"\n--- Đang xử lý: {base_name} ---")
        
        file_errors = []
        
        # 1. Check line count and duplicates
        line_errors = _check_line_count_and_duplicates(txt_path)
        if line_errors:
            results_log.append(f"  [Lỗi File]: {'; '.join(line_errors)}")

        # 2. Detailed check (HTML analysis, XML vs HTML)
        records = _load_txt_file(txt_path)
        xml_path = txt_path.replace("_content.txt", ".xml")
        xdoc, ns = None, {}
        if os.path.exists(xml_path):
            try: 
                xdoc = ET.parse(xml_path).getroot()
                if '}' in xdoc.tag:
                    ns['ns'] = xdoc.tag.split('}')[0][1:]
            except ET.ParseError:
                results_log.append("  [Lỗi File]: Không thể đọc file XML.")

        if not records:
            results_log.append("  [Lỗi File]: Không có dữ liệu trong file TXT.")
            continue
            
        for record in records:
            record_errors = []
            
            # Analyze HTML for "Expand All", loading issues etc.
            record_errors.extend(_analyze_html(record))
            
            # Compare XML fields with HTML content if XML is available
            if xdoc is not None:
                record_id = record['id']
                path = f".//ns:Lead[@ID='{record_id}']" if ns else f".//Lead[@ID='{record_id}']"
                lead_node = xdoc.find(path, namespaces=ns)
                if lead_node is None: # Try uppercase
                    path_upper = f".//ns:Lead[@ID='{record_id.upper()}']" if ns else f".//Lead[@ID='{record_id.upper()}']"
                    lead_node = xdoc.find(path_upper, namespaces=ns)

                if lead_node is not None:
                    record_errors.extend(_check_xml_vs_html(record, lead_node))
            
            if record_errors:
                file_errors.extend(record_errors)

        if not file_errors and not line_errors:
            results_log.append("  ✅ Không phát hiện lỗi.")
        else:
            for err in file_errors:
                results_log.append(f"  ❌ {err}")
        
        results_log.append(f"  📌 Tổng số lỗi của file: {len(file_errors) + len(line_errors)}")

    return "\n".join(results_log)

# File này chứa TOÀN BỘ logic xử lý của tool Flager, KHÔNG chứa code giao diện (tkinter).

import os
import base64
import gzip
import xml.etree.ElementTree as ET
import csv
import re
import html
from bs4 import BeautifulSoup
from collections import defaultdict

# ==== Helper chọn thư mục dữ liệu “đúng” (dùng chung) ====
def _has_data_here(d):
    try:
        for f in os.listdir(d):
            fl = f.lower()
            if fl.endswith(".xml") or fl.endswith("_content.txt"):
                return True
    except Exception:
        pass
    return False

def resolve_data_dir(base_dir: str) -> str:
    base_dir = os.path.abspath(base_dir)
    if os.path.isdir(base_dir) and _has_data_here(base_dir):
        return base_dir
    test_dir = os.path.join(base_dir, "Test")
    if os.path.isdir(test_dir) and _has_data_here(test_dir):
        return test_dir
    try:
        subs = [n for n in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, n))]
        if len(subs) == 1:
            child = os.path.join(base_dir, subs[0])
            if _has_data_here(child):
                return child
            test2 = os.path.join(child, "Test")
            if os.path.isdir(test2) and _has_data_here(test2):
                return test2
    except Exception:
        pass
    for cur, dirs, files in os.walk(base_dir):
        for f in files:
            fl = f.lower()
            if fl.endswith(".xml") or fl.endswith("_content.txt"):
                return cur
    return base_dir

# --- CÁC HÀM XỬ LÝ CỐT LÕI (di chuyển từ file gốc) ---
def decode_base64_gzip(data):
    try:
        if len(data) % 4: data += '=' * (4 - len(data) % 4)
        decoded = base64.b64decode(data)
        return gzip.decompress(decoded).decode('utf-8', errors='replace')
    except Exception: return None

def decode_nested_base64(line):
    parts = line.strip().split('|')
    if len(parts) < 3: return None, None
    uuid, outer_b64 = parts[0], parts[2]
    xml_str = decode_base64_gzip(outer_b64)
    if not xml_str: return uuid, None
    try:
        root = ET.fromstring(xml_str)
        inner_elem = root.find("Base64EncodedGZipCompressedContent")
        if inner_elem is not None and inner_elem.text:
            return uuid, decode_base64_gzip(inner_elem.text.strip())
    except ET.ParseError: return uuid, None
    return uuid, None

def load_xml_case_keys(xml_path):
    case_key_map = {}
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        namespace = {'ns': 'http://risk.regn.net/LeadList'}
        for lead in root.findall('.//ns:Lead', namespace):
            case_id, case_key = lead.get('ID'), lead.get('CaseKey')
            if case_id and case_key: case_key_map[case_id] = case_key.strip()
    except Exception as e: 
        raise IOError(f"Lỗi khi đọc tệp XML {os.path.basename(xml_path)}: {e}")
    return case_key_map

def validate_html(html_content, expected_case_key):
    if not html_content: return False, "Nội dung HTML rỗng."
    soup = BeautifulSoup(html_content, 'lxml')
    required_section_ids = ["summaryAccordion", "partyAccordion", "chargeAccordion", "caseDocketsAccordion"]
    if all(not soup.find('div', id=sid) for sid in required_section_ids):
        return False, "Collection sai"
    case_number_tag = soup.find('dd', class_="casenumber")
    if not case_number_tag: return False, "Loading...(Chưa tải hết dữ liệu)"
    actual_case_number = case_number_tag.get_text(strip=True).replace('\xa0', ' ').strip()
    if actual_case_number != expected_case_key:
        return False, f"Sai caseNumber. XML: '{expected_case_key}', HTML: '{actual_case_number}'"
    return True, "Khớp"

def check_for_cases_found(html_content):
    if html_content and 'cases found' in html_content.lower(): return True
    return False

def extract_value_from_filter_div(soup, class_name):
    filter_div = soup.find('div', class_=class_name)
    if not filter_div: return ""
    text_nodes = filter_div.find_all(string=True, recursive=False)
    return ''.join(text_nodes).strip()

def validate_cases_found_page(html_content, expected_case_key):
    if not html_content:
        return 'ERROR_UNKNOWN', "Nội dung HTML rỗng"
    soup = BeautifulSoup(html_content, 'lxml')
    html_case_key = extract_value_from_filter_div(soup, 'searchFilter')
    if not html_case_key:
        return 'ERROR_UNKNOWN', "Không tìm thấy div 'searchFilter' chứa CaseNumber trên HTML"
    if html_case_key != expected_case_key:
        return 'ERROR_CASEKEY', (expected_case_key, html_case_key)
    search_type = extract_value_from_filter_div(soup, 'searchTypeFilter')
    if not search_type:
        return 'ERROR_UNKNOWN', "Không tìm thấy div 'searchTypeFilter' chứa Search Type trên HTML"
    if search_type != "CaseNumber":
        return 'ERROR_SEARCHTYPE', None
    return 'VALID', None

def process_file_pair_logic(file_pair, results_log):
    xml_file, content_file = file_pair
    xml_filename = os.path.basename(xml_file)
    results_log.append(f"\n--- Đang xử lý: {xml_filename} ---")
    case_key_map = load_xml_case_keys(xml_file)
    html_in_memory = {}
    hard_error_uuids = set()
    errors_for_this_file = []
    try:
        with open(content_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            uuid, html_content = decode_nested_base64(line)
            if not uuid: continue
            html_in_memory[uuid] = html_content
            case_key = case_key_map.get(uuid, "")
            if check_for_cases_found(html_content):
                status, payload = validate_cases_found_page(html_content, case_key)
                if status == 'ERROR_CASEKEY':
                    xml_key, html_key = payload
                    errors_for_this_file.append(f"ID:{uuid}| Sai caseNumer ({xml_key}), ({html_key}).")
                    hard_error_uuids.add(uuid)
                elif status == 'ERROR_SEARCHTYPE':
                    errors_for_this_file.append(f"ID:{uuid}| Chọn sai kiểu Search")
                    hard_error_uuids.add(uuid)
                elif status != 'VALID':
                    errors_for_this_file.append(f"ID:{uuid}| Lỗi không xác định trên trang 'cases found': {payload}")
                    hard_error_uuids.add(uuid)
            else:
                is_valid, message = validate_html(html_content, case_key)
                if not is_valid:
                    errors_for_this_file.append(f"ID:{uuid}| {message}")
                    hard_error_uuids.add(uuid)
        results_log.extend(errors_for_this_file)
    except Exception as e:
        results_log.append(f"Lỗi nghiêm trọng khi xử lý {xml_filename}: {e}")

def run_flager_check(directory_path):
    """
    Hàm chính để chạy toàn bộ logic kiểm tra cho tool Flager từ server.
    """
    data_dir = resolve_data_dir(directory_path)  # <-- CHUẨN HOÁ
    results_log = []
    results_log.append("--- Bắt đầu quá trình quét file cho tool Flager ---")
    try:
        content_files = [f for f in os.listdir(data_dir) if f.endswith("_content.txt")]
        file_pairs = []
        for content_filename in content_files:
            base_name = content_filename.replace('_content.txt', '')
            xml_filename = base_name + '.xml'
            xml_path = os.path.join(data_dir, xml_filename)
            if os.path.exists(xml_path):
                file_pairs.append((xml_path, os.path.join(data_dir, content_filename)))
            else:
                results_log.append(f"Cảnh báo: Tìm thấy {content_filename} nhưng không có file {xml_filename} tương ứng.")
        if not file_pairs:
            return "Lỗi: Không tìm thấy cặp file `_content.txt` và `.xml` hợp lệ nào."
        results_log.append(f"Đã phát hiện {len(file_pairs)} cặp file hợp lệ. Bắt đầu xử lý...")
        for pair in file_pairs:
            process_file_pair_logic(pair, results_log)
        total_errors = len([line for line in results_log if line.strip().startswith('ID:')])
        results_log.append(f"\n--- HOÀN THÀNH ---")
        results_log.append(f"Tổng cộng có {total_errors} lỗi được phát hiện.")
    except FileNotFoundError:
        return f"Lỗi: Thư mục '{data_dir}' không tồn tại trên server."
    except Exception as e:
        return f"Lỗi không xác định xảy ra trong quá trình xử lý: {str(e)}"
    return "\n".join(results_log)

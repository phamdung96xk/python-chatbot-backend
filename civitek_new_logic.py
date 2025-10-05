import os
import re
import gzip
import shutil
import base64
import xml.etree.ElementTree as ET
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

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

# --- Helper Functions ---
def fully_decode_base64_gzip(base64_content: str) -> str:
    current_content = base64_content
    for _ in range(10): 
        try:
            if len(current_content) % 4: current_content += '=' * (4 - len(current_content) % 4)
            decoded_bytes = base64.b64decode(current_content)
            decompressed_bytes = gzip.decompress(decoded_bytes)
            decompressed_str = decompressed_bytes.decode('utf-8')
            match = re.search(r'<Base64EncodedGZipCompressedContent>(.*?)</Base64EncodedGZipCompressedContent>', decompressed_str, re.DOTALL)
            if not match: return decompressed_str
            current_content = match.group(1).strip()
        except Exception:
            return current_content
    return current_content

def normalize_date_str(date_string):
    if not date_string: return None
    try:
        parts = date_string.split('/')
        month = int(parts[0]); day = int(parts[1]); year = int(parts[2])
        if month == 0: month = 1
        if day == 0: day = 1
        return f"{month}/{day}/{year}"
    except (ValueError, IndexError):
        return date_string

# --- Validation Functions ---
def validate_county_name(soup, fields):
    errors = []
    xml_county = fields.get('1', '')
    if not xml_county: return errors
    html_title_tag = soup.find('title')
    html_title = html_title_tag.string.strip() if html_title_tag and html_title_tag.string else "Không tìm thấy Title"
    xml_county_norm = re.sub(r'\s+', '', xml_county).lower()
    html_title_norm = re.sub(r'\s+', '', html_title).lower()
    if xml_county_norm not in html_title_norm:
        errors.append(f"Sai County Name (XML=\"{xml_county}\"; Title=\"{html_title}\")")
    return errors

def validate_search_form(soup, fields):
    errors = []
    errors.extend(validate_county_name(soup, fields))
    def check_and_add_error(field_id, error_name, selector, is_date=False):
        xml_val = fields.get(field_id, '')
        element = soup.select_one(selector)
        if not element:
            errors.append(f"Sai {error_name} (XML=\"{xml_val}\", HTML=Không tìm thấy element)")
            return
        html_val = element.get('value', 'Không tìm thấy')
        if is_date:
            xml_norm = normalize_date_str(xml_val)
            html_norm = normalize_date_str(html_val)
            if html_norm != xml_norm:
                errors.append(f"Sai {error_name} (XML=\"{xml_val}\", HTML=\"{html_val}\")")
        elif html_val != xml_val:
            errors.append(f"Sai {error_name} (XML=\"{xml_val}\", HTML=\"{html_val}\")")
    check_and_add_error('2', 'Last Name', r'#form\:search_tab\:lastname')
    check_and_add_error('3', 'First Name', r'#form\:search_tab\:fname')
    check_and_add_error('4', 'Date From', r'#form\:search_tab\:fromDate_input', is_date=True)
    check_and_add_error('5', 'Date To', r'#form\:search_tab\:toDate_input', is_date=True)
    xml_val = fields.get('6', '')
    selected_options = soup.find_all('option', selected="selected")
    if len(selected_options) != 1: 
        errors.append(f"Sai Court Type (XML=\"{xml_val}\", HTML=Tìm thấy {len(selected_options)} lựa chọn)")
    elif selected_options[0].get('value') != xml_val:
        html_val = selected_options[0].get('value')
        errors.append(f"Sai Court Type (XML=\"{xml_val}\", HTML=\"{html_val}\")")
    return errors

def validate_results_page_best_effort(soup, fields):
    errors = []
    errors.extend(validate_county_name(soup, fields))
    if "Charge Seq#" not in soup.get_text():
        errors.append("Loading...(Trang chưa tải xong...)")
        return errors
    xml_lastname, xml_firstname = fields.get('2', '').upper(), fields.get('3', '').upper()
    person_rows = soup.select(r'tbody#searchPartyResults\:partySearchResultsTable_data > tr.ui-widget-content')
    target_row = None
    for row in person_rows:
        name_cells = row.find_all('td', role='gridcell')
        if len(name_cells) > 2:
            name_text = name_cells[2].get_text(strip=True).upper()
            if xml_lastname in name_text and xml_firstname in name_text: 
                target_row = row
                break
    if not target_row:
        errors.append(f"Thiếu Last/First Name (Không tìm thấy dòng khớp với '{xml_firstname} {xml_lastname}')")
        return errors
    checkbox_inputs = soup.select('div#searchPartyResults\\:partySearchResultsTable input[name="searchPartyResults:partySearchResultsTable_checkbox"]')
    is_any_unchecked = False
    if person_rows and checkbox_inputs:
        for cb_input in checkbox_inputs:
            if not ('aria-label' in cb_input.attrs and cb_input['aria-label'] == 'Select All'):
                if not cb_input.has_attr('checked'):
                    is_any_unchecked = True
                    break
    if is_any_unchecked:
        errors.append("Sai checkbox")
    details_row = soup.select_one('tr.ui-expanded-row-content')
    if not details_row:
        errors.append("Cảnh báo: Tìm thấy người dùng nhưng không có mục chi tiết.")
    else:
        xml_date_from_str, xml_date_to_str = fields.get('4', ''), fields.get('5', '')
        file_date_cells = details_row.find_all('td', string=re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$'))
        if not file_date_cells:
            errors.append(f"Sai Date (Không tìm thấy FileDate trong HTML)")
        else:
            try:
                html_file_date_str = file_date_cells[0].get_text(strip=True)
                date_format = "%m/%d/%Y"
                from_obj = datetime.strptime(normalize_date_str(xml_date_from_str), date_format)
                to_obj   = datetime.strptime(normalize_date_str(xml_date_to_str), date_format)
                file_obj = datetime.strptime(normalize_date_str(html_file_date_str), date_format)
                if not (from_obj <= file_obj <= to_obj):
                    errors.append(f"Sai Date (XML Range=\"{xml_date_from_str}\"-\"{xml_date_to_str}\"; HTML FileDate=\"{html_file_date_str}\")")
            except (ValueError, TypeError) as e:
                errors.append(f"Sai Date (Lỗi định dạng ngày tháng: {e})")
        xml_code = fields.get('6', '')
        ucn_link = details_row.select_one('a.ui-link')
        if not ucn_link:
            errors.append("Lỗi: Không tìm thấy UCN link trong mục chi tiết.")
        else:
            ucn_raw = ucn_link.get_text(strip=True)
            ucn_normalized = re.sub(r'[^A-Z0-9]+', '', ucn_raw.upper())
            if len(ucn_normalized) > 2 and ucn_normalized[:2].isdigit():
                ucn_normalized = ucn_normalized[2:]
            match = re.search(r'^\d{4}([A-Z]{1,3})', ucn_normalized)
            html_code = match.group(1) if match else "Không thể trích xuất"
            if html_code != xml_code:
                errors.append(f"Sai Court Type (XML=\"{xml_code}\", Trích xuất từ UCN=\"{html_code}\")")
    return errors

# --- Main Logic Function ---
def run_civitek_new_check(directory_path):
    data_dir = Path(resolve_data_dir(directory_path))  # <-- CHUẨN HOÁ
    results_log = []
    content_files = list(data_dir.glob("*_content.txt"))
    if not content_files:
        return "Không tìm thấy file _content.txt nào để xử lý."
    results_log.append(f"Bắt đầu kiểm tra {len(content_files)} cặp file...\n")
    for content_file_path in content_files:
        base_name = content_file_path.stem.replace('_content', '')
        xml_file_path = data_dir / f"{base_name}.xml"
        results_log.append(f"\n--- Đang xử lý: {base_name} ---")
        if not xml_file_path.exists():
            results_log.append(f"  ❌ Lỗi: Không tìm thấy file XML tương ứng: {xml_file_path.name}")
            continue
        id_to_html = {}
        try:
            with open(content_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 3:
                        uuid, base64_content = parts[0], parts[2]
                        html_content = fully_decode_base64_gzip(base64_content)
                        if uuid and html_content:
                            id_to_html[uuid] = html_content
        except Exception as e:
            results_log.append(f"  ❌ Lỗi khi giải mã {content_file_path.name}: {e}")
            continue
        leads_data_from_xml = {}
        try:
            tree = ET.parse(str(xml_file_path)); root = tree.getroot()
            ns_match = re.match(r'\{([^}]+)\}', root.tag); ns = {'ns': ns_match.group(1)} if ns_match else {}
            for lead in root.findall(".//ns:Lead", ns) if ns else root.findall(".//Lead"):
                lead_id = lead.get('ID')
                if lead_id:
                    leads_data_from_xml[lead_id] = {
                        inp.get('FieldID'): inp.text for inp in (lead.findall("ns:InputValue", ns) if ns else lead.findall("InputValue"))
                    }
        except ET.ParseError as e:
            results_log.append(f"  ❌ Lỗi khi đọc {xml_file_path.name}: {e}")
            continue
        file_errors = []
        for lead_id, fields in leads_data_from_xml.items():
            errors_for_lead = []
            html_content = id_to_html.get(lead_id)
            if not html_content:
                errors_for_lead.append("Lỗi: Không có file HTML nào được giải mã.")
            else:
                soup = BeautifulSoup(html_content, 'html.parser')
                if soup.select_one(r'#form\:search_tab\:lastname'): 
                    errors_for_lead.extend(validate_search_form(soup, fields))
                else: 
                    errors_for_lead.extend(validate_results_page_best_effort(soup, fields))
            if errors_for_lead:
                file_errors.append(f"ID: {lead_id} | {', '.join(errors_for_lead)}")
        if not file_errors:
            results_log.append("  ✅ Không phát hiện lỗi.")
        else:
            for error in file_errors:
                results_log.append(f"  ❌ {error}")
    return "\n".join(results_log)

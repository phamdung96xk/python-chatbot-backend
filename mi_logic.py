# mi_logic.py
import os
import re
import csv
import math
import html
import gzip
import base64
from datetime import datetime
from collections import defaultdict
import xml.etree.ElementTree as ET

try:
    from bs4 import BeautifulSoup
except ImportError:
    # This function will act as a placeholder if BeautifulSoup is not installed
    def BeautifulSoup(markup, features):
        return None

# --- Helper Functions ---

def decode_txt(encoded):
    try:
        if len(encoded) % 4:
            encoded += "=" * (4 - len(encoded) % 4)
        decoded = base64.b64decode(encoded)
        return gzip.decompress(decoded).decode("utf-8", errors="replace")
    except:
        return ""

def decode_nested_html_from_line(line):
    # This simplified version is safer for server-side execution
    def _decode_once(data):
        try:
            if len(data) % 4: data += '=' * (4 - len(data) % 4)
            decoded = base64.b64decode(data)
            return gzip.decompress(decoded).decode('utf-8', errors='replace')
        except Exception:
            return None

    parts = line.strip().split('|')
    if len(parts) < 3: return None, None
    
    uuid, outer_b64 = parts[0], parts[2]
    xml_str = _decode_once(outer_b64)
    if not xml_str: return uuid, None
    
    try:
        # Using regex which is more robust against malformed XML than ET.fromstring
        match = re.search(r'<Base64EncodedGZipCompressedContent>(.*?)</Base64EncodedGZipCompressedContent>', xml_str, re.DOTALL)
        if match:
            inner_b64 = match.group(1).strip()
            return uuid, _decode_once(inner_b64)
    except Exception:
        return uuid, None
        
    return uuid, None
    
def parse_xml(xml_path):
    results = {}
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        ns_tag = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
        ns = {"ns": ns_tag} if ns_tag else {}
        
        for lead in root.findall(".//ns:Lead", ns) if ns else root.findall(".//Lead"):
            guid = lead.attrib.get("ID")
            fields = {val.attrib["FieldID"]: val.text for val in (lead.findall("ns:InputValue", ns) if ns else lead.findall("InputValue"))}
            results[guid] = {
                "ID": guid,
                "LAST_NAME_XML": fields.get("1", ""),
                "DATE_XML": fields.get("2", ""),
            }
    except ET.ParseError:
        print(f"Warning: Could not parse XML file {xml_path}")
    return results

def normalize_date_range(date_str):
    if not date_str or "-" not in date_str: return date_str
    try:
        left, right = [x.strip() for x in date_str.split("-")]
        mm1, dd1, yyyy1 = left.split("/")
        mm2, dd2, yyyy2 = right.split("/")
        return f"{mm1.zfill(2)}/{dd1.zfill(2)}/{yyyy1} - {mm2.zfill(2)}/{dd2.zfill(2)}/{yyyy2}"
    except Exception:
        return date_str

def extract_date_from_url(url):
    filed_from = re.search(r"filedDateFrom=(\d{4}-\d{2}-\d{2})", url)
    filed_to = re.search(r"filedDateTo=(\d{4}-\d{2}-\d{2})", url)
    if filed_from and filed_to:
        try:
            f1, f2 = filed_from.group(1), filed_to.group(1)
            dt1 = f"{int(f1[5:7]):02d}/{int(f1[8:10]):02d}/{f1[:4]}"
            dt2 = f"{int(f2[5:7]):02d}/{int(f2[8:10]):02d}/{f2[:4]}"
            return f"{dt1} - {dt2}"
        except Exception: return ""
    return ""

# --- Checking Functions ---

def check_case_status_and_category(csv_file_path, xml_filename):
    errors = []
    required_case_status = {"adjudicated", "disposed", "closed"}
    required_case_type_subcategory = {"1"}
    try:
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                id_value, url = row.get("ID", ""), row.get("URL", "")
                last_name_pos = url.find("lastName=")
                if last_name_pos == -1: continue
                
                case_status_matches = re.findall(r"caseStatus=([^&]+)", url)
                case_type_subcategory_matches = re.findall(r"caseTypeSubCategory=([^&]+)", url)
                
                if (required_case_status != set(case_status_matches) or 
                    required_case_type_subcategory != set(case_type_subcategory_matches)):
                    errors.append(f"ID: {id_value} | Tích thiếu hoặc sai caseStatus và caseTypeSubCategory")
    except Exception as e:
        errors.append(f"ID: N/A | Lỗi khi kiểm tra caseStatus: {str(e)}")
    return errors

def check_name_in_csv(csv_file_path, xml_filename):
    errors = []
    try:
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                if row.get("CHECK_NAME", "").strip().lower() == 'false':
                    errors.append(f"ID: {row.get('ID', '')} | Sai name (XML: '{row.get('LAST_NAME_XML', '')}' vs TXT: '{row.get('LAST_NAME_TXT', '')}')")
    except Exception as e:
        errors.append(f"ID: N/A | Lỗi khi kiểm tra name: {str(e)}")
    return errors

def check_date_in_csv(csv_file_path, xml_filename):
    errors = []
    try:
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                if row.get("CHECK_DATE", "").strip().lower() == 'false':
                    errors.append(f"ID: {row.get('ID', '')} | Sai DATE (XML: '{row.get('DATE_XML', '')}' vs TXT: '{row.get('DATE_TXT', '')}')")
    except Exception as e:
        errors.append(f"ID: N/A | Lỗi khi kiểm tra DATE: {str(e)}")
    return errors

def check_duplicate_id_page(csv_file_path, xml_filename):
    errors = []
    id_page_map = set()
    seen_ids = set()
    try:
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                guid, page = row.get("ID", ""), row.get("PAGE", "")
                key = (guid, page)
                if key in id_page_map:
                    if guid not in seen_ids:
                        errors.append(f"ID: {guid} | Trùng ID+PAGE")
                        seen_ids.add(guid)
                else:
                    id_page_map.add(key)
    except Exception as e:
        errors.append(f"ID: N/A | Lỗi khi kiểm tra trùng ID+PAGE: {str(e)}")
    return errors

def check_missing_collection(csv_file_path, content_txt_path, xml_filename):
    errors = []
    if not os.path.exists(content_txt_path):
        return [f"ID: N/A | Không tìm thấy file _content.txt để kiểm tra collection."]

    id_to_html = {}
    try:
        with open(content_txt_path, 'r', encoding='utf-8') as f:
            for line in f:
                guid, html_content = decode_nested_html_from_line(line)
                if guid and html_content:
                    id_to_html[guid] = html_content
    except Exception as e:
        return [f"ID: N/A | Lỗi khi đọc file content.txt: {e}"]
        
    id_pages_from_csv = defaultdict(set)
    try:
        with open(csv_file_path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                guid, url = row.get("ID", "").strip(), row.get("URL", "")
                if guid and url and (page_match := re.search(r"[?&]page=(\d+)", url)):
                    try: id_pages_from_csv[guid].add(int(page_match.group(1)))
                    except (ValueError, TypeError): pass
    except Exception as e:
        return [f"ID: N/A | Lỗi đọc file CSV: {str(e)}"]

    for guid, found_pages_set in id_pages_from_csv.items():
        html_content = id_to_html.get(guid)
        if not html_content or BeautifulSoup is None: continue
        
        expected_pages = 0
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            count_element = soup.find(string=re.compile(r"Total Record Count:\s*\d+"))
            if count_element and (match := re.search(r'(\d+)', count_element.strip())):
                total_records = int(match.group(1))
                if total_records > 0:
                    expected_pages = math.ceil(total_records / 10)
        except Exception:
            continue
            
        if expected_pages > 0:
            expected_page_set = set(range(1, int(expected_pages) + 1))
            if found_pages_set != expected_page_set:
                errors.append(f"ID: {guid} | Collection thiếu (Page chuẩn = {int(expected_pages)}, Page hiện có = {len(found_pages_set)})")
                
    return errors

# --- Main Logic Function ---

def run_mi_check(directory_path):
    results_log = []
    xml_files = [f for f in os.listdir(directory_path) if f.endswith(".xml")]
    if not xml_files:
        return "Không tìm thấy tệp .xml trong thư mục."

    for xml_file in xml_files:
        base_name = os.path.splitext(xml_file)[0]
        txt_path = os.path.join(directory_path, f"{base_name}_content.txt")
        xml_path = os.path.join(directory_path, xml_file)
        
        results_log.append(f"\n--- Đang xử lý: {base_name} ---")

        if not os.path.exists(txt_path):
            results_log.append(f"  ❌ Lỗi: Không tìm thấy tệp {os.path.basename(txt_path)}.")
            continue
            
        # 1. Create Compare CSV
        all_rows = []
        try:
            xml_data = parse_xml(xml_path)
            with open(txt_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split("|", 2)
                    if len(parts) < 3: continue
                    guid, _, encoded = parts
                    decoded = decode_txt(encoded)
                    if not decoded: continue
                    
                    xml_info = xml_data.get(guid, {})
                    last_xml = xml_info.get("LAST_NAME_XML", "").strip().upper()
                    date_xml = normalize_date_range(xml_info.get("DATE_XML", ""))
                    
                    uri_blocks = re.findall(r"<Uri>(.*?)</Uri>", decoded, re.DOTALL)
                    urls = [html.unescape(uri.strip()).replace("&amp;", "&") for uri in uri_blocks]
                    
                    last_txt = ""
                    date_txt = ""
                    if urls:
                        match = re.search(r"lastName=([^&\s]+)", urls[0])
                        if match: last_txt = match.group(1).strip().upper()
                        date_txt = extract_date_from_url(urls[0])
                        
                    for j, url in enumerate(urls, 1):
                        page_match = re.search(r"[?&]page=(\d+)", url)
                        page = page_match.group(1) if page_match else str(j)
                        check_name = "True" if last_xml == last_txt else "False"
                        check_date = "True" if date_xml == date_txt else "False"
                        all_rows.append([xml_file, guid, last_xml or last_txt, last_txt, check_name, date_xml, date_txt, check_date, str(page), url])

            output_file = os.path.join(directory_path, f"{base_name}_compare_output.csv")
            with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f, delimiter=";", lineterminator="\r\n", quoting=csv.QUOTE_ALL)
                writer.writerow(["FILE_XML", "ID", "LAST_NAME_XML", "LAST_NAME_TXT", "CHECK_NAME", "DATE_XML", "DATE_TXT", "CHECK_DATE", "PAGE", "URL"])
                writer.writerows(all_rows)
            results_log.append(f"  ✅ Đã tạo file {os.path.basename(output_file)}")
        except Exception as e:
            results_log.append(f"  ❌ Lỗi khi tạo CSV: {e}")
            continue # Skip to next file if CSV creation fails

        # 2. Run checks on the created CSV
        file_errors = []
        file_errors.extend(check_name_in_csv(output_file, xml_file))
        file_errors.extend(check_date_in_csv(output_file, xml_file))
        file_errors.extend(check_duplicate_id_page(output_file, xml_file))
        file_errors.extend(check_case_status_and_category(output_file, xml_file))
        file_errors.extend(check_missing_collection(output_file, txt_path, xml_file))

        if not file_errors:
            results_log.append("  ✅ Không phát hiện lỗi.")
        else:
            for error in file_errors:
                results_log.append(f"  ❌ {error}")

    return "\n".join(results_log)

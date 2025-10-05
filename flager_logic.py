# flager_logic.py — server version with CSV creation + collection checks

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

# --- Giải mã & đọc dữ liệu (từ bản server trước) ---
def decode_base64_gzip(data):
    try:
        if len(data) % 4: data += '=' * (4 - len(data) % 4)
        decoded = base64.b64decode(data)
        return gzip.decompress(decoded).decode('utf-8', errors='replace')
    except Exception:
        return None

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
    except ET.ParseError:
        return uuid, None
    return uuid, None

def load_xml_case_keys(xml_path):
    case_key_map = {}
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        namespace = {'ns': 'http://risk.regn.net/LeadList'}
        for lead in root.findall('.//ns:Lead', namespace):
            case_id, case_key = lead.get('ID'), lead.get('CaseKey')
            if case_id and case_key:
                case_key_map[case_id] = case_key.strip()
    except Exception as e:
        raise IOError(f"Lỗi khi đọc tệp XML {os.path.basename(xml_path)}: {e}")
    return case_key_map

def validate_html(html_content, expected_case_key):
    if not html_content:
        return False, "Nội dung HTML rỗng."
    soup = BeautifulSoup(html_content, 'lxml')

    # Các accordion quan trọng — nếu không có coi như “collection sai”
    required_section_ids = ["summaryAccordion", "partyAccordion", "chargeAccordion", "caseDocketsAccordion"]
    if all(not soup.find('div', id=sid) for sid in required_section_ids):
        return False, "Collection sai"

    case_number_tag = soup.find('dd', class_="casenumber")
    if not case_number_tag:
        return False, "Loading...(Chưa tải hết dữ liệu)"

    actual_case_number = case_number_tag.get_text(strip=True).replace('\xa0', ' ').strip()
    if actual_case_number != expected_case_key:
        return False, f"Sai caseNumber. XML: '{expected_case_key}', HTML: '{actual_case_number}'"
    return True, "Khớp"

def check_for_cases_found(html_content):
    return bool(html_content and 'cases found' in html_content.lower())

def extract_value_from_filter_div(soup, class_name):
    div = soup.find('div', class_=class_name)
    if not div: return ""
    text_nodes = div.find_all(string=True, recursive=False)
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

# ================== [ADD] Khối CSV (theo app desktop v3.5) ==================
# Tạo CSV theo đúng “xuat_csv_ChatGpt_v2”: header không quote, data QUOTE_ALL, utf-8-sig, ;, CRLF
_CSVV2_HEADER = "FILE_XML;ID;LAST_NAME_XML;LAST_NAME_TXT;CHECK_NAME;DATE_XML;DATE_TXT;CHECK_DATE;PAGE;URL\r\n"

def _csvv2_d(b64):
    try:
        b64 += "=" * ((4 - len(b64) % 4) % 4)
        return gzip.decompress(base64.b64decode(b64)).decode("utf-8", "replace")
    except Exception:
        return ""

def _csvv2_px(xml_path):
    # lấy FieldID 1 (LAST_NAME_XML), FieldID 2 (DATE_XML) — giữ đúng semantics app
    r = ET.parse(xml_path).getroot()
    ns = ""
    leads = []
    input_tag = "InputValue"
    if r.tag.startswith("{"):
        ns = r.tag[1:r.tag.index("}")]
        leads = r.findall(".//{"+ns+"}Lead")
        input_tag = "{"+ns+"}InputValue"
    else:
        leads = r.findall(".//Lead")
    z = {}
    for e in leads:
        gid = e.attrib.get("ID")
        fields = {iv.attrib.get("FieldID",""): (iv.text or "") for iv in e.findall(input_tag)}
        z[gid] = [(fields.get("1","") or ""), (fields.get("2","") or "")]
    return z

def _csvv2_nd(s):
    # chuẩn hoá “MM/DD/YYYY - MM/DD/YYYY”
    if not s or "-" not in s:
        return s or ""
    try:
        a, b = [x.strip() for x in s.split("-")]
        m1, d1, y1 = [t.strip() for t in a.split("/")]
        m2, d2, y2 = [t.strip() for t in b.split("/")]
        return f"{m1.zfill(2)}/{d1.zfill(2)}/{y1} - {m2.zfill(2)}/{d2.zfill(2)}/{y2}"
    except Exception:
        return s

def _csvv2_du(u):
    # trích khoảng ngày từ URL
    a = re.search(r"filedDateFrom=(\d{4}-\d{2}-\d{2})", u or "")
    b = re.search(r"filedDateTo=(\d{4}-\d{2}-\d{2})", u or "")
    if a and b:
        try:
            f1, f2 = a.group(1), b.group(1)
            d1 = f"{int(f1[5:7]):02d}/{int(f1[8:10]):02d}/{f1[:4]}"
            d2 = f"{int(f2[5:7]):02d}/{int(f2[8:10]):02d}/{f2[:4]}"
            return f"{d1} - {d2}"
        except Exception:
            return ""
    return ""

def _csvv2_create_for_pair(xml_path, txt_path, output_dir, logger=None):
    """
    Tạo <XMLBase>_compare_output.csv nếu chưa tồn tại,
    y hệt luồng trong app desktop: PAGE luôn '0', URL giữ nguyên.
    """
    def log(msg): 
        if logger: logger(msg)

    try:
        z = _csvv2_px(xml_path)
    except Exception as e:
        log(f"XML {e}")
        return None

    rows = []
    try:
        with open(txt_path, encoding="utf-8") as f:
            for line in f:
                p = line.strip().split("|", 2)
                if len(p) < 3:
                    continue
                gid, enc = p[0], p[2]
                decoded = _csvv2_d(enc)
                # XML fields
                last_xml = (z.get(gid, ["",""])[0] or "").strip().upper()
                date_xml = _csvv2_nd(z.get(gid, ["",""])[1] or "")
                # URLs
                urls = [html.unescape(u.strip()).replace("&amp;", "&")
                        for u in re.findall(r"<Uri>(.*?)</Uri>", decoded, re.S)]
                last_txt = ""
                date_txt = ""
                if urls:
                    m = re.search(r"lastName=([^&\s]+)", urls[0])
                    if m: last_txt = m.group(1).strip().upper()
                    date_txt = _csvv2_du(urls[0])
                    for u in urls:
                        rows.append([
                            os.path.basename(xml_path), gid, last_xml or last_txt, last_txt,
                            "True" if last_xml == last_txt else "False",
                            date_xml, date_txt, 
                            "True" if date_xml == date_txt else "False",
                            "0", u
                        ])
                else:
                    rows.append([
                        os.path.basename(xml_path), gid, last_xml, last_txt,
                        "True" if last_xml == last_txt else "False",
                        date_xml, date_txt, 
                        "True" if date_xml == date_txt else "False",
                        "0", ""
                    ])
    except Exception as e:
        log(f"TXT {e}")
        return None

    base = os.path.splitext(os.path.basename(xml_path))[0]
    out_csv = os.path.join(output_dir, f"{base}_compare_output.csv")
    try:
        with open(out_csv, "w", encoding="utf-8-sig", newline="") as w:
            # header KHÔNG quote
            w.write(_CSVV2_HEADER)
            # data QUOTE_ALL
            writer = csv.writer(w, delimiter=";", lineterminator="\r\n", quoting=csv.QUOTE_ALL)
            writer.writerows(rows)
        log(f"OK {os.path.basename(out_csv)} ({len(rows)} dòng)")
        return out_csv
    except Exception as e:
        log(f"CSV {e}")
        return None
# ================== [END ADD] ==================

def _collect_html_and_basic_checks(xml_file, content_file, results_log):
    """Giai đoạn 1: đọc HTML từ TXT & kiểm tra caseNumber / 'cases found' / collection cơ bản."""
    xml_filename = os.path.basename(xml_file)
    case_key_map = load_xml_case_keys(xml_file)
    html_in_memory = {}
    hard_error_uuids = set()
    errors_for_this_file = []

    try:
        with open(content_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            uuid, html_content = decode_nested_base64(line)
            if not uuid:
                continue
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

    return html_in_memory, hard_error_uuids

def _ensure_csv_and_check_collection(xml_file, content_file, html_in_memory, hard_error_uuids, results_log):
    """
    Giai đoạn 2: tìm CSV (hoặc tạo nếu chưa có) rồi kiểm tra Collection theo đúng luật
    như app desktop v3.5.
    """
    xml_filename = os.path.basename(xml_file)
    directory = os.path.dirname(xml_file)
    xml_base_name = os.path.splitext(xml_filename)[0]

    # Tên CSV khả dĩ (giữ tương thích với thói quen đặt tên)
    candidates = [f"{xml_base_name}_Compare.csv", f"{xml_base_name}_compare_output.csv"]
    found_csv_path = None
    try:
        dir_files = os.listdir(directory)
        for name in candidates:
            if any(f.lower() == name.lower() for f in dir_files):
                found_csv_path = os.path.join(directory, name)
                break
        # Nếu không có, TẠO THEO CHUẨN xuat_csv_ChatGpt_v2
        if not found_csv_path:
            created = _csvv2_create_for_pair(xml_file, content_file, directory)
            if created:
                found_csv_path = created
                results_log.append(f"📝 Đã tạo CSV: {os.path.basename(created)}")
            else:
                results_log.append("⚠️ Không thể tạo CSV; bỏ qua kiểm tra Collection.")
                return

        # Đọc CSV thành map ID → list(URL)
        id_to_urls = defaultdict(list)
        try:
            with open(found_csv_path, 'r', encoding='utf-8-sig', newline='') as f:
                try:
                    reader = csv.DictReader(f, delimiter=';')
                    headers = reader.fieldnames or []
                    if 'ID' not in headers or 'URL' not in headers:
                        raise ValueError("Header 'ID' hoặc 'URL' không tồn tại.")
                    for row in reader:
                        uuid, url = row.get('ID'), row.get('URL')
                        if uuid is not None:
                            id_to_urls[uuid].append(url)
                except (ValueError, csv.Error):
                    # fallback đọc thủ công
                    f.seek(0)
                    lines = f.readlines()
                    if lines:
                        header = [h.strip().lower() for h in lines[0].split(';')]
                        id_idx = header.index('id') if 'id' in header else -1
                        url_idx = header.index('url') if 'url' in header else -1
                        if id_idx == -1 or url_idx == -1:
                            results_log.append("Lỗi: CSV không có cột 'ID' hoặc 'URL'.")
                            return
                        for line in lines[1:]:
                            cols = [c.strip() for c in line.split(';')]
                            if len(cols) > max(id_idx, url_idx):
                                uuid, url = cols[id_idx], cols[url_idx]
                                if uuid: id_to_urls[uuid].append(url)
        except Exception as e:
            results_log.append(f"Lỗi đọc CSV '{os.path.basename(found_csv_path)}': {e}")
            return

        # Kiểm tra Collection cho từng ID (giống app)
        for uuid, urls in id_to_urls.items():
            if uuid in hard_error_uuids:
                continue

            count = len(urls)
            # Nếu bất kỳ URL có chứa 'error' → coi như trang chưa load được
            if any(((u or "").lower().find("error") != -1) for u in urls):
                results_log.append(f"ID:{uuid}| Collection sai (Trang chưa load được)")
                hard_error_uuids.add(uuid)
                continue

            html_content = html_in_memory.get(uuid)
            has_cases_found = check_for_cases_found(html_content)

            if has_cases_found:
                # Trang 'cases found' → CSV phải có 1 dòng
                if count != 1:
                    results_log.append(f"ID:{uuid}| Collection sai (có 'cases found' nhưng count={count} dòng)")
                    hard_error_uuids.add(uuid)
            else:
                # Trang chi tiết → CSV phải có 2 dòng, và 2 URL phải khác nhau
                if count != 2:
                    results_log.append(f"ID:{uuid}| Collection sai (phải có 2 dòng nhưng count={count} dòng)")
                    hard_error_uuids.add(uuid)
                elif len(set(urls)) != 2:
                    results_log.append(f"ID:{uuid}| Collection sai (có 2 dòng nhưng URL giống nhau)")
                    hard_error_uuids.add(uuid)
    except Exception as e:
        results_log.append(f"Lỗi khi kiểm tra CSV cho {xml_filename}: {e}")

def run_flager_check(directory_path):
    """
    Hàm chính để chạy toàn bộ logic kiểm tra cho tool Flager từ server:
      - Giai đoạn 1: kiểm tra HTML (caseNumber / 'cases found')
      - Giai đoạn 2: đảm bảo có CSV (tự tạo nếu thiếu) rồi kiểm tra Collection theo CSV
    """
    data_dir = resolve_data_dir(directory_path)  # <-- CHUẨN HOÁ
    results_log = []
    results_log.append("--- Bắt đầu quá trình quét file cho tool Flager ---")
    try:
        content_files = [f for f in os.listdir(data_dir) if f.lower().endswith("_content.txt")]
        file_pairs = []
        for content_filename in content_files:
            base_name = content_filename[:-12]  # remove '_content.txt'
            xml_filename = base_name + '.xml'
            xml_path = os.path.join(data_dir, xml_filename)
            if os.path.exists(xml_path):
                file_pairs.append((xml_path, os.path.join(data_dir, content_filename)))
            else:
                results_log.append(f"Cảnh báo: Tìm thấy {content_filename} nhưng không có file {xml_filename} tương ứng.")
        if not file_pairs:
            return "Lỗi: Không tìm thấy cặp file `_content.txt` và `.xml` hợp lệ nào."

        results_log.append(f"Đã phát hiện {len(file_pairs)} cặp file hợp lệ. Bắt đầu xử lý...")

        for (xml_file, content_file) in file_pairs:
            xml_filename = os.path.basename(xml_file)
            results_log.append(f"\n--- Đang xử lý: {xml_filename} ---")

            # Giai đoạn 1 — HTML
            html_in_memory, hard_error_uuids = _collect_html_and_basic_checks(xml_file, content_file, results_log)

            # Giai đoạn 2 — CSV & Collection
            _ensure_csv_and_check_collection(xml_file, content_file, html_in_memory, hard_error_uuids, results_log)

        total_errors = len([line for line in results_log if line.strip().startswith('ID:')])
        results_log.append(f"\n--- HOÀN THÀNH ---")
        results_log.append(f"Tổng cộng có {total_errors} lỗi được phát hiện.")
    except FileNotFoundError:
        return f"Lỗi: Thư mục '{data_dir}' không tồn tại trên server."
    except Exception as e:
        return f"Lỗi không xác định xảy ra trong quá trình xử lý: {str(e)}"

    return "\n".join(results_log)

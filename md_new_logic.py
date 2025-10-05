import os
import re
import base64
import gzip
import xml.etree.ElementTree as ET
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

# ===== Helpers decode =====
def b64_gzip_decode_best_effort(s: str) -> str:
    try:
        payload = ''.join(s.strip().split())
        if len(payload) % 4:
            payload += '=' * (4 - len(payload) % 4)
        raw = base64.b64decode(payload, validate=False)
        try:
            raw = gzip.decompress(raw)
        except OSError:
            pass
        for enc in ("utf-8", "latin-1"):
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return ""

def decode_nested_txt_line(line_content):
    uuid, html, error = None, None, None
    parts = line_content.split('|')
    if len(parts) >= 3:
        uuid = parts[0]
        try:
            outer = b64_gzip_decode_best_effort(parts[2])
            root = ET.fromstring(outer)
            inner = root.find('Base64EncodedGZipCompressedContent')
            if inner is not None and inner.text:
                html = b64_gzip_decode_best_effort(inner.text.strip())
            else:
                error = "Không tìm thấy nội dung lồng nhau."
        except Exception as e:
            error = f"Lỗi giải mã: {e}"
    else:
        error = "Định dạng dòng TXT không hợp lệ."
    return uuid, html, error

# ===== CaseType from filename =====
def infer_case_type_from_filename(filename: str) -> str:
    base = os.path.basename(filename)
    name = os.path.splitext(base)[0]
    tokens = [t for t in re.split(r'[_\-]+', name) if t != ""]
    if "NameSearch" in tokens:
        idx = tokens.index("NameSearch")
        if idx > 0:
            return tokens[idx - 1]
    for tok in tokens:
        if tok.upper() == "CASENUMBERFILE":
            return "CASENUMBERFILE"
    try:
        if tokens and tokens[0].upper().startswith("MDSWJD") and len(tokens) >= 3:
            return tokens[2]
    except Exception:
        pass
    for tok in tokens:
        up = tok.upper()
        if up in ("ALLCASETYPES", "ALLCASETYPE", "ALLCASES", "ALLCASE"):
            return "ALLCASETYPES"
        if "CASETYPE" in up:
            return tok
    return ""

# ===== Main checker =====
def run_md_moi_check(directory_path):
    data_dir = resolve_data_dir(directory_path)  # <-- CHUẨN HOÁ
    log = []
    xml_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".xml")]
    if not xml_files:
        return "Không tìm thấy tệp .xml nào trong thư mục."
    for xml_file in xml_files:
        base_name = os.path.splitext(xml_file)[0]
        txt_path = os.path.join(data_dir, f"{base_name}_content.txt")
        xml_path = os.path.join(data_dir, xml_file)
        log.append(f"\n--- Đang xử lý (MD Mới): {base_name} ---")
        if not os.path.exists(txt_path):
            log.append(f"  ❌ Lỗi: Thiếu tệp TXT '{os.path.basename(txt_path)}'.")
            continue
        case_type_from_name = infer_case_type_from_filename(xml_file)
        if not case_type_from_name:
            case_type_from_name = "ALLCASETYPES"
            log.append("  ⚠️ Không tách được Case Type từ tên file; dùng mặc định: ALLCASETYPES")
        case_keys_from_xml = {}
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            namespace = {'ns': 'http://risk.regn.net/LeadList'}
            for lead in root.findall('.//ns:Lead', namespace):
                lead_id, case_key = lead.get('ID'), lead.get('CaseKey')
                if lead_id and case_key:
                    case_keys_from_xml[lead_id] = case_key
        except Exception as e:
            log.append(f"  ❌ Lỗi đọc XML: {e}")
            continue
        uuid_to_html = {}
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith("HEADER ROW") or not line.strip():
                        continue
                    uuid, html, error = decode_nested_txt_line(line)
                    if uuid:
                        uuid_to_html[uuid] = (html, error)
        except Exception as e:
            log.append(f"  ❌ Lỗi đọc file TXT: {e}")
            continue
        file_errors = []
        for lead_id, case_key_raw in case_keys_from_xml.items():
            html_content, decode_error = uuid_to_html.get(lead_id, (None, "Không tìm thấy ID trong TXT"))
            if decode_error:
                file_errors.append(f"ID: {lead_id} | Lỗi: {decode_error}")
                continue
            if not html_content:
                file_errors.append(f"ID: {lead_id} | Lỗi: Nội dung HTML rỗng")
                continue
            case_key_match = re.search(r"([\d\/\-]{10})-([\d\/\-]{10}) (.*?)%,(.*?)%", case_key_raw)
            if not case_key_match:
                continue
            range_from_xml, range_to_xml, last_name_xml, first_name_xml = [s.strip() for s in case_key_match.groups()]
            last_name_xml += "%"
            first_name_xml += "%"
            lead_errors = []
            if "DATA NOT FOUND" in html_content:
                fn_html = re.search(r'<input[^>]*name="firstName"[^>]*value="([^"]*)"[^>]*>', html_content, re.I)
                ln_html = re.search(r'<input[^>]*name="lastName"[^>]*value="([^"]*)"[^>]*>', html_content, re.I)
                start_html = re.search(r'<input[^>]*name="filingStart"[^>]*value="([^"]*)"[^>]*>', html_content, re.I)
                end_html = re.search(r'<input[^>]*name="filingEnd"[^>]*value="([^"]*)"[^>]*>', html_content, re.I)
                if not fn_html or fn_html.group(1).strip() != first_name_xml:
                    lead_errors.append("First Name")
                if not ln_html or ln_html.group(1).strip() != last_name_xml:
                    lead_errors.append("Last Name")
                try:
                    if not start_html or datetime.strptime(start_html.group(1).strip(), '%m/%d/%Y') != datetime.strptime(range_from_xml, '%m/%d/%Y'):
                        lead_errors.append("Range From")
                    if not end_html or datetime.strptime(end_html.group(1).strip(), '%m/%d/%Y') != datetime.strptime(range_to_xml, '%m/%d/%Y'):
                        lead_errors.append("Range To")
                except ValueError:
                    lead_errors.append("Filing Date Range (invalid format)")
            else:
                fn_html = re.search(r"First Name:\s*<span[^>]*>([\w\s%]+?)</span>", html_content, re.I)
                ln_html = re.search(r"Last Name:\s*<span[^>]*>([\w\s%]+?)</span>", html_content, re.I)
                range_html = re.search(r"Filing Date Range:\s*<span[^>]*>([\w\s\/\- to]+?)</span>", html_content, re.I)
                if not fn_html or fn_html.group(1).strip() != first_name_xml:
                    lead_errors.append("First Name")
                if not ln_html or ln_html.group(1).strip() != last_name_xml:
                    lead_errors.append("Last Name")
                if not range_html:
                    lead_errors.append("Filing Date Range")
                else:
                    try:
                        start_str_html, end_str_html = [d.strip() for d in range_html.group(1).strip().split("to")]
                        if datetime.strptime(start_str_html, '%m/%d/%Y') != datetime.strptime(range_from_xml, '%m/%d/%Y') or \
                           datetime.strptime(end_str_html, '%m/%d/%Y') != datetime.strptime(range_to_xml, '%m/%d/%Y'):
                            lead_errors.append("Filing Date Range")
                    except (ValueError, IndexError):
                        lead_errors.append("Filing Date Range (invalid format)")
            if lead_errors:
                file_errors.append(f"ID: {lead_id} | Lỗi sai do: {', '.join(lead_errors)}")
        if not file_errors:
            log.append("  ✅ Không có lỗi.")
        else:
            for err in file_errors:
                log.append(f"  ❌ {err}")
    return "\n".join(log)

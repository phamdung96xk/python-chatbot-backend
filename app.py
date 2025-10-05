# app.py (full)
import os, io, zipfile, tempfile, time, uuid, re, glob, importlib, inspect, threading, fnmatch, shutil
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, make_response, send_file, redirect
from flask_cors import CORS
from concurrent.futures import ProcessPoolExecutor, TimeoutError

# boto3 (optional, cho S3 nếu dùng)
try:
    import boto3  # type: ignore
except Exception:
    boto3 = None

# ================= Flask app & config =================
app = Flask(__name__, static_folder="static", static_url_path="/static")
# Giới hạn kích thước request: 500MB
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024

UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# CORS mở cho /api/* (same-origin vẫn OK)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)

# S3 (optional)
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
s3 = boto3.client("s3", region_name=AWS_REGION) if (boto3 and S3_BUCKET) else None

# ================== Disk & janitor helpers ==================
def dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path, onerror=lambda e: None):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except Exception:
                pass
    return total

def human(n):
    for u in ["B","KB","MB","GB","TB"]:
        if n < 1024: return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"

def ensure_free_space(min_free_bytes: int, base_dir: str = "/tmp"):
    """Raise RuntimeError nếu dung lượng trống < ngưỡng."""
    usage = shutil.disk_usage(base_dir)
    if usage.free < min_free_bytes:
        raise RuntimeError(
            f"Hệ thống sắp đầy đĩa: còn {human(usage.free)} trống (< {human(min_free_bytes)})."
        )

def cleanup_uploads(max_age_hours: int = 12,
                    max_total_bytes: int = 3 * 1024**3,   # ~3GB
                    base_dir: str = UPLOAD_DIR):
    """
    Xóa thư mục/file tạm quá tuổi hoặc khi tổng dung lượng vượt ngưỡng.
    1) Xóa theo tuổi
    2) Nếu vẫn > ngưỡng: xóa LRU (cũ trước) cho tới khi đủ.
    """
    try:
        if not os.path.isdir(base_dir): return
        now = time.time()
        items = []
        total = 0
        for name in os.listdir(base_dir):
            p = os.path.join(base_dir, name)
            try:
                st = os.stat(p)
                size = dir_size_bytes(p) if os.path.isdir(p) else os.path.getsize(p)
                total += size
                items.append((p, st.st_mtime, size))
            except Exception:
                pass

        # 1) Xóa theo tuổi
        cutoff = now - max_age_hours * 3600
        for p, mtime, _ in items:
            if mtime < cutoff:
                shutil.rmtree(p, ignore_errors=True)

        # Quét lại
        items2, total2 = [], 0
        for name in os.listdir(base_dir):
            p = os.path.join(base_dir, name)
            try:
                st = os.stat(p)
                size = dir_size_bytes(p) if os.path.isdir(p) else os.path.getsize(p)
                total2 += size
                items2.append((p, st.st_mtime, size))
            except Exception:
                pass

        # 2) Nếu vẫn > ngưỡng -> xóa LRU
        if total2 > max_total_bytes:
            items2.sort(key=lambda x: x[1])  # mtime tăng dần (cũ trước)
            for p, _, sz in items2:
                shutil.rmtree(p, ignore_errors=True)
                total2 -= sz
                if total2 <= max_total_bytes:
                    break
    except Exception:
        # Không để lỗi dọn rác phá request
        pass

@app.before_request
def _maybe_cleanup_tmp():
    # 1% xác suất dọn rác cho mỗi request (nhẹ, không block)
    if uuid.uuid4().int % 100 == 0:
        cleanup_uploads(
            max_age_hours=int(os.environ.get("TMP_MAX_AGE_H", "12")),
            max_total_bytes=int(float(os.environ.get("TMP_MAX_TOTAL_GB", "3")) * 1024**3),
        )

# ================ Job store & ProcessPool ================
JOBS = {}  # job_id -> {"status": "queued|running|done|error", "result": ..., "error": ..., "updated": datetime}

WORKERS = int(os.environ.get("WORKERS", "2"))
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "900"))  # 15 phút
EXEC = ProcessPoolExecutor(max_workers=WORKERS)

def _gc_jobs(hours: int = 6):
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    for k, v in list(JOBS.items()):
        if v.get("updated") and v["updated"] < cutoff:
            del JOBS[k]

def _run_command_background(job_id: str, command: str):
    """Chạy command trong tiến trình riêng (CPU-bound không chặn web worker)."""
    try:
        JOBS[job_id] = {"status": "running", "updated": datetime.utcnow()}
        fut = EXEC.submit(route_command, command)  # chạy ở process khác
        res = fut.result(timeout=JOB_TIMEOUT)
        if res.get("ok"):
            if res.get("help"):
                out = {"result": res.get("message"), "help": True}
            else:
                out = {
                    "result": res.get("output"),
                    "module": res.get("module"),
                    "fn": res.get("fn"),
                    "data_dir": res.get("data_dir"),
                }
            JOBS[job_id] = {"status": "done", "result": out, "updated": datetime.utcnow()}
        else:
            JOBS[job_id] = {"status": "error", "error": res.get("error"), "updated": datetime.utcnow()}
    except TimeoutError:
        JOBS[job_id] = {"status": "error", "error": f"Timeout > {JOB_TIMEOUT}s", "updated": datetime.utcnow()}
    except Exception as e:
        JOBS[job_id] = {"status": "error", "error": f"{type(e).__name__}: {e}", "updated": datetime.utcnow()}

# ================== ZIP helpers & download ==================
def analyze_zip_stream(body_bytes: bytes) -> dict:
    t0 = time.time()
    try:
        zf = zipfile.ZipFile(io.BytesIO(body_bytes))
        names = zf.namelist()
        return {"ok": True, "elapsed_sec": round(time.time()-t0, 3),
                "summary": {"num_entries_in_zip": len(names), "sample_first_10": names[:10]}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

def analyze_zip_file(zip_path: str) -> dict:
    t0 = time.time()
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            names = z.namelist()
        return {"ok": True, "elapsed_sec": round(time.time()-t0, 3),
                "summary": {"entries": len(names), "sample": names[:10]}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

def _normalize_gdrive(url: str) -> str:
    m = re.search(r"/file/d/([^/]+)", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([^&]+)", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def _download_zip_to_file(url: str, dest_path: str):
    """
    - Tải ZIP 1MB/chunk (nhanh hơn).
    - Bỏ qua HTML confirm của Google Drive nếu có.
    - Xác thực nội dung cuối cùng là ZIP.
    - Kiểm tra dung lượng trống trước khi ghi.
    """
    ensure_free_space(min_free_bytes=500 * 1024 * 1024, base_dir=os.path.dirname(dest_path))
    is_drive = "drive.google.com" in url
    if is_drive:
        url = _normalize_gdrive(url)

    with requests.Session() as s:
        r = s.get(url, stream=True, allow_redirects=True, timeout=(30, 300))
        if is_drive and "text/html" in (r.headers.get("Content-Type", "")).lower():
            try:
                text = r.text
                if "confirm=" in text and "uc?export=download" in text:
                    m = re.search(r'href="(\/uc\?export=download[^"]+confirm=[^"]+)"', text)
                    if m:
                        confirm_url = "https://drive.google.com" + m.group(1).replace("&amp;", "&")
                        r = s.get(confirm_url, stream=True, allow_redirects=True, timeout=(30, 300))
            except Exception:
                pass

        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):  # 1MB/chunk
                if chunk:
                    f.write(chunk)

    if not zipfile.is_zipfile(dest_path):
        raise ValueError("Nội dung tải về không phải file ZIP.")

# ========= Chỉ extract file cần (XML & *_content.txt) =========
def extract_needed(zippath: str, outdir: str):
    """
    Chỉ giải nén *.xml và *_content.txt để giảm I/O và tăng tốc.
    """
    os.makedirs(outdir, exist_ok=True)
    with zipfile.ZipFile(zippath, "r") as z:
        for info in z.infolist():
            name = info.filename
            low = name.lower()
            if low.endswith(".xml") or low.endswith("_content.txt"):
                z.extract(info, path=outdir)

# ================== Data-dir canonicalization ==================
XML_PATTERNS = ["*.xml", "*.[xX][mM][lL]"]
TXT_PATTERNS = ["*_content.txt", "*_CONTENT.TXT", "*_Content.txt"]

def _has_data(dirpath: str) -> bool:
    try:
        for pat in XML_PATTERNS + TXT_PATTERNS:
            if glob.glob(os.path.join(dirpath, pat)):
                return True
    except Exception:
        pass
    return False

def _single_child_dir(dirpath: str):
    try:
        names = [n for n in os.listdir(dirpath) if os.path.isdir(os.path.join(dirpath, n))]
        if len(names) == 1:
            return os.path.join(dirpath, names[0])
    except Exception:
        pass
    return None

def _first_data_dir_recursive(root_dir: str):
    try:
        for cur, dirs, files in os.walk(root_dir):
            if _has_data(cur):
                return cur
    except Exception:
        pass
    return None

def _canonical_data_dir(root_dir: str) -> str:
    root_dir = os.path.abspath(root_dir)
    if os.path.isdir(root_dir) and _has_data(root_dir):
        return root_dir
    test_dir = os.path.join(root_dir, "Test")
    if os.path.isdir(test_dir) and _has_data(test_dir):
        return test_dir
    child = _single_child_dir(root_dir)
    if child:
        test2 = os.path.join(child, "Test")
        if os.path.isdir(test2) and _has_data(test2):
            return test2
        if _has_data(child):
            return child
    found = _first_data_dir_recursive(root_dir)
    if found:
        return found
    return root_dir

# ================== Drive visibility probe ==================
def _probe_drive_visibility(url: str):
    norm = _normalize_gdrive(url)
    try:
        with requests.Session() as s:
            r = s.get(norm, headers={"Range": "bytes=0-512"}, stream=True,
                      allow_redirects=True, timeout=(10, 20))
            status = r.status_code
            ctype = (r.headers.get("Content-Type") or "").lower()
            cdisp = (r.headers.get("Content-Disposition") or "").lower()
            if status in (401, 403):
                return {"public": False, "reason": f"HTTP {status} (cần quyền)", "normalized_url": norm}
            if status == 404:
                return {"public": False, "reason": "HTTP 404 (không tồn tại/không truy cập được)", "normalized_url": norm}
            if "text/html" in ctype:
                try:
                    snippet = r.raw.read(512, decode_content=True) or b""
                    text = snippet.decode("utf-8", errors="ignore").lower()
                    if ("you need access" in text or "request access" in text
                        or "sign in" in text or "accounts.google.com" in text):
                        return {"public": False, "reason": "Drive báo cần đăng nhập/đòi quyền", "normalized_url": norm}
                except Exception:
                    pass
            if "attachment" in cdisp:
                return {"public": True, "reason": "Có header tải xuống", "normalized_url": norm}
            try:
                r2 = s.get(norm, headers={"Range": "bytes=0-8"}, stream=True,
                           allow_redirects=True, timeout=(10, 20))
                head = next(r2.iter_content(16), b"")
                if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06") or head.startswith(b"PK\x07\x08"):
                    return {"public": True, "reason": "Nhận diện chữ ký ZIP", "normalized_url": norm}
            except Exception:
                pass
            return {"public": True, "reason": "Truy cập được", "normalized_url": norm}
    except Exception as e:
        return {"public": False, "reason": f"Lỗi probe: {type(e).__name__}: {e}", "normalized_url": norm}

# ================== Health & OPTIONS ==================
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": time.time()})

@app.route("/api/<path:any_path>", methods=["OPTIONS"])
def api_options(any_path):
    resp = make_response("", 204)
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    resp.headers.setdefault("Access-Control-Allow-Headers", "*")
    return resp

# ================== Upload (form-data) ==================
@app.route("/api/upload-files", methods=["POST", "OPTIONS"])
def upload_files():
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        if "file" not in request.files:
            return jsonify({"error": "Thiếu field 'file' trong form-data"}), 400
        f = request.files["file"]
        if f.filename == "":
            return jsonify({"error": "Tên file rỗng"}), 400
        ensure_free_space(min_free_bytes=500 * 1024 * 1024, base_dir=UPLOAD_DIR)
        save_path = os.path.join(UPLOAD_DIR, f.filename)
        f.save(save_path)
        return jsonify({"status": "ok", "filename": f.filename, "saved_to": save_path})
    except OSError as e:
        if getattr(e, "errno", None) == 28:  # ENOSPC
            return jsonify({"ok": False, "error": "Server hết dung lượng tạm, vui lòng thử lại sau."}), 507
        raise

# ======= Trích xuất ZIP đã upload -> chỉ extract file cần -> trả data_dir =======
@app.route("/api/extract-uploaded", methods=["POST"])
def extract_uploaded():
    """
    JSON body:
      - saved_to: đường dẫn tuyệt đối file zip đã upload (ưu tiên)
      - hoặc filename: tên file trong /tmp/uploads
    Trả về:
      { ok: true, data_dir: "...", extracted_to: "...", summary: {...} }
    """
    data = request.get_json(silent=True) or {}
    saved_to = data.get("saved_to")
    filename = data.get("filename")
    if not saved_to and not filename:
        return jsonify({"ok": False, "error": "Thiếu 'saved_to' hoặc 'filename'"}), 400

    if not saved_to:
        saved_to = os.path.join(UPLOAD_DIR, filename)

    if not os.path.isfile(saved_to):
        return jsonify({"ok": False, "error": f"Không tìm thấy file: {saved_to}"}), 404
    if not zipfile.is_zipfile(saved_to):
        return jsonify({"ok": False, "error": "File không phải ZIP hợp lệ"}), 400

    try:
        ensure_free_space(min_free_bytes=500 * 1024 * 1024, base_dir="/tmp")
        extract_dir = tempfile.mkdtemp(prefix="ul_", dir=UPLOAD_DIR)
        extract_needed(saved_to, extract_dir)

        # XÓA file ZIP gốc ngay khi đã extract
        try:
            os.remove(saved_to)
        except Exception:
            pass

        best_dir = _canonical_data_dir(extract_dir)

        # tóm tắt
        found_xml, found_txt = [], []
        for root, dirs, files in os.walk(extract_dir):
            for n in files:
                nl = n.lower()
                if nl.endswith(".xml"):
                    found_xml.append(os.path.relpath(os.path.join(root, n), extract_dir))
                if nl.endswith("_content.txt"):
                    found_txt.append(os.path.relpath(os.path.join(root, n), extract_dir))

        return jsonify({
            "ok": True,
            "saved_to": saved_to,
            "extracted_to": extract_dir,
            "data_dir": best_dir,
            "summary": {
                "xml_count": len(found_xml),
                "txt_count": len(found_txt),
                "xml_sample": found_xml[:10],
                "txt_sample": found_txt[:10],
            }
        })
    except OSError as e:
        if getattr(e, "errno", None) == 28:
            return jsonify({"ok": False, "error": "Server hết dung lượng tạm khi giải nén. Thử xóa bớt hoặc đợi janitor dọn."}), 507
        raise
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500

# ======= Demo: tóm tắt ZIP từ URL + probe quyền Drive =======
@app.route("/api/analyze-by-url", methods=["POST"])
def analyze_by_url():
    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    try:
        visibility = None
        if "drive.google.com" in url:
            visibility = _probe_drive_visibility(url)
            if not visibility.get("public"):
                return jsonify({
                    "ok": False,
                    "visibility": visibility,
                    "error": "Link Drive chưa công khai (Anyone with the link)."
                }), 400
            url = visibility.get("normalized_url", url)

        tmp = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}.zip")
        _download_zip_to_file(url, tmp)
        with open(tmp, "rb") as f:
            result = analyze_zip_stream(f.read())
        # Xóa file tạm
        try:
            os.remove(tmp)
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "source": url,
            "visibility": visibility or {"public": True, "reason": "URL thường"},
            "result": result
        })
    except OSError as e:
        if getattr(e, "errno", None) == 28:
            return jsonify({"ok": False, "error": "Server hết dung lượng tạm khi tải URL."}), 507
        raise
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500

# ================== Dispatcher & tool mapping ==================
TOOL_KEYWORDS = [
    (r"\bcivitek\s+new\b", "civitek_new_logic"),
    (r"\bmd\s+new\b",      "md_new_logic"),
    (r"\bcivitek\b",       "civitek_logic"),
    (r"\bflager\b",        "flager_logic"),
    (r"\bmi\b",            "mi_logic"),
    (r"\bmd\b",            "md_logic"),
]

HELP_TEXT = (
    "HƯỚNG DẪN NHANH:\n"
    "• Civitek: gõ \"civitek <dấu cách> (link google drive)\"\n"
    "• Civitek new: gõ \"civitek new <dấu cách> (link google drive)\"\n"
    "• Flager: gõ \"flager <dấu cách> (link google drive)\"\n"
    "• MI: gõ \"mi <dấu cách> (link google drive)\"\n"
    "• MD: gõ \"md <dấu cách> (link google drive)\"\n"
    "• MD New: gõ \"md new <dấu cách> (link google drive)\"\n"
    "Mẹo: dùng gợi ý (autocomplete) cho nhanh."
)

def _call_tool_module(module_name: str, command: str):
    """
    Gọi module tool theo 3 bước:
    1) Nếu module có run/main/handle thì gọi thẳng.
    2) Nếu lệnh có URL: tải ZIP, chỉ extract file cần, tự gán path= dir tốt nhất.
    3) Nếu module có run_*_check(dir) thì gọi với data_dir đã chuẩn hoá.
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {"ok": False, "error": f"Không import được module '{module_name}': {e}"}

    # (1) run/main/handle
    for fn_name in ("run", "main", "handle"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                out = fn(command) if len(sig.parameters) >= 1 else fn()
                return {"ok": True, "module": module_name, "fn": fn_name, "output": out}
            except Exception as e:
                return {"ok": False, "module": module_name, "fn": fn_name,
                        "error": f"Lỗi khi gọi {module_name}.{fn_name}: {type(e).__name__}: {e}"}

    # (2) URL -> tải & chỉ extract file cần -> auto path
    murl = re.search(r"url\s*=\s*([^\s]+)", command, flags=re.I)
    if not murl:
        murl = re.search(r"(https?://\S+)", command, flags=re.I)
    if murl:
        url_in = murl.group(1)
        try:
            tmp_zip = os.path.join(UPLOAD_DIR, f"link_{uuid.uuid4().hex}.zip")
            _download_zip_to_file(url_in, tmp_zip)

            extract_dir = tempfile.mkdtemp(prefix="gd_", dir=UPLOAD_DIR)
            ensure_free_space(min_free_bytes=500 * 1024 * 1024, base_dir="/tmp")
            extract_needed(tmp_zip, extract_dir)

            # Xóa zip tải về ngay
            try: os.remove(tmp_zip)
            except Exception: pass

            best_dir = _canonical_data_dir(extract_dir)
            if not re.search(r"\bpath\s*=", command, flags=re.I):
                command = f"{command} path={best_dir}"
        except OSError as e:
            if getattr(e, "errno", None) == 28:
                return {"ok": False, "error": "Server hết dung lượng tạm khi tải/giải nén URL."}
            return {"ok": False, "error": f"Tải/Giải nén từ URL lỗi: {type(e).__name__}: {e}"}
        except Exception as e:
            return {"ok": False, "error": f"Tải/Giải nén từ URL lỗi: {type(e).__name__}: {e}"}

    # (3) run_*_check(dir)
    name_map = {
        "civitek_new_logic": "run_civitek_new_check",
        "civitek_logic":     "run_civitek_check",
        "flager_logic":      "run_flager_check",
        "mi_logic":          "run_mi_check",
        "md_logic":          "run_md_cu_check",
        "md_new_logic":      "run_md_moi_check",
    }
    check_fn_name = name_map.get(module_name)
    if check_fn_name and hasattr(mod, check_fn_name):
        check_fn = getattr(mod, check_fn_name)
        try:
            m = re.search(r"path\s*=\s*([^\s]+)", command, flags=re.I)
            raw_dir = m.group(1) if m else UPLOAD_DIR
            data_dir = _canonical_data_dir(raw_dir)
            if not os.path.isdir(data_dir):
                parent = os.path.dirname(data_dir)
                if os.path.isdir(parent):
                    data_dir = _canonical_data_dir(parent)
            if not os.path.isdir(data_dir):
                return {"ok": False, "module": module_name, "fn": check_fn_name,
                        "error": (f"Thư mục dữ liệu không tồn tại: '{raw_dir}'. "
                                  f"Hãy bỏ 'path=' để backend tự chọn, hoặc chỉ định đúng thư mục đã giải nén.")}
            out = check_fn(data_dir)
            return {"ok": True, "module": module_name, "fn": check_fn_name,
                    "output": out, "data_dir": data_dir}
        except Exception as e:
            return {"ok": False, "module": module_name, "fn": check_fn_name,
                    "error": f"Lỗi khi gọi {module_name}.{check_fn_name}: {type(e).__name__}: {e}"}

    return {"ok": False, "error": f"Module '{module_name}' không có entry phù hợp (run/main/handle hay run_*_check)."}

def route_command(command: str):
    if not command:
        return {"ok": False, "error": "Empty command"}
    cmd_lower = command.lower()
    if re.search(r"\bhelp\b", cmd_lower):
        return {"ok": True, "help": True, "message": HELP_TEXT}
    for pattern, module_name in TOOL_KEYWORDS:
        if re.search(pattern, cmd_lower, flags=re.IGNORECASE):
            return _call_tool_module(module_name, command)
    return {"ok": False, "error": "Không nhận dạng được tool từ lệnh. Gõ 'help' để xem hướng dẫn."}

# ================== Run & Poll APIs ==================
@app.route("/api/run-tool", methods=["POST"])
def run_tool():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "queued", "updated": datetime.utcnow()}
    _gc_jobs()
    threading.Thread(target=_run_command_background, args=(job_id, command), daemon=True).start()
    return jsonify({"job_id": job_id, "status": "queued"}), 202

@app.route("/api/run-tool-async", methods=["POST"])
def run_tool_async():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "queued", "updated": datetime.utcnow()}
    _gc_jobs()
    threading.Thread(target=_run_command_background, args=(job_id, command), daemon=True).start()
    return jsonify({"job_id": job_id, "status": "queued"}), 202

@app.route("/api/job/<job_id>", methods=["GET"])
def get_job(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job không tồn tại"}), 404
    return jsonify(job)

# ================== S3 presign & analyze (optional) ==================
@app.route("/api/s3/presign", methods=["POST"])
def s3_presign():
    if not s3 or not S3_BUCKET:
        return jsonify({"ok": False, "error": "S3 not configured"}), 400
    data = request.get_json(silent=True) or {}
    filename = data.get("filename", "upload.zip")
    content_type = data.get("contentType", "application/zip")
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
    key = f"uploads/{uuid.uuid4().hex}_{safe_name}"
    presigned = s3.generate_presigned_post(
        Bucket=S3_BUCKET, Key=key,
        Fields={"Content-Type": content_type, "x-amz-server-side-encryption": "AES256", "key": key},
        Conditions=[{"bucket": S3_BUCKET}, {"Content-Type": content_type},
                    ["content-length-range", 0, 500*1024*1024],
                    {"key": key}, {"x-amz-server-side-encryption": "AES256"}],
        ExpiresIn=600
    )
    return jsonify({"url": presigned["url"], "fields": presigned["fields"], "key": key})

@app.route("/api/analyze", methods=["POST"])
def analyze_object():
    if not s3 or not S3_BUCKET:
        return jsonify({"ok": False, "error": "S3 not configured"}), 400
    data = request.get_json(silent=True) or {}
    key = data.get("key")
    if not key:
        return jsonify({"ok": False, "error": "Missing key"}), 400
    buf = io.BytesIO()
    s3.download_fileobj(S3_BUCKET, key, buf)
    buf.seek(0)
    result = analyze_zip_stream(buf.read())
    return jsonify({"ok": True, "key": key, "result": result})

# ============ Smart delete (stream, tiết kiệm RAM) ============
ID_PAT = re.compile(r"\bID\s*:\s*([A-Za-z0-9._\-]+)", re.I)

def _extract_error_ids_from_log(log_text: str):
    ids = set()
    for line in (log_text or "").splitlines():
        m = ID_PAT.search(line)
        if m:
            ids.add(m.group(1).strip())
    return sorted(ids)

def _delete_lines_with_ids_in_file_stream(in_path: str, ids: set[str]) -> dict:
    removed, kept = 0, 0
    tmp_path = in_path + ".tmp"
    try:
        with open(in_path, "r", encoding="utf-8", errors="ignore") as fin, \
             open(tmp_path, "w", encoding="utf-8") as fout:
            for ln in fin:
                ln_low = ln.lower()
                hit = any(ln.startswith(i + "|") or i.lower() in ln_low for i in ids)
                if hit: removed += 1
                else:   kept += 1; fout.write(ln)
        os.replace(tmp_path, in_path)
        return {"file": os.path.basename(in_path), "removed": removed, "kept": kept}
    except Exception as e:
        try:
            if os.path.exists(tmp_path): os.remove(tmp_path)
        except: pass
        return {"file": os.path.basename(in_path), "error": f"{type(e).__name__}: {e}"}

@app.route("/api/delete-error-lines", methods=["POST"])
def delete_error_lines():
    data = request.get_json(silent=True) or {}
    data_dir = data.get("data_dir")
    log_text = data.get("log_text") or ""
    dry_run = bool(data.get("dry_run", False))

    if not data_dir or not os.path.isdir(data_dir):
        return jsonify({"ok": False, "error": "Thiếu hoặc sai 'data_dir'."}), 400
    ids = _extract_error_ids_from_log(log_text)
    if not ids:
        return jsonify({"ok": False, "error": "Không tìm thấy ID trong log. Vui lòng chạy tool trước rồi dùng chức năng này."}), 400

    patterns = ["*_content.txt", "*_CONTENT.TXT", "*_Content.txt"]
    targets = []
    for root, dirs, files in os.walk(data_dir):
        for name in files:
            if any(fnmatch.fnmatch(name, pat) for pat in patterns):
                targets.append(os.path.join(root, name))

    if not targets:
        return jsonify({"ok": False, "error": "Không tìm thấy file *_content.txt trong thư mục dữ liệu."}), 400

    if dry_run:
        return jsonify({"ok": True, "dry_run": True, "ids": ids, "targets": [os.path.basename(p) for p in targets]})

    ids_set = set(ids)
    reports, total_removed, modified_files = [], 0, []
    for p in targets:
        rep = _delete_lines_with_ids_in_file_stream(p, ids_set)
        if rep.get("removed"):
            total_removed += rep["removed"]
            modified_files.append(os.path.basename(p))
        reports.append(rep)

    return jsonify({
        "ok": True,
        "deleted_total": total_removed,
        "ids_count": len(ids_set),
        "modified_files": modified_files,
        "reports": reports
    })

# ================== Download cleaned files ==================
@app.route("/api/download-cleaned-one", methods=["GET"])
def download_cleaned_one():
    data_dir = request.args.get("data_dir", "").strip()
    name = request.args.get("name", "").strip()
    if not data_dir or not os.path.isdir(data_dir):
        return jsonify({"ok": False, "error": "Thiếu hoặc sai 'data_dir'."}), 400
    if not name:
        return jsonify({"ok": False, "error": "Thiếu 'name'."}), 400

    file_path = os.path.join(data_dir, name)
    if not os.path.isfile(file_path):
        cand = None
        low = name.lower()
        for f in os.listdir(data_dir):
            if f.lower() == low:
                cand = os.path.join(data_dir, f)
                name = f
                break
        if not cand:
            return jsonify({"ok": False, "error": f"Không tìm thấy file: {name}"}), 404
        file_path = cand

    return send_file(file_path, as_attachment=True, download_name=name)

@app.route("/api/download-cleaned", methods=["GET"])
def download_cleaned_zip():
    data_dir = request.args.get("data_dir", "").strip()
    names_param = request.args.get("names", "").strip()
    if not data_dir or not os.path.isdir(data_dir):
        return jsonify({"ok": False, "error": "Thiếu hoặc sai 'data_dir'."}), 400

    targets = []
    if names_param:
        wanted = {n.strip().lower() for n in names_param.split(",") if n.strip()}
        for f in os.listdir(data_dir):
            if f.lower() in wanted:
                targets.append(os.path.join(data_dir, f))
    else:
        for f in os.listdir(data_dir):
            if f.lower().endswith("_content.txt"):
                targets.append(os.path.join(data_dir, f))

    if not targets:
        return jsonify({"ok": False, "error": "Không có file để đóng gói."}), 404

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in targets:
            arcname = os.path.basename(p)
            z.write(p, arcname=arcname)
    mem.seek(0)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_name = f"cleaned_{ts}.zip"
    return send_file(mem, as_attachment=True, download_name=zip_name, mimetype="application/zip")

# ================== Serve chatbot.html (same-origin) ==================
@app.get("/chatbot.html")
def chatbot_page():
    return app.send_static_file("chatbot.html")

@app.get("/")
def index():
    return redirect("/chatbot.html")

# ================== Local dev ==================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

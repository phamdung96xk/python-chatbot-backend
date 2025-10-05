# app.py
import os, io, zipfile, tempfile, time, uuid, re, glob, importlib, inspect, threading
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS

# boto3 là tùy chọn (chỉ tạo client nếu có S3_BUCKET)
try:
    import boto3  # type: ignore
except Exception:
    boto3 = None

app = Flask(__name__)

# ===== Config =====
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB
UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ===== CORS =====
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)

# ===== (Optional) S3 presign =====
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
s3 = boto3.client("s3", region_name=AWS_REGION) if (boto3 and S3_BUCKET) else None

# ===== Async job store =====
JOBS = {}  # job_id -> {"status": "queued|running|done|error", "result": ..., "error": ..., "updated": datetime}

def _gc_jobs(hours: int = 6):
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    for k, v in list(JOBS.items()):
        if v.get("updated") and v["updated"] < cutoff:
            del JOBS[k]

def _run_command_background(job_id: str, command: str):
    try:
        JOBS[job_id] = {"status": "running", "updated": datetime.utcnow()}
        res = route_command(command)
        if res.get("ok"):
            if res.get("help"):
                out = {"result": res.get("message")}
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
    except Exception as e:
        JOBS[job_id] = {"status": "error", "error": f"{type(e).__name__}: {e}", "updated": datetime.utcnow()}

# ===== ZIP helpers =====
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
            z.extractall(UPLOAD_DIR)
            names = z.namelist()
        return {"ok": True, "elapsed_sec": round(time.time()-t0, 3),
                "summary": {"entries": len(names), "sample": names[:10]}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

# ===== Download & normalize helpers =====
def _normalize_gdrive(url: str) -> str:
    """Mọi dạng link Drive -> direct download."""
    m = re.search(r"/file/d/([^/]+)", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    m = re.search(r"[?&]id=([^&]+)", url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url

def _download_zip_to_file(url: str, dest_path: str):
    """
    Tải ZIP (Drive/URL thường) xuống file trực tiếp (streaming), tiết kiệm RAM.
    Cuối cùng verify dest_path là ZIP hợp lệ, nếu không -> raise.
    """
    is_drive = "drive.google.com" in url
    if is_drive:
        url = _normalize_gdrive(url)

    with requests.Session() as s:
        r = s.get(url, stream=True, allow_redirects=True, timeout=(30, 300))
        # confirm page của Drive (file lớn/virus scan)
        if is_drive and "text/html" in r.headers.get("Content-Type", "").lower():
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
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)

    if not zipfile.is_zipfile(dest_path):
        raise ValueError("Nội dung tải về không phải file ZIP.")

# ===== Case-insensitive XML detection =====
XML_PATTERNS = ["*.xml", "*.[xX][mM][lL]"]  # .xml, .XML, .Xml, ...

def _has_xml(dirpath: str) -> bool:
    for pat in XML_PATTERNS:
        if glob.glob(os.path.join(dirpath, pat)):
            return True
    return False

def _detect_best_data_dir(root_dir: str) -> str:
    """Chọn thư mục chạy tool:
       - Nếu có .xml ngay trong root_dir -> dùng root_dir
       - Nếu nằm ở thư mục con -> trỏ tới thư mục chứa .xml đầu tiên
    """
    try:
        if _has_xml(root_dir):
            return root_dir
        for pat in XML_PATTERNS:
            deep = glob.glob(os.path.join(root_dir, "**", pat), recursive=True)
            if deep:
                return os.path.dirname(deep[0])
    except Exception:
        pass
    return root_dir

# ===== Health =====
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": time.time()})

# ===== OPTIONS catch-all =====
@app.route("/api/<path:any_path>", methods=["OPTIONS"])
def api_options(any_path):
    resp = make_response("", 204)
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    resp.headers.setdefault("Access-Control-Allow-Headers", "*")
    return resp

# ===== Upload ZIP =====
@app.route("/api/upload-files", methods=["POST", "OPTIONS"])
def upload_files():
    if request.method == "OPTIONS":
        return ("", 204)
    if "file" not in request.files:
        return jsonify({"error": "Thiếu field 'file' trong form-data"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "Tên file rỗng"}), 400
    save_path = os.path.join(UPLOAD_DIR, f.filename)
    f.save(save_path)
    return jsonify({"status": "ok", "filename": f.filename, "saved_to": save_path})

# ===== Demo tóm tắt ZIP từ URL =====
@app.route("/api/analyze-by-url", methods=["POST"])
def analyze_by_url():
    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    try:
        tmp = os.path.join(UPLOAD_DIR, f"tmp_{uuid.uuid4().hex}.zip")
        _download_zip_to_file(url, tmp)
        with open(tmp, "rb") as f:
            result = analyze_zip_stream(f.read())
        return jsonify({"ok": True, "source": url, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500

# ===== Dispatcher =====
TOOL_KEYWORDS = [
    (r"\bcivitek\s+new\b", "civitek_new_logic"),
    (r"\bcivitek\b",       "civitek_logic"),
    (r"\bflager\b",        "flager_logic"),
    (r"\bmi\b",            "mi_logic"),
    (r"\bmd\b",            "md_logic"),
]

HELP_TEXT = (
    "Tool \"civitek_logic.py\" từ khóa \"civitek\" (viết hoa viết thường đều được), "
    "\"flager_logic.py\" từ khóa \"flager\" (viết hoa viết thường đều được), "
    "\"mi_logic.py\" từ khóa \"MI\" (viết hoa viết thường đều được), "
    "\"md_logic.py\" từ khóa \"MD\" (viết hoa viết thường đều được), "
    "\"civitek_new_logic.py\" từ khóa \"civitek new\". "
    "Hướng dẫn sử dụng thì ghi như nội dung tôi vừa gửi bạn đây từ khóa \"help\"."
)

def _call_tool_module(module_name: str, command: str):
    """Quy trình:
       1) run/main/handle (có tham số hoặc không)
       1.5) Có URL (url=... hoặc URL trần): tải ZIP (stream), giải nén, gắn path=... (best dir)
       2) Fallback: run_*_check(directory_path)
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {"ok": False, "error": f"Không import được module '{module_name}': {e}"}

    # B1: run/main/handle
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

    # B1.5: nhận URL (url=... hoặc URL trần) -> tải ZIP xuống file, extract, gắn path=...
    murl = re.search(r"url\s*=\s*([^\s]+)", command, flags=re.I)
    if not murl:
        murl = re.search(r"(https?://\S+)", command, flags=re.I)
    if murl:
        url_in = murl.group(1)
        try:
            tmp_zip = os.path.join(UPLOAD_DIR, f"link_{uuid.uuid4().hex}.zip")
            _download_zip_to_file(url_in, tmp_zip)  # streaming to disk
            extract_dir = tempfile.mkdtemp(prefix="gd_", dir=UPLOAD_DIR)
            with zipfile.ZipFile(tmp_zip, "r") as z:
                z.extractall(extract_dir)
            best_dir = _detect_best_data_dir(extract_dir)
            command = f"{command} path={best_dir}"
        except Exception as e:
            return {"ok": False, "error": f"Tải/Giải nén từ URL lỗi: {type(e).__name__}: {e}"}

    # B2: fallback run_*_check(dir)
    name_map = {
        "civitek_new_logic": "run_civitek_new_check",
        "civitek_logic":     "run_civitek_check",
        "flager_logic":      "run_flager_check",
        "mi_logic":          "run_mi_check",
        "md_logic":          "run_md_check",
    }
    check_fn_name = name_map.get(module_name)
    if check_fn_name and hasattr(mod, check_fn_name):
        check_fn = getattr(mod, check_fn_name)
        try:
            m = re.search(r"path\s*=\s*([^\s]+)", command, flags=re.I)
            raw_dir = m.group(1) if m else UPLOAD_DIR
            data_dir = _detect_best_data_dir(raw_dir)
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

# ===== /api/run-tool — luôn chạy ASYNC cho mọi lệnh =====
@app.route("/api/run-tool", methods=["POST"])
def run_tool():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400

    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "queued", "updated": datetime.utcnow()}
    _gc_jobs()
    t = threading.Thread(target=_run_command_background, args=(job_id, command), daemon=True)
    t.start()
    return jsonify({"job_id": job_id, "status": "queued"}), 202

# ===== /api/run-tool-async (ép nền cho mọi lệnh) =====
@app.route("/api/run-tool-async", methods=["POST"])
def run_tool_async():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400

    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "queued", "updated": datetime.utcnow()}
    _gc_jobs()
    t = threading.Thread(target=_run_command_background, args=(job_id, command), daemon=True)
    t.start()
    return jsonify({"job_id": job_id, "status": "queued"}), 202

# ===== /api/job/<id> =====
@app.route("/api/job/<job_id>", methods=["GET"])
def get_job(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job không tồn tại"}), 404
    return jsonify(job)

# ===== S3 presign & analyze (optional) =====
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

# ===== Local dev =====
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

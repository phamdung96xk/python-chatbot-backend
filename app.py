# app.py
import os, io, zipfile, tempfile, time, uuid, re, glob, shutil, importlib, inspect
import boto3, requests
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS

app = Flask(__name__)

# ===== Config cơ bản =====
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB
UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ===== CORS =====
# Không dùng cookie -> supports_credentials=False; origins="*" cho tiện cross-origin
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)

# ===== S3 client (cho flow presign) =====
S3_BUCKET = os.environ.get("S3_BUCKET")                 # ví dụ: tv-tools-uploads
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
s3 = boto3.client("s3", region_name=AWS_REGION)

# ===== Helpers: phân tích ZIP trong memory / trên đĩa =====
def analyze_zip_stream(body_bytes: bytes) -> dict:
    t0 = time.time()
    try:
        zf = zipfile.ZipFile(io.BytesIO(body_bytes))
        names = zf.namelist()
        summary = {"num_entries_in_zip": len(names), "sample_first_10": names[:10]}
        # (TODO) GỌI TOOL THẬT Ở ĐÂY nếu bạn muốn: giải nén -> gọi checker của bạn
        return {"ok": True, "elapsed_sec": round(time.time()-t0, 3), "summary": summary}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

def analyze_zip_file(zip_path: str) -> dict:
    t0 = time.time()
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(UPLOAD_DIR)  # hoặc temp dir riêng nếu muốn dọn dẹp sau
            names = z.namelist()
        # (TODO) GỌI TOOL THẬT ở đây, ví dụ check folder đã extract
        return {"ok": True, "elapsed_sec": round(time.time()-t0, 3),
                "summary": {"entries": len(names), "sample": names[:10]}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

# ===== Helpers: tải file từ URL / Google Drive =====
def _download_google_drive(url: str) -> bytes:
    """Tải file public từ Google Drive, xử lý trang confirm khi file lớn."""
    with requests.Session() as s:
        r = s.get(url, allow_redirects=True, timeout=60)
        # Nếu là trang confirm (quét virus), tìm link chứa confirm=
        if "confirm=" in r.text and "download" in r.text:
            m = re.search(r'href="(\/uc\?export=download[^"]+confirm=[^"]+)"', r.text)
            if m:
                confirm_url = "https://drive.google.com" + m.group(1).replace("&amp;", "&")
                r = s.get(confirm_url, allow_redirects=True, timeout=60)
        r.raise_for_status()
        return r.content

def _download_generic(url: str) -> bytes:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        return b"".join(chunk for chunk in r.iter_content(1024 * 64) if chunk)

# ===== Helper: chọn thư mục tốt nhất chứa .xml =====
def _detect_best_data_dir(root_dir: str) -> str:
    """Trả về thư mục phù hợp nhất để chạy tool:
       - Nếu có .xml ngay trong root_dir -> dùng root_dir
       - Nếu .xml nằm trong các thư mục con -> trỏ tới thư mục chứa .xml đầu tiên
    """
    try:
        if glob.glob(os.path.join(root_dir, "*.xml")):
            return root_dir
        deep = glob.glob(os.path.join(root_dir, "**", "*.xml"), recursive=True)
        if deep:
            return os.path.dirname(deep[0])
    except Exception:
        pass
    return root_dir

# ===== Health =====
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": time.time()})

# ===== OPTIONS catch-all cho /api/* (đảm bảo preflight không bao giờ fail) =====
@app.route("/api/<path:any_path>", methods=["OPTIONS"])
def api_options(any_path):
    resp = make_response("", 204)
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    resp.headers.setdefault("Access-Control-Allow-Headers", "*")
    return resp

# ===== Upload ZIP trực tiếp tới backend =====
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

    # (Tuỳ chọn) phân tích ngay sau upload:
    # result = analyze_zip_file(save_path)
    # return jsonify({"status": "ok", "filename": f.filename, "saved_to": save_path, "result": result})

    return jsonify({"status": "ok", "filename": f.filename, "saved_to": save_path})

# ===== Phân tích qua URL (Google Drive / URL trực tiếp) =====
@app.route("/api/analyze-by-url", methods=["POST"])
def analyze_by_url():
    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    try:
        if "drive.google.com" in url:
            body = _download_google_drive(url)
        else:
            body = _download_generic(url)
        result = analyze_zip_stream(body)   # hoặc giải nén ra đĩa & gọi tool thật
        return jsonify({"ok": True, "source": url, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500

# ======== DISPATCHER: map keyword -> module ========
# Quy ước entrypoint module: ưu tiên gọi lần lượt: run(cmd) -> main(cmd) -> handle(cmd)
TOOL_KEYWORDS = [
    # (regex pattern (case-insensitive), module_name)
    (r"\bcivitek\s+new\b", "civitek_new_logic"),  # phải đặt 'civitek new' trước 'civitek'
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
    """Gọi module theo 3 bước:
       1) run/main/handle (có tham số hoặc không)
       1.5) Nếu có URL (url=... hoặc dán trần), tải ZIP & giải nén -> gắn path=... vào command
       2) Fallback sang run_*_check(directory_path) hiện có trong module
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {"ok": False, "error": f"Không import được module '{module_name}': {e}"}

    # --- B1: thử run/main/handle ---
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

    # --- B1.5: nếu lệnh có URL (url=... hoặc dán trần) ---
    murl = re.search(r"url\s*=\s*([^\s]+)", command, flags=re.I)
    if not murl:
        murl = re.search(r"(https?://\S+)", command, flags=re.I)  # URL trần
    if murl:
        url_in = murl.group(1)
        try:
            body = _download_google_drive(url_in) if "drive.google.com" in url_in else _download_generic(url_in)
            tmp_zip = os.path.join(UPLOAD_DIR, f"link_{uuid.uuid4().hex}.zip")
            with open(tmp_zip, "wb") as f:
                f.write(body)
            extract_dir = tempfile.mkdtemp(prefix="gd_", dir=UPLOAD_DIR)
            with zipfile.ZipFile(tmp_zip, "r") as z:
                z.extractall(extract_dir)
            best_dir = _detect_best_data_dir(extract_dir)
            command = f"{command} path={best_dir}"
        except Exception as e:
            return {"ok": False, "error": f"Tải/Giải nén từ URL lỗi: {type(e).__name__}: {e}"}

    # --- B2: fallback sang run_*_check(dir) ---
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
            # lấy thư mục dữ liệu từ lệnh: ví dụ "mi path=/tmp/uploads/batch_01"
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
    """Xác định module theo keyword và gọi module tương ứng."""
    if not command:
        return {"ok": False, "error": "Empty command"}

    cmd_lower = command.lower()

    # help
    if re.search(r"\bhelp\b", cmd_lower):
        return {"ok": True, "help": True, "message": HELP_TEXT}

    # civitek new phải match trước civitek thường (đã sắp xếp trên)
    for pattern, module_name in TOOL_KEYWORDS:
        if re.search(pattern, cmd_lower, flags=re.IGNORECASE):
            return _call_tool_module(module_name, command)

    # không match gì
    return {"ok": False, "error": "Không nhận dạng được tool từ lệnh. Gõ 'help' để xem hướng dẫn."}

# ===== Bot (/api/run-tool) dùng dispatcher =====
@app.route("/api/run-tool", methods=["POST"])
def run_tool():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400

    result = route_command(command)
    # Trả gọn cho frontend
    if result.get("ok"):
        if result.get("help"):
            return jsonify({"result": result.get("message")})
        return jsonify({
            "result": result.get("output"),
            "module": result.get("module"),
            "fn": result.get("fn"),
            "data_dir": result.get("data_dir")
        })
    else:
        return jsonify({"result": result.get("error")})

# ===== S3 presign (upload thẳng S3) =====
@app.route("/api/s3/presign", methods=["POST"])
def s3_presign():
    """Cấp pre-signed POST cho trình duyệt upload trực tiếp lên S3."""
    data = request.get_json(silent=True) or {}
    filename = data.get("filename", "upload.zip")
    content_type = data.get("contentType", "application/zip")
    size = int(data.get("size", 0))

    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
    key = f"uploads/{uuid.uuid4().hex}_{safe_name}"

    # Giới hạn 0 .. 500MB
    conditions = [
        {"bucket": S3_BUCKET},
        {"Content-Type": content_type},
        ["content-length-range", 0, 500 * 1024 * 1024],
        {"key": key},
        {"x-amz-server-side-encryption": "AES256"},
    ]
    fields = {
        "Content-Type": content_type,
        "x-amz-server-side-encryption": "AES256",
        "key": key,
    }

    presigned = s3.generate_presigned_post(
        Bucket=S3_BUCKET,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=600,  # 10 phút
    )
    return jsonify({"url": presigned["url"], "fields": presigned["fields"], "key": key})

# ===== Phân tích object trên S3 sau khi upload xong =====
@app.route("/api/analyze", methods=["POST"])
def analyze_object():
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
    app.run("0.0.0.0", 5000, debug=True)

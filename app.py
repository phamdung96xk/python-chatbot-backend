# app.py
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import os
from datetime import datetime

app = Flask(__name__)

# ==== cấu hình cơ bản ====
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100MB
UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ==== CORS (BẢN DEBUG: thoáng để khoanh vùng) ====
# Quan trọng: không dùng cookie => supports_credentials=False, origins="*"
CORS(
    app,
    resources={r"/api/*": {"origins": "*"}},
    supports_credentials=False,
    methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ==== Healthcheck ====
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})

# ==== BẮT OPTIONS CHUNG CHO MỌI /api/* (đảm bảo preflight không bao giờ lỗi) ====
@app.route("/api/<path:any_path>", methods=["OPTIONS"])
def api_options(any_path):
    resp = make_response("", 204)
    # Flask-CORS đã chèn header CORS hộ rồi; tuy vậy ta đặt thêm để chắc ăn:
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    resp.headers.setdefault("Access-Control-Allow-Headers", "*")
    return resp

# ==== Upload files ====
@app.route("/api/upload-files", methods=["POST"])
def upload_files():
    if "file" not in request.files:
        return jsonify({"error": "Thiếu field 'file'"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "Tên file rỗng"}), 400

    save_path = os.path.join(UPLOAD_DIR, f.filename)
    f.save(save_path)
    return jsonify({"status": "ok", "filename": f.filename, "saved_to": save_path})

# ==== Bot command (demo) ====
@app.route("/api/run-tool", methods=["POST"])
def run_tool():
    data = request.get_json(silent=True) or {}
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "Không có lệnh nào được cung cấp"}), 400
    cmd = command.lower()
    if "thời tiết" in cmd:
        resp = "Kết quả từ tool thời tiết: Trời nắng, 32°C."
    elif "phân tích" in cmd:
        resp = "Kết quả từ tool phân tích: Doanh thu tăng 15%."
    else:
        resp = "Tool không được nhận dạng."
    return jsonify({"result": resp})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

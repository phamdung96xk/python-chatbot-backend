# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os

# --- QUAN TRỌNG: VỀ TÊN FILE LOGIC ---
# Python yêu cầu tên file để import phải là các module hợp lệ (không chứa dấu '-', dấu '.' v.v...).
# BẠN BẮT BUỘC PHẢI ĐỔI TÊN các file logic của mình như sau:
# "check_civitek_3.5.py" (logic) -> đổi thành -> "civitek_logic.py"
# "Check_Flager_v3.5.py" (logic) -> đổi thành -> "flager_logic.py"
# "Check_MI_v19(New OK).py" (logic) -> đổi thành -> "mi_logic.py"
# "MD_ALL.py" (logic) -> đổi thành -> "md_logic.py"
# "Civitek_New_v3.9.5.py" (logic) -> đổi thành -> "civitek_new_logic.py"

# --- Import các file logic đã được tách ---
# Server sẽ cố gắng import tất cả các file logic. Nếu file nào chưa có, nó sẽ báo lỗi
# một cách thân thiện nhưng server vẫn sẽ chạy để bạn có thể kiểm thử các tool đã hoàn thành.

try:
    from flager_logic import run_flager_check
except ImportError:
    def run_flager_check(path): return "Lỗi Server: File 'flager_logic.py' chưa được tạo hoặc chưa được đổi tên."

try:
    from civitek_logic import run_civitek_check
except ImportError:
    def run_civitek_check(path): return "Lỗi Server: File 'civitek_logic.py' chưa được tạo hoặc chưa được đổi tên."

try:
    from mi_logic import run_mi_check
except ImportError:
    def run_mi_check(path): return "Lỗi Server: File 'mi_logic.py' chưa được tạo hoặc chưa được đổi tên."

try:
    from md_logic import run_md_check
except ImportError:
    def run_md_check(path): return "Lỗi Server: File 'md_logic.py' chưa được tạo hoặc chưa được đổi tên."

try:
    from civitek_new_logic import run_civitek_new_check
except ImportError:
    def run_civitek_new_check(path): return "Lỗi Server: File 'civitek_new_logic.py' chưa được tạo hoặc chưa được đổi tên."


app = Flask(__name__)
CORS(app)

# Thư mục chứa dữ liệu (file XML, TXT) trên server Render
DATA_DIRECTORY_ON_SERVER = "data_input"

@app.route('/api/run-tool', methods=['POST'])
def run_tool():
    data = request.json
    command_full = data.get('command', '').lower().strip()

    if not command_full:
        return jsonify({'error': 'Không có lệnh nào được cung cấp'}), 400

    response_message = ""

    try:
        # Tách lệnh và thư mục con (nếu có)
        parts = command_full.split()
        command_keyword = parts[0]
        sub_directory = parts[1] if len(parts) > 1 else ""

        # Xử lý trường hợp lệnh có 2 từ như "civitek new"
        if len(parts) > 1 and f"{parts[0]} {parts[1]}" == "civitek new":
             command_keyword = "civitek new"
             sub_directory = parts[2] if len(parts) > 2 else ""

        target_directory = os.path.join(DATA_DIRECTORY_ON_SERVER, sub_directory)

        # Kiểm tra sự tồn tại của thư mục data trước khi chạy lệnh
        if not os.path.isdir(target_directory) and command_keyword != 'help':
             return jsonify({'result': f"Lỗi: Không tìm thấy thư mục '{target_directory}' trên server. Hãy đảm bảo bạn đã tải dữ liệu lên và gõ đúng tên thư mục con (nếu có)."})

        # --- Logic để gọi đúng tool dựa trên từ khóa ---
        if command_keyword == "civitek":
            response_message = run_civitek_check(target_directory)

        elif command_keyword == "flager":
            response_message = run_flager_check(target_directory)

        elif command_keyword == "mi":
            response_message = run_mi_check(target_directory)

        elif command_keyword == "md":
            response_message = run_md_check(target_directory)

        elif command_keyword == "civitek new":
             response_message = run_civitek_new_check(target_directory)

        elif command_keyword == "help":
            response_message = (
                "Các lệnh có sẵn:\n"
                "------------------------------------\n"
                "civitek <thư_mục_con (tùy chọn)>\n"
                "flager <thư_mục_con (tùy chọn)>\n"
                "mi <thư_mục_con (tùy chọn)>\n"
                "md <thư_mục_con (tùy chọn)>\n"
                "civitek new <thư_mục_con (tùy chọn)>\n"
                "------------------------------------\n"
                "Nếu không có thư mục con, tool sẽ chạy trên thư mục data chính."
            )

        else:
            response_message = "Tool không được nhận dạng. Gõ 'help' để xem các lệnh có sẵn."

    except Exception as e:
        response_message = f"Đã có lỗi không xác định xảy ra trong server: {str(e)}"

    return jsonify({'result': response_message})

if __name__ == '__main__':
    if not os.path.exists(DATA_DIRECTORY_ON_SERVER):
        os.makedirs(DATA_DIRECTORY_ON_SERVER)
        print(f"Đã tạo thư mục '{DATA_DIRECTORY_ON_SERVER}'. Hãy đặt dữ liệu của bạn vào đây khi chạy local.")

    app.run(host='0.0.0.0', port=5000)


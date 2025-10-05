import os
import sys
import subprocess
import zipfile
import uuid
import shutil
import time  # Thêm thư viện time
from flask import Flask, request, jsonify
from flask_cors import CORS

# --- Cấu hình cho server ---
# Thư mục này sẽ được tạo trên Render Disk để lưu trữ dữ liệu
DATA_DIRECTORY_ON_SERVER = "/data/data_input"

# --- LOGIC MỚI: Chờ cho thư mục Disk được mount ---
def wait_for_disk_mount(directory_path, timeout=60):
    """
    Chờ cho đến khi thư mục được chỉ định xuất hiện (được mount).
    Hàm sẽ thử kiểm tra mỗi giây cho đến khi hết thời gian chờ.
    """
    start_time = time.time()
    while not os.path.exists(directory_path):
        if time.time() - start_time > timeout:
            print(f"LỖI NGHIÊM TRỌNG: Thư mục disk '{directory_path}' không xuất hiện sau {timeout} giây.")
            # Có thể thêm các hành động khác ở đây, ví dụ: thoát chương trình
            return False
        print(f"Đang chờ thư mục disk '{directory_path}' được mount...")
        time.sleep(1) # Chờ 1 giây rồi thử lại
    
    # Nếu thư mục đã tồn tại nhưng không phải là thư mục, tạo nó
    if not os.path.isdir(directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)
            print(f"Đã tạo thư mục '{directory_path}' vì nó chưa tồn tại.")
        except OSError as e:
            print(f"LỖI: Không thể tạo thư mục '{directory_path}'. Lỗi: {e}")
            return False
            
    print(f"Thành công: Thư mục disk '{directory_path}' đã sẵn sàng.")
    return True

# --- Chạy hàm chờ ngay khi ứng dụng khởi động ---
if not wait_for_disk_mount(DATA_DIRECTORY_ON_SERVER):
    # Nếu sau một thời gian chờ mà thư mục vẫn không có, có thể dừng ứng dụng
    # để Render tự khởi động lại.
    sys.exit("Không thể khởi động: Thư mục disk không sẵn sàng.")

# --- Import các file logic xử lý ---
# ... (Phần import giữ nguyên như cũ) ...
try:
    import civitek_logic
    import flager_logic
    import mi_logic
    import md_logic
    import civitek_new_logic
except ImportError as e:
    print(f"LỖI QUAN TRỌNG: Không thể import file logic. Hãy chắc chắn rằng bạn đã đổi tên file chính xác. Lỗi: {e}")
    sys.exit(1)

# --- Khởi tạo Flask App ---
app = Flask(__name__)
CORS(app)

# --- Các endpoint (/api/upload-files và /api/run-tool) ---
# --- GIỮ NGUYÊN TOÀN BỘ PHẦN CODE CỦA CÁC ENDPOINT NÀY ---
# --- KHÔNG CÓ THAY ĐỔI GÌ Ở ĐÂY ---
@app.route('/api/upload-files', methods=['POST'])
def upload_files():
    if 'file' not in request.files:
        return jsonify({'error': 'Không có file nào được gửi lên.'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Chưa chọn file nào.'}), 400

    if file and file.filename.endswith('.zip'):
        try:
            batch_id = str(uuid.uuid4())[:13]
            batch_dir = os.path.join(DATA_DIRECTORY_ON_SERVER, batch_id)
            os.makedirs(batch_dir, exist_ok=True)

            zip_path = os.path.join(batch_dir, file.filename)
            file.save(zip_path)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(batch_dir)
            
            os.remove(zip_path)

            return jsonify({
                'status': 'success',
                'message': f'Tải lên và giải nén thành công vào thư mục {batch_id}.',
                'batch_id': batch_id
            }), 200

        except Exception as e:
            return jsonify({'error': f'Lỗi server khi xử lý file: {str(e)}'}), 500
    else:
        return jsonify({'error': 'Chỉ chấp nhận file .zip'}), 400

@app.route('/api/run-tool', methods=['POST'])
def run_tool():
    data = request.get_json()
    if not data or 'command' not in data:
        return jsonify({'error': 'Yêu cầu không hợp lệ.'}), 400

    full_command = data['command'].strip()
    command_parts = full_command.split()
    
    tool_name = command_parts[0].lower() if command_parts else ''
    batch_id = command_parts[1] if len(command_parts) > 1 else None

    if tool_name == 'help':
        help_text = """
        --- Hướng dẫn sử dụng các tool ---
        1. civitek <batch_id>
        2. flager <batch_id>
        3. mi <batch_id>
        4. md <batch_id>
        5. civiteknew <batch_id>
        ------------------------------------
        Lưu ý: <batch_id> là mã bạn nhận được sau khi tải file ZIP lên.
        """
        return jsonify({'result': help_text.strip()})
    
    if not batch_id:
        return jsonify({'error': 'Lệnh không hợp lệ. Cần cung cấp Batch ID. Ví dụ: flager abc-123'}), 400
        
    target_directory = os.path.join(DATA_DIRECTORY_ON_SERVER, batch_id)
    if not os.path.isdir(target_directory):
        return jsonify({'error': f'Không tìm thấy dữ liệu cho Batch ID "{batch_id}". Vui lòng kiểm tra lại.'}), 404

    result_message = ""
    try:
        if tool_name == 'civitek':
            result_message = civitek_logic.run_civitek_check(target_directory)
        elif tool_name == 'flager':
            result_message = flager_logic.run_flager_check(target_directory)
        elif tool_name == 'mi':
            result_message = mi_logic.run_mi_check(target_directory)
        elif tool_name == 'md':
            result_message = md_logic.run_md_check(target_directory)
        elif tool_name == 'civiteknew':
            result_message = civitek_new_logic.run_civitek_new_check(target_directory)
        else:
            return jsonify({'error': f'Tool "{tool_name}" không được nhận dạng. Gõ "help" để xem danh sách.'}), 400
        
        return jsonify({'result': result_message})

    except NameError as e:
        print(f"Lỗi NameError: {e}. Có thể file logic chưa được import đúng.")
        return jsonify({'error': f'Lỗi server: Tool "{tool_name}" chưa được cấu hình đúng. Vui lòng kiểm tra lại tên file logic.'}), 500
    except Exception as e:
        print(f"Đã xảy ra lỗi khi chạy tool '{tool_name}' trên thư mục '{target_directory}': {e}")
        return jsonify({'error': f'Đã có lỗi xảy ra khi thực thi tool. Vui lòng kiểm tra logs trên server.'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)


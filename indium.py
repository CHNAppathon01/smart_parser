import logging
import os
import io
import base64
import pandas as pd
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask.views import MethodView
import asyncio
from asgiref.wsgi import WsgiToAsgi
from file_reader import UploadAPI 
from mapper import ColumnMapper # Import function to register routes
from map import ColumnMapperAPI
from error_logger import ClaimFileProcessor,secure_filename,send_file
import logging as logger

# ✅ Setup Logging
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "file_upload.log")

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# ✅ Setup Logger
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__)
CORS(app)  # Enable CORS for all domains
asgi_app = WsgiToAsgi(app)  # Convert Flask app to an ASGI-compatible app

@app.errorhandler(404)
def not_found_error(error):
    if request.path == "/health":
        return "", 204  # ✅ Return 204 No Content for /health requests to suppress logging
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

# ✅ Conditionally exclude the `/health` route
EXCLUDE_HEALTH_ROUTE = True  # Set to True to remove the `/health` route
app.config["UPLOAD_FOLDER"] = 'UPLOAD_FOLDER'
if not os.path.exists('UPLOAD_FOLDER'):
    os.makedirs('UPLOAD_FOLDER')

if not EXCLUDE_HEALTH_ROUTE:
    @app.route('/health', methods=['GET'])
    def health_check():
        return jsonify({"status": "healthy"}), 200
    
def process_file(file_path, previous_record_count=None):
    logger.info(f"Processing file: {file_path}")
    # Process logic here
    return {"message": f"Processed {file_path}", "record_count": previous_record_count}

def process_files_in_folder(folder_path, previous_counts=None):
    if not os.path.exists(folder_path):
        return {"error": "Folder not found"}
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    results = [process_file(f, previous_counts.get(os.path.basename(f))) for f in files]
    return results

class ClaimFileProcessor(MethodView):
    def post(self, action):
        if action == "process-file":
            return self.process_file()
        elif action == "process-folder":
            return self.process_folder()
        elif action == "upload-file":
            return self.upload_file()
        return jsonify({"error": "Invalid action"}), 400

    def process_file(self):
        data = request.json
        file_path = data.get("file_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 400
        result = process_file(file_path)
        return jsonify(result)

    def process_folder(self):
        previous_counts = {"input_file_28.csv": 74, "input_file_14.csv": 70}
        result = process_files_in_folder(app.config["UPLOAD_FOLDER"], previous_counts)
        return jsonify(result)

    def upload_file(self):
        if "vendor_file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        file = request.files["vendor_file"]
        if file.filename == "":
            return jsonify({"error": "No selected file"}), 400
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(file_path)
        previous_counts = {"input_file_28.csv": 74, "input_file_14.csv": 70}
        result = process_file(file_path, previous_counts.get(filename))
        return jsonify(result)

@app.route("/download-log", methods=["GET"])
def download_log():
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, as_attachment=True)
    return jsonify({"error": "Log file not found"}), 404

claim_processor_view = ClaimFileProcessor.as_view("claim_processor")
app.add_url_rule("/api/<string:action>", view_func=claim_processor_view, methods=["POST"])

#  Register routes from external file
app.add_url_rule('/upload/', view_func=UploadAPI.as_view('upload_api'))
app.add_url_rule('/process', view_func=ColumnMapper.as_view('column_mapper'), methods=['POST'])
app.add_url_rule('/map', view_func=ColumnMapperAPI.as_view('ColumnMapperAPI'), methods=['POST'])
app.add_url_rule("/api/<string:action>", view_func=ClaimFileProcessor, methods=["POST"])

if __name__ == "__main__":
    print("\033[92m[INFO] Starting Flask server on port 5000...\033[0m")
    app.run(host="0.0.0.0", port=5000, debug=True)
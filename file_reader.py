import logging
import os
import json
import io
import base64
import pandas as pd
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask.views import MethodView
import asyncio
from asgiref.wsgi import WsgiToAsgi

# ✅ Setup Logging
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "file_upload.log")

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# app = Flask(__name__)
# CORS(app)  # Enable CORS for all domains
# asgi_app = WsgiToAsgi(app)  # Convert Flask app to an ASGI-compatible app

class FileProcessor:
    @staticmethod
    async def process_file(file):
        filename = file.filename
        file_extension = filename.split(".")[-1].lower()
        logging.info(f"Received file: {filename} (Type: {file_extension})")

        try:
            file_content = await asyncio.to_thread(file.read)

            if file_extension == "json":
                try:
                    json_data = json.loads(file_content)
                    logging.info(f"Processing JSON file: {filename}")
                    return {"filename": filename, "content": json_data}
                except json.JSONDecodeError as e:
                    logging.error(f"Corrupt JSON file: {filename} - {str(e)}")
                    return {"error": "Invalid JSON file, unable to decode JSON"}
                except Exception as e:
                    logging.error(f"Unexpected error in JSON file: {filename} - {str(e)}")
                    return {"error": f"Invalid JSON file: {str(e)}"}

            elif file_extension in ["csv", "xls", "xlsx"]:
                try:
                    if file_extension == "csv":
                        df = pd.read_csv(io.BytesIO(file_content), encoding_errors='replace')
                    else:
                        df = pd.read_excel(io.BytesIO(file_content))

                    if df.empty:
                        raise ValueError("Empty data frame")
                    logging.info(f"Processing {file_extension.upper()} file: {filename} (Columns: {list(df.columns)})")
                    return {"filename": filename, "columns": list(df.columns)}
                except pd.errors.EmptyDataError:
                    logging.error(f"Empty CSV/Excel file: {filename}")
                    return {"error": "Uploaded CSV/Excel file is empty"}
                except pd.errors.ParserError:
                    logging.error(f"Corrupt CSV/Excel file: {filename}")
                    return {"error": "Uploaded CSV/Excel file is corrupt or incorrectly formatted"}
                except UnicodeDecodeError as e:
                    logging.error(f"Encoding error in CSV/Excel file: {filename} - {str(e)}")
                    return {"error": "Uploaded file has encoding issues, unable to decode content"}
                except Exception as e:
                    logging.error(f"Error processing CSV/Excel file: {filename} - {str(e)}")
                    return {"error": f"Invalid CSV/Excel file: {str(e)}"}

            else:
                logging.warning(f"Unsupported file type: {filename}")
                return {"error": "Unsupported file type"}

        except Exception as e:
            logging.error(f"Unexpected error processing file: {filename} - {str(e)}")
            return {"error": f"File processing failed: {str(e)}"}

class UploadAPI(MethodView):
    def post(self):
        try:
            if "file" not in request.files:
                logging.warning("No file found in request")
                return jsonify({"error": "Missing file in request"}), 400

            file = request.files["file"]
            result = asyncio.run(FileProcessor.process_file(file))
            return jsonify(result)

        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            return jsonify({"error": f"File processing failed: {str(e)}"}), 500

class HealthCheckAPI(MethodView):
    def get(self):
        return jsonify({"status": "API is working fine"}), 200

# Register Routes
# app.add_url_rule('/upload/', view_func=UploadAPI.as_view('upload_api'))
# app.add_url_rule('/check', view_func=HealthCheckAPI.as_view('health_check_api'))

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=True)

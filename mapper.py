

import pandas as pd
import os
import json
import google.generativeai as genai
from flask import Flask, request, jsonify
from flask.views import MethodView
import io
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Initialize the Gemini AI model
def get_gen_ai_model():
    """Initialize and return the Gemini AI model."""
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])  # Use environment variable for security
    return genai.GenerativeModel("gemini-2.0-flash-exp")


# Function to read files from Flask request
def read_file(file_storage):
    """Reads a CSV or Excel file from Flask FileStorage object."""
    if not file_storage:
        raise ValueError("No file provided")

    file_extension = os.path.splitext(file_storage.filename)[1].lower()
    
    if file_extension == ".csv":
        return pd.read_csv(io.StringIO(file_storage.read().decode('utf-8', errors='ignore')), dtype=str)
    elif file_extension in [".xls", ".xlsx"]:
        return pd.read_excel(file_storage, dtype=str)
    else:
        raise ValueError("Unsupported file type. Please provide a CSV or Excel file.")

# Function to extract column names
def extract_columns(df):
    """Extracts column names from a DataFrame, using 'Field Name' column if available."""
    if 'Field Name' in df.columns:
        return df['Field Name'].dropna().tolist()
    else:
        column_names = df.iloc[0].dropna().tolist()
        df = df[1:].reset_index(drop=True)
        df.columns = column_names
        return column_names, df

def map_fields(standard_columns, vendor_columns):
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])

    # Create the model
    generation_config = {
        "temperature": 1,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 8192,
        "response_mime_type": "application/json",
    }

    model = genai.GenerativeModel(
        model_name="gemini-2.0-flash-exp",
        generation_config=generation_config,
        system_instruction=f"""
        <Forget all previous instructions>
        Customer-to-Vendor Field Mapping
        You are an intelligent data transformation assistant. Your task is to map fields from a customer's standard format to a vendor's required input format based on the provided mappings.

        Instructions:
        Input: You will receive two sets of data:
        - A customer's standard format containing column names.
        - A vendor's required input format containing column names.
        -Understand the domain knowldge then give first compare with all data 

        Processing:
        - Identify corresponding fields between the two formats based on similarity and semantics.
        - Generate a structured mapping where each customer column is matched to the most relevant vendor column.
        - If a direct match does not exist, suggest the closest possible field name based on meaning.

        Output Format:
        - Return a JSON object where each key represents a customer field and the value represents the corresponding vendor field.
        - If no match is found, return "UNMAPPED" as the value.

        Example Input:
        Customer Standard Format:
        {standard_columns}
        Vendor Input Format:
        {vendor_columns}

        Example Output:
        {{
          "PRESLSTNME": "Patient_Last_Name",
          "PRESTFNME": "Patient_First_Name",
          "DOB": "Date_Of_Birth",
          "GENDER": "Sex",
          "ADDR": "Address"
        }}

        Constraints:
        - The mapping should be accurate and context-aware.
        - If multiple possible matches exist, return the most relevant one.
        - Ensure consistency across different mappings.

        Your goal is to automate this mapping process efficiently. Now, generate the required field mapping in JSON format.
        """,
    )

    chat_session = model.start_chat(history=[])
    response = chat_session.send_message("Provide the JSON mapping for the given fields.")
    
    return response.text

# Flask View Class
class ColumnMapper(MethodView):
    def post(self):
        """Handles file upload, processes column mapping using Gemini AI, and returns JSON response."""
        if "standard_file" not in request.files or "vendor_file" not in request.files:
            return jsonify({"error": "Both standard_file and vendor_file are required"}), 400

        standard_file = request.files["standard_file"]
        vendor_file = request.files["vendor_file"]

        try:
            # Read files
            standard_df = read_file(standard_file)
            vendor_df = read_file(vendor_file)

            # Extract column names
            standard_columns, standard_df = extract_columns(standard_df)
            vendor_columns, vendor_df = extract_columns(vendor_df)

            # Get column mappings from Gemini AI
            mapped_columns = map_fields(standard_columns, vendor_columns)

            # Filter and rename columns
            filtered_vendor_df = vendor_df[list(mapped_columns.values())]
            filtered_vendor_df.rename(columns={v: k for k, v in mapped_columns.items() if v != "UNMAPPED"}, inplace=True)

            # Convert to JSON response
            return jsonify({"message": "File processed successfully", "data": filtered_vendor_df.to_dict(orient='records')}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Register the route
app.add_url_rule('/process', view_func=ColumnMapper.as_view('column_mapper'), methods=['POST'])

if __name__ == '__main__':
    app.run(debug=True)

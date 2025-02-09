#!/usr/bin/env python3
import os
import csv
import re
import logging
from datetime import datetime

# ==============================
# Setup Logging (to file and console)
# ==============================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicate messages.
if logger.hasHandlers():
    logger.handlers.clear()

# File handler
fh = logging.FileHandler("smart_parser.log")
fh.setLevel(logging.INFO)
fh_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(levelname)s - %(message)s")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

# Prevent propagation so that messages are not duplicated.
logger.propagate = False

# ==============================
# Schema Definitions
# ==============================
HEADER_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "HDR", "max_length": 3, "type": "text"},
    {"name": "Creation Date", "required": True, "format": "date", "max_length": 10, "type": "date"}
]

ALLOWED_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

CLAIM_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "CLM", "max_length": 3, "type": "text"},
    {"name": "RecordNumber", "required": True, "type": "integer", "unique": True},
    {"name": "ClaimID", "required": True, "type": "text", "pattern": r"^[A-Z]{2}-\d{4}-\d{1,8}$", "max_length": 50},
    {"name": "OriginalClaim", "required": False, "type": "text", "max_length": 50},
    {"name": "Group_ID", "required": True, "type": "text", "max_length": 50},
    {"name": "Payer Contact Name", "required": False, "type": "text", "max_length": 10},
    {"name": "Payer Contact Phone", "required": False, "type": "text", "pattern": r"^\d{3}-\d{3}-\d{4}$", "max_length": 12},
    {"name": "Employee_ID", "required": False, "type": "text", "max_length": 50},
    {"name": "SSN", "required": True, "type": "text", "pattern": r"^\d{3}-\d{2}-\d{4}$", "max_length": 11},
    {"name": "Patient_ID", "required": True, "type": "text", "max_length": 30},
    {"name": "Patient_Last_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Patient_First_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Address1", "required": False, "type": "text", "max_length": 35},
    {"name": "Address2", "required": False, "type": "text", "max_length": 35},
    {"name": "City", "required": False, "type": "text", "max_length": 30},
    {"name": "State", "required": False, "type": "text", "max_length": 2, "allowed": ALLOWED_STATES},
    {"name": "ZipCode", "required": False, "type": "integer"},
    {"name": "Relationship", "required": True, "type": "integer", "allowed": [1, 2, 3]},
    {"name": "DoB", "required": True, "type": "date", "max_length": 10},
    {"name": "Prescriber_Name", "required": False, "type": "text", "max_length": 50},
    {"name": "Pharmacy_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Pharmacy_Type", "required": False, "type": "text", "max_length": 1, "allowed": ["R", "M", "H", "C"]},
    {"name": "Date_of_Service", "required": True, "type": "date", "max_length": 10},
    {"name": "Prescription_No", "required": False, "type": "text", "max_length": 50},
    {"name": "Prescription_Filled_Date", "required": True, "type": "date", "max_length": 10},
    {"name": "In_Out_Network", "required": False, "type": "text", "max_length": 1, "allowed": ["I", "O"]},
    {"name": "National Drug Code", "required": True, "type": "text", "max_length": 50},
    {"name": "Label_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Brand_Generic", "required": True, "type": "text", "max_length": 1, "allowed": ["B", "G"]},
    {"name": "Drug_Strength", "required": True, "type": "text", "max_length": 20},
    {"name": "Days_Supply", "required": True, "type": "integer"},
    {"name": "Quantity", "required": True, "type": "integer"},
    {"name": "Dosage Form", "required": False, "type": "text", "max_length": 20},
    {"name": "Formulary", "required": True, "type": "text", "max_length": 1, "allowed": ["Y", "N"]},
    {"name": "Total_Billed_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Plan_Paid_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Copay_Coins", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Deductible", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Other_Cost", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Total_Paid_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Paid_Date", "required": True, "type": "date", "max_length": 10},
    {"name": "Status", "required": False, "type": "text", "max_length": 1, "allowed": ["A", "D", "R"]}
]

TRAILER_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "TRL", "max_length": 3, "type": "text"},
    {"name": "Record Count", "required": True, "type": "integer"}
]

EXPECTED_HEADER_FIELDS = len(HEADER_SCHEMA)
EXPECTED_CLAIM_FIELDS = 42
EXPECTED_TRAILER_FIELDS = len(TRAILER_SCHEMA)

# ==============================
# Validation Functions
# ==============================
def is_valid_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True, None
    except ValueError:
        return False, f"Invalid date format (expected YYYY-MM-DD): {value}"

def is_valid_integer(value):
    try:
        int(value)
        return True, None
    except ValueError:
        return False, f"Not a valid integer: {value}"

def is_valid_decimal(value):
    try:
        float(value)
        return True, None
    except ValueError:
        return False, f"Not a valid decimal: {value}"

def check_length(value, max_length):
    if len(value) > max_length:
        return False, f"Length {len(value)} exceeds max length {max_length}"
    return True, None

def check_pattern(value, pattern):
    if not re.match(pattern, value):
        return False, f"Value '{value}' does not match pattern: {pattern}"
    return True, None

def check_allowed(value, allowed_list):
    if value not in allowed_list:
        return False, f"Value '{value}' is not in allowed list: {allowed_list}"
    return True, None

def validate_field(field_schema, value):
    errors = []
    if field_schema.get("required", False) and (value is None or value == ""):
        errors.append(f"Field '{field_schema['name']}' is required but missing.")
        return errors

    if value == "":
        return errors

    if "expected" in field_schema:
        if value != field_schema["expected"]:
            errors.append(f"Expected '{field_schema['expected']}' but got '{value}'.")

    if "max_length" in field_schema:
        valid, err = check_length(value, field_schema["max_length"])
        if not valid:
            errors.append(err)

    ftype = field_schema.get("type")
    converted_value = value
    if ftype == "integer":
        valid, err = is_valid_integer(value)
        if not valid:
            errors.append(err)
        else:
            try:
                converted_value = int(value)
            except Exception as e:
                errors.append(f"Conversion to integer failed: {e}")
    elif ftype == "decimal":
        valid, err = is_valid_decimal(value)
        if not valid:
            errors.append(err)
        else:
            try:
                converted_value = float(value)
            except Exception as e:
                errors.append(f"Conversion to decimal failed: {e}")

    if "allowed" in field_schema:
        allowed_list = field_schema["allowed"]
        if ftype in ["integer", "decimal"]:
            if converted_value not in allowed_list:
                errors.append(f"Value '{value}' is not in allowed list: {allowed_list}")
        else:
            valid, err = check_allowed(value, allowed_list)
            if not valid:
                errors.append(err)

    if "pattern" in field_schema and ftype not in ["integer", "decimal"]:
        valid, err = check_pattern(value, field_schema["pattern"])
        if not valid:
            errors.append(err)

    if ftype == "date":
        valid, err = is_valid_date(value)
        if not valid:
            errors.append(err)

    if ftype in ["integer", "decimal"] and "min" in field_schema:
        if converted_value < field_schema["min"]:
            errors.append(f"Value {value} is less than minimum {field_schema['min']}.")

    return errors

def validate_row(row, schema, row_number):
    errors = []
    if len(row) != len(schema):
        errors.append(f"Row {row_number}: Expected {len(schema)} columns, found {len(row)} columns.")
        return errors
    for idx, field_schema in enumerate(schema):
        field_value = row[idx].strip()
        field_errors = validate_field(field_schema, field_value)
        if field_errors:
            for err in field_errors:
                errors.append(f"Row {row_number}, Column '{field_schema['name']}': {err}")
    return errors

# ==============================
# File Processing Functions
# ==============================
def process_file(file_path, previous_record_count=None):
    logger.info(f"Processing file: {file_path}")
    print(f"\nProcessing file: {file_path}")

    if not os.path.exists(file_path):
        err_msg = f"File not found: {file_path}"
        logger.error(err_msg)
        print(err_msg)
        return

    file_errors = set()
    unique_record_numbers = set()
    claim_count = 0

    try:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)
    except Exception as e:
        err_msg = f"Error reading file {file_path}: {str(e)}"
        logger.error(err_msg)
        print(err_msg)
        return

    if not rows:
        err_msg = f"File {file_path} is empty."
        logger.error(err_msg)
        print(err_msg)
        return

    # Detect header and trailer rows.
    has_header = (rows[0][0].strip() == "HDR") if rows[0] else False
    has_trailer = (rows[-1][0].strip() == "TRL") if rows[-1] else False

    if has_header:
        header_row = rows[0]
        header_errors = validate_row(header_row, HEADER_SCHEMA, row_number=1)
        if header_errors:
            file_errors.update(header_errors)
            logger.error(f"Header errors in {file_path}: {header_errors}")
            print("Header errors:")
            for err in header_errors:
                print(err)
    else:
        warning_msg = f"File {file_path} does not have a header row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print("Warning: No header row found. Treating all rows as claim records.")

    if has_trailer:
        trailer_row = rows[-1]
        trailer_errors = validate_row(trailer_row, TRAILER_SCHEMA, row_number=len(rows))
        if trailer_errors:
            file_errors.update(trailer_errors)
            logger.error(f"Trailer errors in {file_path}: {trailer_errors}")
            print("Trailer errors:")
            for err in trailer_errors:
                print(err)
    else:
        warning_msg = f"File {file_path} does not have a trailer row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print("Warning: No trailer row found. Treating all rows as claim records.")

    # Determine claim rows.
    start_idx = 1 if has_header else 0
    end_idx = -1 if has_trailer else len(rows)
    claim_rows = rows[start_idx:end_idx]

    for idx, row in enumerate(claim_rows, start=start_idx+1):
        if len(row) != EXPECTED_CLAIM_FIELDS:
            err = f"Row {idx}: Expected {EXPECTED_CLAIM_FIELDS} columns, found {len(row)}."
            file_errors.add(err)
            print(err)
            continue
        errors = validate_row(row, CLAIM_SCHEMA, row_number=idx)
        for err in errors:
            file_errors.add(err)
            print(err)
        record_number = row[1].strip()
        if record_number in unique_record_numbers:
            dup_err = f"Row {idx}: Duplicate RecordNumber {record_number}."
            file_errors.add(dup_err)
            print(dup_err)
        else:
            unique_record_numbers.add(record_number)
        claim_count += 1

    if has_trailer:
        try:
            expected_count = int(trailer_row[1].strip())
            if expected_count != claim_count:
                err = f"Trailer count {expected_count} does not match actual claim count {claim_count}."
                file_errors.add(err)
                print(err)
        except Exception as e:
            err = f"Error parsing trailer Record Count: {e}"
            file_errors.add(err)
            print(err)

    if previous_record_count is not None:
        if previous_record_count > 0 and abs(claim_count - previous_record_count) / previous_record_count > 0.5:
            warn_msg = f"Warning: Claim count {claim_count} differs by more than 50% from previous count {previous_record_count}."
            logger.warning(warn_msg)
            print(warn_msg)

    error_count = len(file_errors)
    if error_count > 0:
        sorted_errors = sorted(file_errors)
        logger.error(f"Errors found in file {file_path}:")
        for err in sorted_errors:
            logger.error(err)
        summary_msg = f"Finished processing file {file_path} with {error_count} error(s) and {claim_count} claim record(s)."
        logger.error(summary_msg)
        print(f"\n{summary_msg}")
        print("Error details:")
        for err in sorted_errors:
            print(f" - {err}")
    else:
        summary_msg = f"File {file_path} processed successfully with {claim_count} claim record(s) and no errors."
        logger.info(summary_msg)
        print(summary_msg)
        logger.info("Total errors: 0")
        print("Total errors: 0")

def process_uploaded_file(file_obj, previous_record_count=None):
    """
    This function is similar to process_file() but accepts a file-like object.
    It can be used when a single file is uploaded (e.g., via a web form).
    """
    # Ensure we start reading from the beginning.
    file_obj.seek(0)
    print("\nProcessing uploaded file...")
    try:
        reader = csv.reader(file_obj)
        rows = list(reader)
    except Exception as e:
        err_msg = f"Error reading uploaded file: {str(e)}"
        logger.error(err_msg)
        print(err_msg)
        return

    if not rows:
        err_msg = "Uploaded file is empty."
        logger.error(err_msg)
        print(err_msg)
        return

    # Use the same logic as process_file (note: we do not have a file path here)
    file_errors = set()
    unique_record_numbers = set()
    claim_count = 0

    has_header = (rows[0][0].strip() == "HDR") if rows[0] else False
    has_trailer = (rows[-1][0].strip() == "TRL") if rows[-1] else False

    if has_header:
        header_row = rows[0]
        header_errors = validate_row(header_row, HEADER_SCHEMA, row_number=1)
        if header_errors:
            file_errors.update(header_errors)
            logger.error(f"Header errors in uploaded file: {header_errors}")
            print("Header errors:")
            for err in header_errors:
                print(err)
    else:
        warning_msg = "Uploaded file does not have a header row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print(warning_msg)

    if has_trailer:
        trailer_row = rows[-1]
        trailer_errors = validate_row(trailer_row, TRAILER_SCHEMA, row_number=len(rows))
        if trailer_errors:
            file_errors.update(trailer_errors)
            logger.error(f"Trailer errors in uploaded file: {trailer_errors}")
            print("Trailer errors:")
            for err in trailer_errors:
                print(err)
    else:
        warning_msg = "Uploaded file does not have a trailer row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print(warning_msg)

    start_idx = 1 if has_header else 0
    end_idx = -1 if has_trailer else len(rows)
    claim_rows = rows[start_idx:end_idx]

    for idx, row in enumerate(claim_rows, start=start_idx+1):
        if len(row) != EXPECTED_CLAIM_FIELDS:
            err = f"Row {idx}: Expected {EXPECTED_CLAIM_FIELDS} columns, found {len(row)}."
            file_errors.add(err)
            print(err)
            continue
        errors = validate_row(row, CLAIM_SCHEMA, row_number=idx)
        for err in errors:
            file_errors.add(err)
            print(err)
        record_number = row[1].strip()
        if record_number in unique_record_numbers:
            dup_err = f"Row {idx}: Duplicate RecordNumber {record_number}."
            file_errors.add(dup_err)
            print(dup_err)
        else:
            unique_record_numbers.add(record_number)
        claim_count += 1

    if has_trailer:
        try:
            expected_count = int(trailer_row[1].strip())
            if expected_count != claim_count:
                err = f"Trailer count {expected_count} does not match actual claim count {claim_count}."
                file_errors.add(err)
                print(err)
        except Exception as e:
            err = f"Error parsing trailer Record Count: {e}"
            file_errors.add(err)
            print(err)

    if previous_record_count is not None:
        if previous_record_count > 0 and abs(claim_count - previous_record_count) / previous_record_count > 0.5:
            warn_msg = f"Warning: Claim count {claim_count} differs by more than 50% from previous count {previous_record_count}."
            logger.warning(warn_msg)
            print(warn_msg)

    error_count = len(file_errors)
    if error_count > 0:
        sorted_errors = sorted(file_errors)
        logger.error("Errors found in uploaded file:")
        for err in sorted_errors:
            logger.error(err)
        summary_msg = f"Finished processing uploaded file with {error_count} error(s) and {claim_count} claim record(s)."
        logger.error(summary_msg)
        print(f"\n{summary_msg}")
        print("Error details:")
        for err in sorted_errors:
            print(f" - {err}")
    else:
        summary_msg = f"Uploaded file processed successfully with {claim_count} claim record(s) and no errors."
        logger.info(summary_msg)
        print(summary_msg)
        logger.info("Total errors: 0")
        print("Total errors: 0")

def process_files_in_folder(folder_path, previous_counts=None):
    if not os.path.exists(folder_path):
        err_msg = f"Folder not found: {folder_path}"
        logger.error(err_msg)
        print(err_msg)
        return

    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
             if os.path.isfile(os.path.join(folder_path, f))]

    if not files:
        msg = f"No files found in folder: {folder_path}"
        logger.info(msg)
        print(msg)
        return

    for file_path in files:
        base_name = os.path.basename(file_path)
        prev_count = previous_counts.get(base_name) if previous_counts else None
        process_file(file_path, previous_record_count=prev_count)

# ==============================
# Main Processing Block
# ==============================
#!/usr/bin/env python3
import os
import csv
import re
import logging
from datetime import datetime

# ==============================
# Setup Logging (to file and console)
# ==============================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicate messages.
if logger.hasHandlers():
    logger.handlers.clear()

# File handler
fh = logging.FileHandler("smart_parser.log")
fh.setLevel(logging.INFO)
fh_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(levelname)s - %(message)s")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

# Prevent propagation so that messages are not duplicated.
logger.propagate = False

# ==============================
# Schema Definitions
# ==============================
HEADER_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "HDR", "max_length": 3, "type": "text"},
    {"name": "Creation Date", "required": True, "format": "date", "max_length": 10, "type": "date"}
]

ALLOWED_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

CLAIM_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "CLM", "max_length": 3, "type": "text"},
    {"name": "RecordNumber", "required": True, "type": "integer", "unique": True},
    {"name": "ClaimID", "required": True, "type": "text", "pattern": r"^[A-Z]{2}-\d{4}-\d{1,8}$", "max_length": 50},
    {"name": "OriginalClaim", "required": False, "type": "text", "max_length": 50},
    {"name": "Group_ID", "required": True, "type": "text", "max_length": 50},
    {"name": "Payer Contact Name", "required": False, "type": "text", "max_length": 10},
    {"name": "Payer Contact Phone", "required": False, "type": "text", "pattern": r"^\d{3}-\d{3}-\d{4}$", "max_length": 12},
    {"name": "Employee_ID", "required": False, "type": "text", "max_length": 50},
    {"name": "SSN", "required": True, "type": "text", "pattern": r"^\d{3}-\d{2}-\d{4}$", "max_length": 11},
    {"name": "Patient_ID", "required": True, "type": "text", "max_length": 30},
    {"name": "Patient_Last_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Patient_First_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Address1", "required": False, "type": "text", "max_length": 35},
    {"name": "Address2", "required": False, "type": "text", "max_length": 35},
    {"name": "City", "required": False, "type": "text", "max_length": 30},
    {"name": "State", "required": False, "type": "text", "max_length": 2, "allowed": ALLOWED_STATES},
    {"name": "ZipCode", "required": False, "type": "integer"},
    {"name": "Relationship", "required": True, "type": "integer", "allowed": [1, 2, 3]},
    {"name": "DoB", "required": True, "type": "date", "max_length": 10},
    {"name": "Prescriber_Name", "required": False, "type": "text", "max_length": 50},
    {"name": "Pharmacy_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Pharmacy_Type", "required": False, "type": "text", "max_length": 1, "allowed": ["R", "M", "H", "C"]},
    {"name": "Date_of_Service", "required": True, "type": "date", "max_length": 10},
    {"name": "Prescription_No", "required": False, "type": "text", "max_length": 50},
    {"name": "Prescription_Filled_Date", "required": True, "type": "date", "max_length": 10},
    {"name": "In_Out_Network", "required": False, "type": "text", "max_length": 1, "allowed": ["I", "O"]},
    {"name": "National Drug Code", "required": True, "type": "text", "max_length": 50},
    {"name": "Label_Name", "required": True, "type": "text", "max_length": 50},
    {"name": "Brand_Generic", "required": True, "type": "text", "max_length": 1, "allowed": ["B", "G"]},
    {"name": "Drug_Strength", "required": True, "type": "text", "max_length": 20},
    {"name": "Days_Supply", "required": True, "type": "integer"},
    {"name": "Quantity", "required": True, "type": "integer"},
    {"name": "Dosage Form", "required": False, "type": "text", "max_length": 20},
    {"name": "Formulary", "required": True, "type": "text", "max_length": 1, "allowed": ["Y", "N"]},
    {"name": "Total_Billed_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Plan_Paid_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Copay_Coins", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Deductible", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Other_Cost", "required": True, "type": "decimal", "min": 0},
    {"name": "Member_Total_Paid_Amount", "required": True, "type": "decimal", "min": 0},
    {"name": "Paid_Date", "required": True, "type": "date", "max_length": 10},
    {"name": "Status", "required": False, "type": "text", "max_length": 1, "allowed": ["A", "D", "R"]}
]

TRAILER_SCHEMA = [
    {"name": "RecordID", "required": True, "expected": "TRL", "max_length": 3, "type": "text"},
    {"name": "Record Count", "required": True, "type": "integer"}
]

EXPECTED_HEADER_FIELDS = len(HEADER_SCHEMA)
EXPECTED_CLAIM_FIELDS = 42
EXPECTED_TRAILER_FIELDS = len(TRAILER_SCHEMA)

# ==============================
# Validation Functions
# ==============================
def is_valid_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True, None
    except ValueError:
        return False, f"Invalid date format (expected YYYY-MM-DD): {value}"

def is_valid_integer(value):
    try:
        int(value)
        return True, None
    except ValueError:
        return False, f"Not a valid integer: {value}"

def is_valid_decimal(value):
    try:
        float(value)
        return True, None
    except ValueError:
        return False, f"Not a valid decimal: {value}"

def check_length(value, max_length):
    if len(value) > max_length:
        return False, f"Length {len(value)} exceeds max length {max_length}"
    return True, None

def check_pattern(value, pattern):
    if not re.match(pattern, value):
        return False, f"Value '{value}' does not match pattern: {pattern}"
    return True, None

def check_allowed(value, allowed_list):
    if value not in allowed_list:
        return False, f"Value '{value}' is not in allowed list: {allowed_list}"
    return True, None

def validate_field(field_schema, value):
    errors = []
    if field_schema.get("required", False) and (value is None or value == ""):
        errors.append(f"Field '{field_schema['name']}' is required but missing.")
        return errors

    if value == "":
        return errors

    if "expected" in field_schema:
        if value != field_schema["expected"]:
            errors.append(f"Expected '{field_schema['expected']}' but got '{value}'.")

    if "max_length" in field_schema:
        valid, err = check_length(value, field_schema["max_length"])
        if not valid:
            errors.append(err)

    ftype = field_schema.get("type")
    converted_value = value
    if ftype == "integer":
        valid, err = is_valid_integer(value)
        if not valid:
            errors.append(err)
        else:
            try:
                converted_value = int(value)
            except Exception as e:
                errors.append(f"Conversion to integer failed: {e}")
    elif ftype == "decimal":
        valid, err = is_valid_decimal(value)
        if not valid:
            errors.append(err)
        else:
            try:
                converted_value = float(value)
            except Exception as e:
                errors.append(f"Conversion to decimal failed: {e}")

    if "allowed" in field_schema:
        allowed_list = field_schema["allowed"]
        if ftype in ["integer", "decimal"]:
            if converted_value not in allowed_list:
                errors.append(f"Value '{value}' is not in allowed list: {allowed_list}")
        else:
            valid, err = check_allowed(value, allowed_list)
            if not valid:
                errors.append(err)

    if "pattern" in field_schema and ftype not in ["integer", "decimal"]:
        valid, err = check_pattern(value, field_schema["pattern"])
        if not valid:
            errors.append(err)

    if ftype == "date":
        valid, err = is_valid_date(value)
        if not valid:
            errors.append(err)

    if ftype in ["integer", "decimal"] and "min" in field_schema:
        if converted_value < field_schema["min"]:
            errors.append(f"Value {value} is less than minimum {field_schema['min']}.")

    return errors

def validate_row(row, schema, row_number):
    errors = []
    if len(row) != len(schema):
        errors.append(f"Row {row_number}: Expected {len(schema)} columns, found {len(row)} columns.")
        return errors
    for idx, field_schema in enumerate(schema):
        field_value = row[idx].strip()
        field_errors = validate_field(field_schema, field_value)
        if field_errors:
            for err in field_errors:
                errors.append(f"Row {row_number}, Column '{field_schema['name']}': {err}")
    return errors

# ==============================
# File Processing Functions
# ==============================
def process_file(file_path, previous_record_count=None):
    logger.info(f"Processing file: {file_path}")
    print(f"\nProcessing file: {file_path}")

    if not os.path.exists(file_path):
        err_msg = f"File not found: {file_path}"
        logger.error(err_msg)
        print(err_msg)
        return

    file_errors = set()
    unique_record_numbers = set()
    claim_count = 0

    try:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)
    except Exception as e:
        err_msg = f"Error reading file {file_path}: {str(e)}"
        logger.error(err_msg)
        print(err_msg)
        return

    if not rows:
        err_msg = f"File {file_path} is empty."
        logger.error(err_msg)
        print(err_msg)
        return

    # Detect header and trailer rows.
    has_header = (rows[0][0].strip() == "HDR") if rows[0] else False
    has_trailer = (rows[-1][0].strip() == "TRL") if rows[-1] else False

    if has_header:
        header_row = rows[0]
        header_errors = validate_row(header_row, HEADER_SCHEMA, row_number=1)
        if header_errors:
            file_errors.update(header_errors)
            logger.error(f"Header errors in {file_path}: {header_errors}")
            print("Header errors:")
            for err in header_errors:
                print(err)
    else:
        warning_msg = f"File {file_path} does not have a header row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print("Warning: No header row found. Treating all rows as claim records.")

    if has_trailer:
        trailer_row = rows[-1]
        trailer_errors = validate_row(trailer_row, TRAILER_SCHEMA, row_number=len(rows))
        if trailer_errors:
            file_errors.update(trailer_errors)
            logger.error(f"Trailer errors in {file_path}: {trailer_errors}")
            print("Trailer errors:")
            for err in trailer_errors:
                print(err)
    else:
        warning_msg = f"File {file_path} does not have a trailer row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print("Warning: No trailer row found. Treating all rows as claim records.")

    # Determine claim rows.
    start_idx = 1 if has_header else 0
    end_idx = -1 if has_trailer else len(rows)
    claim_rows = rows[start_idx:end_idx]

    for idx, row in enumerate(claim_rows, start=start_idx+1):
        if len(row) != EXPECTED_CLAIM_FIELDS:
            err = f"Row {idx}: Expected {EXPECTED_CLAIM_FIELDS} columns, found {len(row)}."
            file_errors.add(err)
            print(err)
            continue
        errors = validate_row(row, CLAIM_SCHEMA, row_number=idx)
        for err in errors:
            file_errors.add(err)
            print(err)
        record_number = row[1].strip()
        if record_number in unique_record_numbers:
            dup_err = f"Row {idx}: Duplicate RecordNumber {record_number}."
            file_errors.add(dup_err)
            print(dup_err)
        else:
            unique_record_numbers.add(record_number)
        claim_count += 1

    if has_trailer:
        try:
            expected_count = int(trailer_row[1].strip())
            if expected_count != claim_count:
                err = f"Trailer count {expected_count} does not match actual claim count {claim_count}."
                file_errors.add(err)
                print(err)
        except Exception as e:
            err = f"Error parsing trailer Record Count: {e}"
            file_errors.add(err)
            print(err)

    if previous_record_count is not None:
        if previous_record_count > 0 and abs(claim_count - previous_record_count) / previous_record_count > 0.5:
            warn_msg = f"Warning: Claim count {claim_count} differs by more than 50% from previous count {previous_record_count}."
            logger.warning(warn_msg)
            print(warn_msg)

    error_count = len(file_errors)
    if error_count > 0:
        sorted_errors = sorted(file_errors)
        logger.error(f"Errors found in file {file_path}:")
        for err in sorted_errors:
            logger.error(err)
        summary_msg = f"Finished processing file {file_path} with {error_count} error(s) and {claim_count} claim record(s)."
        logger.error(summary_msg)
        print(f"\n{summary_msg}")
        print("Error details:")
        for err in sorted_errors:
            print(f" - {err}")
    else:
        summary_msg = f"File {file_path} processed successfully with {claim_count} claim record(s) and no errors."
        logger.info(summary_msg)
        print(summary_msg)
        logger.info("Total errors: 0")
        print("Total errors: 0")

def process_uploaded_file(file_obj, previous_record_count=None):
    """
    This function is similar to process_file() but accepts a file-like object.
    It can be used when a single file is uploaded (e.g., via a web form).
    """
    # Ensure we start reading from the beginning.
    file_obj.seek(0)
    print("\nProcessing uploaded file...")
    try:
        reader = csv.reader(file_obj)
        rows = list(reader)
    except Exception as e:
        err_msg = f"Error reading uploaded file: {str(e)}"
        logger.error(err_msg)
        print(err_msg)
        return

    if not rows:
        err_msg = "Uploaded file is empty."
        logger.error(err_msg)
        print(err_msg)
        return

    # Use the same logic as process_file (note: we do not have a file path here)
    file_errors = set()
    unique_record_numbers = set()
    claim_count = 0

    has_header = (rows[0][0].strip() == "HDR") if rows[0] else False
    has_trailer = (rows[-1][0].strip() == "TRL") if rows[-1] else False

    if has_header:
        header_row = rows[0]
        header_errors = validate_row(header_row, HEADER_SCHEMA, row_number=1)
        if header_errors:
            file_errors.update(header_errors)
            logger.error(f"Header errors in uploaded file: {header_errors}")
            print("Header errors:")
            for err in header_errors:
                print(err)
    else:
        warning_msg = "Uploaded file does not have a header row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print(warning_msg)

    if has_trailer:
        trailer_row = rows[-1]
        trailer_errors = validate_row(trailer_row, TRAILER_SCHEMA, row_number=len(rows))
        if trailer_errors:
            file_errors.update(trailer_errors)
            logger.error(f"Trailer errors in uploaded file: {trailer_errors}")
            print("Trailer errors:")
            for err in trailer_errors:
                print(err)
    else:
        warning_msg = "Uploaded file does not have a trailer row. Treating all rows as claim records."
        logger.warning(warning_msg)
        print(warning_msg)

    start_idx = 1 if has_header else 0
    end_idx = -1 if has_trailer else len(rows)
    claim_rows = rows[start_idx:end_idx]

    for idx, row in enumerate(claim_rows, start=start_idx+1):
        if len(row) != EXPECTED_CLAIM_FIELDS:
            err = f"Row {idx}: Expected {EXPECTED_CLAIM_FIELDS} columns, found {len(row)}."
            file_errors.add(err)
            print(err)
            continue
        errors = validate_row(row, CLAIM_SCHEMA, row_number=idx)
        for err in errors:
            file_errors.add(err)
            print(err)
        record_number = row[1].strip()
        if record_number in unique_record_numbers:
            dup_err = f"Row {idx}: Duplicate RecordNumber {record_number}."
            file_errors.add(dup_err)
            print(dup_err)
        else:
            unique_record_numbers.add(record_number)
        claim_count += 1

    if has_trailer:
        try:
            expected_count = int(trailer_row[1].strip())
            if expected_count != claim_count:
                err = f"Trailer count {expected_count} does not match actual claim count {claim_count}."
                file_errors.add(err)
                print(err)
        except Exception as e:
            err = f"Error parsing trailer Record Count: {e}"
            file_errors.add(err)
            print(err)

    if previous_record_count is not None:
        if previous_record_count > 0 and abs(claim_count - previous_record_count) / previous_record_count > 0.5:
            warn_msg = f"Warning: Claim count {claim_count} differs by more than 50% from previous count {previous_record_count}."
            logger.warning(warn_msg)
            print(warn_msg)

    error_count = len(file_errors)
    if error_count > 0:
        sorted_errors = sorted(file_errors)
        logger.error("Errors found in uploaded file:")
        for err in sorted_errors:
            logger.error(err)
        summary_msg = f"Finished processing uploaded file with {error_count} error(s) and {claim_count} claim record(s)."
        logger.error(summary_msg)
        print(f"\n{summary_msg}")
        print("Error details:")
        for err in sorted_errors:
            print(f" - {err}")
    else:
        summary_msg = f"Uploaded file processed successfully with {claim_count} claim record(s) and no errors."
        logger.info(summary_msg)
        print(summary_msg)
        logger.info("Total errors: 0")
        print("Total errors: 0")

def process_files_in_folder(folder_path, previous_counts=None):
    if not os.path.exists(folder_path):
        err_msg = f"Folder not found: {folder_path}"
        logger.error(err_msg)
        print(err_msg)
        return

    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
             if os.path.isfile(os.path.join(folder_path, f))]

    if not files:
        msg = f"No files found in folder: {folder_path}"
        logger.info(msg)
        print(msg)
        return

    for file_path in files:
        base_name = os.path.basename(file_path)
        prev_count = previous_counts.get(base_name) if previous_counts else None
        process_file(file_path, previous_record_count=prev_count)

# ==============================
# Main Processing Block
# ==============================
# if __name__ == "__main__":
#     # For folder processing:
#     folder_to_process = "file_"  # Folder containing vendor files
#     previous_counts = {
#         "input_file_28.csv": 74,
#         "input_file_14.csv": 70,
#         # Add additional previous counts as needed.
#     }
#     process_files_in_folder(folder_to_process, previous_counts=previous_counts)

#     # For a single file upload variant:
#     # For example, if you receive an uploaded file from a web form (here simulated by a local file):
#     upload_file_path = r"C:\Users\BARATH\Music\ai_service\input_file_20.csv"  # change to the uploaded file
#     if os.path.exists(upload_file_path):
#         with open(upload_file_path, "r", newline="", encoding="utf-8") as f:
#             process_uploaded_file(f, previous_record_count=previous_counts.get(os.path.basename(upload_file_path)))
from flask import Flask, request, jsonify, send_file
from flask.views import MethodView
import os
import logging
from werkzeug.utils import secure_filename

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
LOG_FILE = "smart_parser.log"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        file = request.files["file"]
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

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

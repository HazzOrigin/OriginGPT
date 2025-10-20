import os
import json
from datetime import datetime, timedelta
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# --- CONFIGURATION (FILL THESE IN) ---
# Your dedicated Google Drive Folder ID
DRIVE_FOLDER_ID = "YOUR_SHARED_DRIVE_FOLDER_ID" 
# Your GCS Staging Bucket Name
GCS_BUCKET_NAME = "your-llm-staging-bucket"
# Drive API Scopes needed (read-only access for files)
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# Name of the output file in GCS
OUTPUT_FILENAME = f"drive_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.jsonl"


def extract_file_content(drive_service, file_id, mime_type):
    """
    CONCEPTUAL FUNCTION: Downloads and extracts raw text from a file.
    **You need to fill in the complex logic here.**
    """
    # Simple example for Google Docs export:
    if 'document' in mime_type or 'text/plain' in mime_type:
        response = drive_service.files().export(
            fileId=file_id, 
            mimeType='text/plain'
        ).execute()
        return response.decode('utf-8')
        
    # Placeholder for PDFs/Sheets (more complex logic is needed here)
    else:
        # For a PDF, you would download the binary and run an OCR/parser.
        # For a Sheet, you would export as CSV and read rows.
        return f"Content extraction skipped for file type: {mime_type}"


def run_ingestion_job():
    """Main job that runs when the Cloud Run container starts."""
    print(f"Starting Drive ingestion job for folder: {DRIVE_FOLDER_ID}")
    
    # 1. AUTHENTICATION & CLIENT SETUP (Uses the service account assigned to the Cloud Run Job)
    # Cloud Run automatically handles the credentials for the assigned service account.
    drive_service = build('drive', 'v3')
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # 2. FILTERING LOGIC (Filter for files modified in the last 7 days)
    # Calculates the ISO-formatted date for 7 days ago, required by the Drive API 'q' parameter.
    one_week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat() + 'Z'
    
    # 3. CONSTRUCT THE DRIVE API QUERY (Filters by folder and modification date)
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and "
        f"modifiedTime > '{one_week_ago}' and "
        f"trashed = false"
    )
    
    print(f"Drive API Query: {query}")
    
    # 4. FETCH FILES AND PROCESS
    files = drive_service.files().list(q=query, fields='files(id, name, mimeType, modifiedTime)').execute().get('files', [])
    
    jsonl_records = []
    print(f"Found {len(files)} files to process.")
    
    for file in files:
        # Call the content extraction function
        extracted_text = extract_file_content(drive_service, file['id'], file['mimeType'])
        
        # Structure the final record
        record = {
            'document_id': file['id'],
            'file_name': file['name'],
            'text_content': extracted_text,
            'last_modified_date': file['modifiedTime'],
            'source': 'Google Drive',
        }
        jsonl_records.append(json.dumps(record))
        print(f"Processed: {file['name']}")

    # 5. UPLOAD TO GCS (The final staging step)
    if jsonl_records:
        blob = bucket.blob(OUTPUT_FILENAME)
        blob.upload_from_string('\n'.join(jsonl_records), content_type='application/x-ndjson') # application/x-ndjson is the correct MIME type for JSONL
        print(f"SUCCESS: Uploaded {len(jsonl_records)} records to gs://{GCS_BUCKET_NAME}/{OUTPUT_FILENAME}")
    else:
        print("INFO: No files were processed, skipping GCS upload.")


if __name__ == "__main__":
    run_ingestion_job()

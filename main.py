import os
import json
import io
import csv
from datetime import datetime, timedelta
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- CONFIGURATION (UPDATE THESE IN YOUR GITHUB REPO) ---

# The ID of your dedicated Google Drive Shared Folder. 
# !!! This has been updated with the value you provided !!!
DRIVE_FOLDER_ID = "0AETr9bCpxmVOUk9PVA" 

# Your GCS Staging Bucket Name (must exist in GCP).
# !!! This is already correctly set based on your GCS screenshot !!!
GCS_BUCKET_NAME = "drive-ingest-bot"

# File name for the JSONL output file in GCS.
OUTPUT_FILENAME = f"drive_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.jsonl"

# --- DRIVE API MIMETYPES ---

# Defines the export format for Google Workspace documents (Docs, Sheets, etc.)
MIMETYPES = {
    # Exported as plain text
    'application/vnd.google-apps.document': 'text/plain',
    'application/vnd.google-apps.presentation': 'text/plain',
    # Exported as CSV (Sheets will be extracted cell-by-cell)
    'application/vnd.google-apps.spreadsheet': 'text/csv',
    # Supported files that are already text or will be downloaded as binary (PDF/Image)
    'text/plain': 'text/plain',
    'application/pdf': 'application/pdf' # Standard PDF, requires external OCR if text is unreadable
}

# --- CORE LOGIC ---

def download_file(drive_service, file_id, mime_type):
    """Downloads files that are NOT Google native formats (e.g., PDFs, external text files)."""
    try:
        # Use a generic download to get the file content
        request = drive_service.files().get_media(fileId=file_id)
        
        # Create an in-memory stream to hold the file content
        file_content = io.BytesIO()
        
        # Download the file content into the stream
        downloader = storage.MediaIoBaseDownload(file_content, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        file_content.seek(0)
        
        # Crude text extraction for PDF/Binary files: return placeholder
        if mime_type == 'application/pdf':
             # In a production system, you would call a Document AI / OCR service here.
             return f"Binary Content (PDF) Downloaded. OCR/Extraction required." 
        
        # Simple decode for text files
        return file_content.read().decode('utf-8')

    except HttpError as error:
        print(f"An error occurred during download: {error}")
        return f"Download Failed: {error}"


def export_google_doc(drive_service, file_id, export_mime):
    """Exports Google native documents (Docs, Slides) as plain text."""
    try:
        request = drive_service.files().export_media(fileId=file_id, mimeType=export_mime)
        return request.execute().decode('utf-8')
    except HttpError as error:
        print(f"An error occurred during export: {error}")
        return f"Export Failed: {error}"


def extract_sheet_content(drive_service, file_id):
    """Exports a Google Sheet as CSV and combines content into a single string."""
    try:
        # Export as CSV
        request = drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
        csv_bytes = request.execute()
        
        # Read the CSV content
        csv_string = csv_bytes.decode('utf-8')
        f = io.StringIO(csv_string)
        reader = csv.reader(f)
        
        # Combine all cell contents into one searchable string
        all_content = []
        for row in reader:
            # Filters out empty cells and joins the rest of the row with a space
            all_content.append(" ".join(filter(None, row)))
            
        # Join all rows into a single document-like string
        return " | ".join(all_content)

    except HttpError as error:
        print(f"An error occurred during Sheet export: {error}")
        return f"Sheet Export Failed: {error}"


def extract_file_content(drive_service, file_id, file_mime, file_name):
    """Routes the file processing based on its MIME type."""
    
    # Check if the file is a Google native type (Doc, Sheet, Slide)
    if file_mime in MIMETYPES:
        export_mime = MIMETYPES[file_mime]
        
        if export_mime == 'text/csv':
            print(f" -- Extracting Sheet: {file_name}")
            return extract_sheet_content(drive_service, file_id)
        
        elif export_mime == 'text/plain':
            print(f" -- Exporting Document: {file_name}")
            return export_google_doc(drive_service, file_id, export_mime)
    
    # Handle files that are NOT Google native (PDF, general text, binary)
    elif file_mime in MIMETYPES.values():
        print(f" -- Downloading Binary: {file_name} ({file_mime})")
        return download_file(drive_service, file_id, file_mime)

    else:
        # Skip unsupported file types
        print(f" -- SKIPPED: Unsupported file type: {file_mime} for {file_name}")
        return "" # Return empty string for skipped files


def run_ingestion_job():
    """Main job that runs when the Cloud Run container starts."""
    
    # 1. AUTHENTICATION & CLIENT SETUP 
    # Client build is simplified as Cloud Run handles credentials automatically.
    try:
        drive_service = build('drive', 'v3')
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
    except Exception as e:
        print(f"ERROR during client setup: {e}")
        # Exit job if setup fails
        return 

    # 2. FILTERING LOGIC (Filter for files modified in the last 7 days)
    # Calculates the ISO-formatted date for 7 days ago.
    one_week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat() + 'Z'
    
    # 3. CONSTRUCT THE DRIVE API QUERY (Filters by folder and modification date)
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and "
        f"modifiedTime > '{one_week_ago}' and "
        f"trashed = false"
    )
    
    print(f"\nStarting Drive ingestion job for folder: {DRIVE_FOLDER_ID}")
    print(f"Querying files modified since: {one_week_ago}")

    # 4. FETCH FILES AND PROCESS
    try:
        files = drive_service.files().list(
            q=query, 
            fields='files(id, name, mimeType, modifiedTime)'
        ).execute().get('files', [])
    except HttpError as e:
        print(f"ERROR fetching files from Drive: {e}")
        return

    jsonl_records = []
    print(f"Found {len(files)} files to process.\n")
    
    for file in files:
        # Extract content using the new routing function
        extracted_text = extract_file_content(drive_service, file['id'], file['mimeType'], file['name'])
        
        # Only process if we successfully got content
        if extracted_text and not extracted_text.startswith(("Content extraction skipped", "Export Failed", "Download Failed")):
            # Structure the final record
            record = {
                'document_id': file['id'],
                'file_name': file['name'],
                'text_content': extracted_text,
                'last_modified_date': file['modifiedTime'],
                'source': 'Google Drive',
            }
            jsonl_records.append(json.dumps(record))
            print(f" -- Successfully prepared record for: {file['name']}")
        else:
            print(f" -- Skipping file: {file['name']} (Reason: Extraction failed or unsupported type)")


    # 5. UPLOAD TO GCS (The final staging step)
    if jsonl_records:
        blob = bucket.blob(OUTPUT_FILENAME)
        blob.upload_from_string('\n'.join(jsonl_records), content_type='application/x-ndjson')
        print(f"\nSUCCESS: Uploaded {len(jsonl_records)} records to gs://{GCS_BUCKET_NAME}/{OUTPUT_FILENAME}")
    else:
        print("\nINFO: No new files were processed in the last week, skipping GCS upload.")


if __name__ == "__main__":
    run_ingestion_job()

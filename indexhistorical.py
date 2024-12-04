import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import os
import json
import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Google Sheet ID

# Raise error if credentials are not set
if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
if not SHEET_ID:
    raise ValueError("SHEET_ID environment variable is not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

# Google Sheets setup
def authenticate_google_sheets():
    # Refresh the credentials if expired
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())

    # Authorize with gspread
    client = gspread.authorize(credentials)
    return client

# Function to update data in Google Sheets
def update_google_sheet(dataframe, sheet_id):
    # Authenticate and get the Google Sheets client
    client = authenticate_google_sheets()

    # Open the spreadsheet by ID
    spreadsheet = client.open_by_key(sheet_id)

    # Check if the "indexhistorical" sheet exists
    try:
        sheet = spreadsheet.worksheet("indexhistorical")
        print("'indexhistorical' sheet found.")
    except gspread.exceptions.WorksheetNotFound:
        # If the sheet does not exist, create it
        print("'indexhistorical' sheet not found. Creating new sheet.")
        sheet = spreadsheet.add_worksheet(title="indexhistorical", rows="1000", cols="20")  # Adjust rows and cols as needed
    
    # Clear any existing data in the sheet
    sheet.clear()

    # Update the sheet with the new data
    sheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    print("Data updated successfully in 'indexhistorical' Google Sheets.")

async def fetch_csv(session, url):
    """Fetch the CSV data from the URL and return it as a pandas DataFrame."""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                csv_content = await response.text()
                return pd.read_csv(StringIO(csv_content))
            else:
                print(f"No data available for {url}. HTTP Status: {response.status}")
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
    return None

async def download_and_combine(start_date, end_date):
    """Download CSV files for dates between start_date and end_date, and combine them in-memory."""
    base_url = "https://archives.nseindia.com/content/indices/ind_close_all_{}.csv"
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            formatted_date = date.strftime("%d%m%Y")
            url = base_url.format(formatted_date)
            tasks.append(fetch_csv(session, url))
        
        # Wait for all download tasks to complete
        results = await asyncio.gather(*tasks)

    # Filter out None values (failed downloads)
    dataframes = [df for df in results if df is not None]

    # Combine all DataFrames into a single DataFrame
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        print("No data available to combine.")
        return None

async def main():
    """Main function to download and combine the last month's data."""
    end_date = datetime.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=90)       # Last 30 days
    
    # Download and combine data
    combined_df = await download_and_combine(start_date, end_date)

    if combined_df is not None:
        # Save the combined data to Google Sheets
        update_google_sheet(combined_df, SHEET_ID)

if __name__ == "__main__":
    asyncio.run(main())

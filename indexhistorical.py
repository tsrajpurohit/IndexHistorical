import aiohttp
import asyncio
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from io import StringIO
import gspread
import traceback
from google.oauth2.service_account import Credentials

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string from environment variable
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Sheet ID from environment variable

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

# Initialize Google Sheets client
client = gspread.authorize(credentials)

# Function to create the sheet if it doesn't exist
def create_sheet_if_not_exists(client, sheet_id, sheet_name):
    sheet = client.open_by_key(sheet_id)
    worksheets = sheet.worksheets()  # Get all worksheets in the spreadsheet
    sheet_titles = [ws.title for ws in worksheets]  # List of sheet names

    if sheet_name not in sheet_titles:
        # If the sheet doesn't exist, create a new one
        sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        print(f"Sheet '{sheet_name}' created.")
    else:
        print(f"Sheet '{sheet_name}' already exists.")

# Function to update data in Google Sheets
def update_google_sheet(dataframe, sheet_id, sheet_name):
    sheet = client.open_by_key(sheet_id)

    # Create the sheet if it doesn't exist
    create_sheet_if_not_exists(client, sheet_id, sheet_name)

    # Select the sheet by name
    worksheet = sheet.worksheet(sheet_name)
    worksheet.clear()  # Clear the existing data

    # Update the sheet with the new data
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    print("Data updated successfully in Google Sheets.")

async def fetch_csv(session, url):
    """Fetch CSV from NSE Archives."""
    try:
        async with session.get(url) as response:
            date_str = url.split("_")[-1].replace(".csv", "")
            if response.status == 200:
                print(f"✅ Success: {date_str}")
                csv_data = await response.text()
                return pd.read_csv(StringIO(csv_data))
            else:
                print(f"❌ HTTP Error {response.status} for {url}")
    except Exception as e:
        print(f"❌ Exception while fetching {url}: {repr(e)}")  # <== FIXED: Now shows exact error
    return None

async def download_and_combine(start_date, end_date):
    base_url = "https://nsearchives.nseindia.com/content/indices/ind_close_all_{}.csv"
    tasks = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for i in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            formatted_date = date.strftime("%d%m%Y")
            url = base_url.format(formatted_date)
            tasks.append(fetch_csv(session, url))

        results = await asyncio.gather(*tasks)
        dataframes = [df for df in results if df is not None]

        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
        else:
            print("❌ No data fetched for any date.")
            return None

async def main():
    """Main function to download and combine the last month's data."""
    end_date = datetime.today() - timedelta(days=0)  # Yesterday
    start_date = end_date - timedelta(days=140)       # Last 140 days

    # Download and combine data
    combined_df = await download_and_combine(start_date, end_date)

    if combined_df is not None and not combined_df.empty:
        try:
            script_directory = os.getcwd()  # Use current working directory
            csv_path = os.path.join(script_directory, 'combined_data.csv')
            combined_df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"Data saved to '{csv_path}'.")

            # Now update the Google Sheet with the data
            sheet_name = "indexhistorical"  # Modify this to match your desired sheet name
            update_google_sheet(combined_df, SHEET_ID, sheet_name)

        except Exception as e:
            print(f"Error saving CSV file: {e}")
            traceback.print_exc()
    else:
        print("DataFrame is empty, no CSV file created.")

if __name__ == "__main__":
    asyncio.run(main())

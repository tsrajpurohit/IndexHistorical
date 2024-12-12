import aiohttp
import asyncio
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from io import StringIO
import gspread
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
    end_date = datetime.today() - timedelta(days=0)  # Yesterday
    start_date = end_date - timedelta(days=140)       # Last 30 days
    
    # Download and combine data
    combined_df = await download_and_combine(start_date, end_date)

    if combined_df is not None:
        # Save the combined data to Google Sheets (specifically to the 'indexhistorical' sheet)
        update_google_sheet(combined_df, SHEET_ID, "indexhistorical")
        
        # Save the combined data to a CSV file
        try:
            if not combined_df.empty:
                script_directory = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
                csv_path = os.path.join(script_directory, 'combined_data.csv')
                combined_df.to_csv(csv_path, index=False)
                print(f"Data saved to '{csv_path}'.")

            else:
                print("DataFrame is empty, no CSV file created.")
        except Exception as e:
            print(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    asyncio.run(main())

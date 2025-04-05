import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

import re # Added import
import gspread
from google.oauth2.service_account import Credentials

from config_loader import AppConfig
# Import QueueManager later if needed for direct interaction, or use callbacks/events
# from queue_manager import QueueManager # Example

logger = logging.getLogger(__name__)

# Headers are now defined in AppConfig and passed via the config object

def sanitize_worksheet_name(name: str) -> str:
    """Removes characters potentially invalid for Google Sheet tab names."""
    # Basic sanitization: remove common invalid chars. Adjust as needed.
    return re.sub(r'[\\/*?:\[\]]', '', name)[:100] # Max 100 chars for sheet names

def setup_google_sheet(config: AppConfig) -> Optional[Dict[str, gspread.Worksheet]]:
    """Set up Google Sheets connection and initialize worksheets."""
    try:
        logger.info("Setting up Google Sheets connection...")

        # Parse Google credentials from config
        try:
            credentials_dict = json.loads(config.google_credentials_json)
            # Ensure private key format if needed (gspread might handle this)
            if 'private_key' in credentials_dict:
                credentials_dict['private_key'] = credentials_dict['private_key'].replace('\\n', '\n')
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
            raise ValueError("Invalid Google Credentials JSON format") from e
        except Exception as e: # Added generic exception handler
            logger.error(f"Unexpected error processing credentials: {e}")
            raise ValueError("Error processing Google Credentials") from e


        # Use minimal required scope
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
        gc = gspread.authorize(creds)

        # Open the spreadsheet
        try:
            spreadsheet = gc.open_by_key(config.google_sheet_id)
            logger.info(f"Opened spreadsheet: {spreadsheet.title}")
        except gspread.exceptions.APIError as e:
            logger.error(f"Failed to access Google Sheet with ID {config.google_sheet_id}: {e}")
            # Check for common errors like permissions
            if e.response.status_code == 403:
                 logger.error("Permission denied. Ensure the service account email has editor access to the sheet.")
            elif e.response.status_code == 404:
                 logger.error("Spreadsheet not found. Verify the GOOGLE_SHEET_ID.")
            raise
        except Exception as e:
             logger.error(f"Unexpected error opening spreadsheet: {e}")
             raise

        # Define and sanitize worksheet names
        sanitized_base_name = sanitize_worksheet_name(config.worksheet_name)
        high_salary_ws_name = f"{sanitized_base_name} - High Salary"
        low_salary_ws_name = f"{sanitized_base_name} - Low Salary"

        sheets = {}
        for ws_name in [high_salary_ws_name, low_salary_ws_name]:
            try:
                worksheet = spreadsheet.worksheet(ws_name)
                logger.info(f"Found existing worksheet: {ws_name}")
                # Optional: Clear existing content if needed (be careful!)
                # worksheet.clear()
                # logger.info(f"Cleared existing worksheet: {ws_name}")
                # Ensure headers are present and correct using config.expected_headers
                header_row = worksheet.row_values(1)
                if header_row != config.expected_headers:
                     logger.warning(f"Header mismatch in '{ws_name}'. Expected: {config.expected_headers}, Found: {header_row}. Overwriting headers.")
                     # Clear potentially misaligned data before setting new headers
                     logger.warning(f"Clearing existing data in worksheet '{ws_name}' due to header mismatch.")
                     worksheet.clear() # Clear all data
                     worksheet.update('A1', [config.expected_headers]) # Update headers
                     # worksheet.clear_basic_filter() # May not be needed after clear()
                     # worksheet.resize(rows=1) # Delete all rows except header
                     # worksheet.resize(rows=1000) # Resize back
                else:
                     logger.debug(f"Headers verified for worksheet: {ws_name}")

            except gspread.WorksheetNotFound:
                logger.info(f"Worksheet '{ws_name}' not found, creating...")
                # Create new worksheet with sufficient rows/cols
                worksheet = spreadsheet.add_worksheet(ws_name, rows=1000, cols=len(config.expected_headers))
                # Add headers to the new sheet
                worksheet.append_row(config.expected_headers)
                logger.info(f"Created worksheet: {ws_name} and added headers.")

            # Apply formatting (optional, can be done once)
            try:
                header_format = {
                    'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8},
                    'textFormat': {'bold': True},
                    'horizontalAlignment': 'CENTER'
                }
                worksheet.format(f'A1:{gspread.utils.rowcol_to_a1(1, len(config.expected_headers))}', header_format)
                # Freeze header row
                worksheet.freeze(rows=1)
            except Exception as format_e:
                 logger.warning(f"Could not apply formatting to '{ws_name}': {format_e}")

            # Store worksheet based on type (high/low salary)
            if ws_name == high_salary_ws_name:
                sheets["high_salary"] = worksheet
            else:
                sheets["low_salary"] = worksheet

        logger.info("Google Sheet setup completed successfully.")
        return sheets

    except Exception as e:
        logger.error(f"Error setting up Google Sheet: {e}", exc_info=True)
        return None # Return None on failure

async def setup_google_sheet_async(config: AppConfig) -> Optional[Dict[str, gspread.Worksheet]]:
    """Async wrapper for setup_google_sheet using asyncio.to_thread."""
    try:
        # gspread operations are blocking, run them in a separate thread
        return await asyncio.to_thread(setup_google_sheet, config)
    except Exception as e:
        logger.error(f"Async error setting up Google Sheets: {e}", exc_info=True)
        return None


class SheetManager:
    """Manages interactions with Google Sheets for saving job data."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.sheets: Optional[Dict[str, gspread.Worksheet]] = None
        self._lock = asyncio.Lock() # Lock for initializing sheets
        self._write_locks: Dict[str, asyncio.Lock] = { # Per-sheet write locks
            "high_salary": asyncio.Lock(),
            "low_salary": asyncio.Lock()
        }
        self._last_request_time = 0
        self._min_request_interval = 1.1 # Seconds between API calls (slightly > 1s)

    async def initialize_sheets(self):
        """Initialize Google Sheets connection and worksheets asynchronously."""
        async with self._lock: # Prevent concurrent initialization
            if self.sheets is None:
                logger.info("Initializing SheetManager sheets...")
                self.sheets = await setup_google_sheet_async(self.config)
                if self.sheets:
                    logger.info("SheetManager sheets initialized successfully.")
                else:
                    logger.error("Failed to initialize SheetManager sheets.")
                    # Consider raising an error or setting a failed state
            return self.sheets is not None

    def _prepare_row_data(self, job_data: dict) -> Optional[List[Any]]:
        """Prepare and validate a single row for Google Sheets insertion."""
        try:
            # Ensure all expected keys exist, provide defaults if necessary
            row = [
                job_data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                job_data.get('channel', 'N/A'),
                job_data.get('position', 'N/A'),
                job_data.get('email', ''),
                job_data.get('what_they_offer', ''),
                job_data.get('application_link', ''),
                job_data.get('telegram_link', ''),
                job_data.get('salary', ''),
                'Yes' if job_data.get('high_salary', False) else 'No',
                job_data.get('schedule_type', 'Unknown'),
                job_data.get('job_type', 'Unknown'),
                job_data.get('fit_percentage', 0) # Ensure it's a number
            ]

            # Basic type validation/conversion (gspread might handle some)
            try:
                # Ensure Fit % is an integer
                fit_percentage_val = job_data.get('fit_percentage', 0)
                row[11] = int(fit_percentage_val) if fit_percentage_val is not None else 0
            except (ValueError, TypeError):
                 logger.warning(f"Could not convert fit_percentage '{job_data.get('fit_percentage')}' to int. Defaulting to 0.")
                 row[11] = 0 # Default to 0 if conversion fails

            # Ensure length matches headers from config
            if len(row) != len(self.config.expected_headers):
                 logger.error(f"Row length mismatch: Expected {len(self.config.expected_headers)}, Got {len(row)}. Data: {job_data}")
                 return None

            return row
        except Exception as e:
            logger.error(f"Error preparing row data: {e}. Data: {job_data}", exc_info=True)
            return None

    async def save_batch_to_sheet(self, queue_type: str, batch_data: List[dict]) -> bool:
        """
        Saves a batch of job data dictionaries to the specified Google Sheet queue type.

        Args:
            queue_type: 'high_salary' or 'low_salary'.
            batch_data: A list of job data dictionaries.

        Returns:
            True if successful, False otherwise.
        """
        if not batch_data:
            logger.debug(f"Empty batch received for {queue_type}, nothing to save.")
            return True # Considered successful as there's nothing to do

        if not await self.initialize_sheets(): # Ensure sheets are ready
             logger.error(f"Cannot save batch to {queue_type}: Sheets not initialized.")
             return False

        if queue_type not in self.sheets:
            logger.error(f"Invalid queue_type '{queue_type}'. Must be 'high_salary' or 'low_salary'.")
            return False

        sheet = self.sheets[queue_type]
        write_lock = self._write_locks[queue_type]

        # Prepare rows from the batch of dictionaries
        rows_to_append = []
        for job_data in batch_data:
            prepared_row = self._prepare_row_data(job_data)
            if prepared_row:
                rows_to_append.append(prepared_row)
            else:
                logger.warning(f"Skipping invalid job data in batch for {queue_type}: {job_data.get('position', 'N/A')}")

        if not rows_to_append:
            logger.warning(f"No valid rows prepared from batch for {queue_type}.")
            return True # No valid data to write is not a failure of the save operation itself

        logger.info(f"Attempting to save batch of {len(rows_to_append)} rows to '{sheet.title}'...")

        async with write_lock: # Ensure only one write operation per sheet at a time
            retry_count = 0
            max_retries = self.config.max_retries
            initial_delay = self.config.initial_retry_delay

            while retry_count < max_retries:
                try:
                    # Rate limiting check before API call
                    current_time = time.monotonic()
                    time_since_last = current_time - self._last_request_time
                    if time_since_last < self._min_request_interval:
                        wait_needed = self._min_request_interval - time_since_last
                        logger.debug(f"Rate limiting: waiting {wait_needed:.2f}s before writing to {sheet.title}")
                        await asyncio.sleep(wait_needed)

                    # Perform the append operation in a separate thread
                    await asyncio.to_thread(sheet.append_rows, values=rows_to_append, value_input_option='USER_ENTERED')

                    self._last_request_time = time.monotonic() # Update last request time on success
                    logger.info(f"Successfully appended {len(rows_to_append)} rows to '{sheet.title}'.")
                    return True # Success

                except gspread.exceptions.APIError as e:
                    retry_count += 1
                    error_code = e.response.status_code
                    error_message = str(e)
                    logger.error(f"API error writing to '{sheet.title}' (Attempt {retry_count}/{max_retries}): {error_code} - {error_message}")

                    if error_code == 429: # Rate limit exceeded
                        # Extract wait time from error if possible, otherwise use default backoff
                        wait_time = 60 # Default wait for rate limit
                        logger.warning(f"Rate limit exceeded for '{sheet.title}'. Waiting {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                        # Don't increment retry count for standard rate limit waits, let it try again
                        retry_count -= 1
                        continue # Skip to next iteration after waiting

                    elif error_code in [500, 503]: # Server error, retryable
                         if retry_count >= max_retries:
                              logger.error(f"Max retries reached for server error on '{sheet.title}'.")
                              return False # Failed after retries
                         wait_time = initial_delay * (2 ** (retry_count -1)) # Exponential backoff
                         logger.warning(f"Server error ({error_code}) on '{sheet.title}'. Retrying in {wait_time:.1f} seconds...")
                         await asyncio.sleep(wait_time)
                         continue
                    else: # Other API errors (permissions, bad request, etc.) - likely not retryable
                         logger.error(f"Non-retryable API error ({error_code}) on '{sheet.title}'. Aborting batch save.")
                         return False # Failed, likely won't succeed with retries

                except Exception as e:
                    retry_count += 1
                    logger.error(f"Unexpected error writing to '{sheet.title}' (Attempt {retry_count}/{max_retries}): {e}", exc_info=True)
                    if retry_count >= max_retries:
                         logger.error(f"Max retries reached for unexpected error on '{sheet.title}'.")
                         return False # Failed after retries
                    wait_time = initial_delay * (2 ** (retry_count - 1))
                    logger.warning(f"Retrying write to '{sheet.title}' in {wait_time:.1f} seconds...")
                    await asyncio.sleep(wait_time)
                    continue

            # If loop finishes without returning True, it means all retries failed
            logger.error(f"Failed to save batch to '{sheet.title}' after {max_retries} attempts.")
            return False

    async def cleanup(self):
        """Perform any cleanup needed for the SheetManager."""
        logger.info("SheetManager cleanup.")
        # No explicit cleanup needed for gspread client usually
        pass

    async def __aenter__(self):
        await self.initialize_sheets()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

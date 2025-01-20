import os
import re
import csv
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from playwright.async_api import (
    async_playwright,
    BrowserContext,
    TimeoutError
)

from login import LoginManager  # Import LoginManager from login.py

# Load environment variables
load_dotenv()

SCOPUS_VIA_PROXY: bool = os.getenv("SCOPUS_VIA_PROXY")
SCOPUS_BASE_URL = os.getenv("SCOPUS_BASE_URL_VIA_PROXY") if SCOPUS_VIA_PROXY else os.getenv("SCOPUS_BASE_URL")

INPUT_CSV_PATH = "eid_with_titles.csv"   # Must have columns: EID, Title
DOWNLOADS_DIR = "miscited_downloads"     # Root folder for all EID folders
LOG_FILE_PATH = Path(DOWNLOADS_DIR) / "miscited_downloads.log"  # Single log file
MAX_CONCURRENCY = 5                     # Number of concurrent pages
CHUNK_SIZE = 100                        # Update status.csv after every 100 EIDs

# Configure global logging
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)


def query_parser(s, parsing_enabled):
    """
    - If s is None or empty, returns an empty string.
    - Otherwise, converts to lowercase and replaces all non-alphanumeric chars with spaces
      if parsing_enabled is True; otherwise returns s unmodified.
    """
    if not s:
        return ""
    s = s.lower()
    return re.sub(r"[^a-zA-Z0-9]+", " ", s) if parsing_enabled else s


class MiscitedDocumentScraper:
    def __init__(self, login_manager: LoginManager):
        # Initialize logger for this module
        self.logger = logging.getLogger(__name__)
        
        self.login_manager = login_manager

    async def login_and_get_context(self, playwright):
        """
        Launch a Chromium browser in headless mode, add saved cookies to the context,
        and verify if the session is still valid. If not, perform re-login.
        """
        browser = await playwright.chromium.launch(headless=True)

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        )

        try:
            cookies = await self.login_manager.relogin_and_reload_cookies()
            await context.add_cookies(cookies)
            self.logger.info("Cookies added to Playwright context.")

        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}")
            raise

        return browser, context

    def read_input_csv(self):
        """Load EIDs/Titles from CSV into a list of dicts."""
        if not Path(INPUT_CSV_PATH).exists():
            self.logger.error(f"CSV file '{INPUT_CSV_PATH}' not found.")
            return []
        rows = []
        with open(INPUT_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        self.logger.info(f"Read {len(rows)} rows from {INPUT_CSV_PATH}.")
        return rows

    async def scrape_single_eid(self, context: BrowserContext, row: dict, sem: asyncio.Semaphore):
        """
        Process a single EID:
          - If folder already exists with success.txt or empty.txt => skip.
          - Else => re-process (folder might exist but no signals => previous fail).
          - If 0 docs => create empty.txt
          - If export success => create success.txt
          - If error => no signals => "fail" in final status.csv
        """
        async with sem:
            eid = row.get("EID", "")
            raw_title = row.get("Title", "")
            folder_path = Path(DOWNLOADS_DIR) / eid

            try:
                # Check if folder already exists and has success/empty
                if folder_path.exists():
                    success_file = folder_path / "success.txt"
                    empty_file = folder_path / "empty.txt"
                    if success_file.exists() or empty_file.exists():
                        self.logger.info(f"[{eid}] Folder has success/empty. Skipping re-processing.")
                        return
                    else:
                        self.logger.info(f"[{eid}] Folder exists but no success/empty. Re-processing.")
                else:
                    self.logger.info(f"[{eid}] Creating folder: {folder_path}")
                    folder_path.mkdir(parents=True, exist_ok=True)

                # Build the search URL using the parsed query
                parsed_title = query_parser(raw_title, True)
                query_text = f'"{parsed_title}"'
                search_url = (
                    f"{SCOPUS_BASE_URL}results/results.uri"
                    f"?sort=plf-f&src=dm&s=ALL%28{query_text}%29"
                    "&limit=10"
                    "&sessionSearchId=placeholder"
                    "&origin=searchbasic"
                    "&sdt=b"
                )

                page = await context.new_page()
                self.logger.info(f"[{eid}] Navigating to: {search_url}")
                await page.goto(search_url, wait_until="networkidle")

                # Check if 0 documents => e.g., span[data-testid='no-results-with-suggestion']
                no_results_found = False
                try:
                    await page.wait_for_selector("span[data-testid='no-results-with-suggestion']", timeout=500)
                    no_results_found = True
                except TimeoutError:
                    pass  # Means we didn't see the "no results" element => likely some results

                if no_results_found:
                    empty_file = folder_path / "empty.txt"
                    empty_file.touch(exist_ok=True)
                    self.logger.info(f"[{eid}] 0 documents found. Created empty.txt.")
                    await page.close()
                    return

                # Attempt the export
                try:
                    await page.locator("input[aria-label='Select all'][type='checkbox']").check()
                    self.logger.info(f"[{eid}] 'Select all' checkbox checked.")

                    await page.locator(".export-dropdown button").click()
                    self.logger.info(f"[{eid}] Export menu opened.")

                    await page.locator("button[data-testid='export-to-csv']").click()
                    self.logger.info(f"[{eid}] 'Export to CSV' clicked.")

                    # Wait for file download
                    async with page.expect_download(timeout=60000) as download_info:
                        await page.locator("button[data-testid='submit-export-button']").click()
                        self.logger.info(f"[{eid}] Export submitted.")
                    download = await download_info.value

                    # Save CSV
                    csv_path = folder_path / f"{eid}.csv"
                    await download.save_as(str(csv_path))
                    self.logger.info(f"[{eid}] Downloaded CSV saved to {csv_path}.")

                    # Mark success
                    success_file = folder_path / "success.txt"
                    success_file.touch(exist_ok=True)
                    self.logger.info(f"[{eid}] Created success.txt.")
                except Exception as e:
                    self.logger.error(f"[{eid}] Error during export flow: {e}")
                    # Folder remains with no success/empty => "fail" in final status

                await page.close()
            except Exception as e:
                self.logger.error(f"[{eid}] Unexpected error: {e}")

    async def run(self):
        """
        Main logic:
          1) Read EID rows.
          2) Connect/login once (reuse the same context).
          3) Process rows in chunks => concurrency with a semaphore.
          4) After each chunk, generate a fresh status.csv.
          5) One final status.csv at the end.
        """
        rows = self.read_input_csv()
        if not rows:
            return

        async with async_playwright() as p:
            try:
                browser, context = await self.login_and_get_context(p)
            except Exception as e:
                self.logger.critical(f"Failed to initialize browser context: {e}")
                return

            total = len(rows)
            for start_index in range(0, total, CHUNK_SIZE):
                chunk = rows[start_index : start_index + CHUNK_SIZE]
                sem = asyncio.Semaphore(MAX_CONCURRENCY)

                tasks = [
                    asyncio.create_task(self.scrape_single_eid(context, row, sem))
                    for row in chunk
                ]
                await asyncio.gather(*tasks)

                # After finishing this chunk => generate status.csv
                self.generate_status_csv(rows)
                self.logger.info(f"[Chunk] Processed up to row {start_index + len(chunk)} / {total}.")

            await browser.close()
            self.logger.info("All chunks completed. Browser closed.")

        # One final status.csv at the end
        self.generate_status_csv(rows)
        self.logger.info("Final status.csv written.")

    def generate_status_csv(self, rows):
        """
        Writes a fresh status.csv in DOWNLOADS_DIR by scanning each EID folder:
          - If folder does not exist => "not_started"
          - If folder has success.txt => "success"
          - Else if folder has empty.txt => "empty"
          - Else => "fail" (folder exists but no success or empty)
        Overwrites status.csv each time, no appending.
        """
        statuses = []
        for row in rows:
            eid = row.get("EID", "")
            folder_path = Path(DOWNLOADS_DIR) / eid
            if not folder_path.exists():
                statuses.append({"EID": eid, "Status": "not_started"})
                continue

            success_file = folder_path / "success.txt"
            empty_file = folder_path / "empty.txt"
            if success_file.exists():
                statuses.append({"EID": eid, "Status": "success"})
            elif empty_file.exists():
                statuses.append({"EID": eid, "Status": "empty"})
            else:
                # Folder exists but no success/empty => "fail"
                statuses.append({"EID": eid, "Status": "fail"})

        status_csv_path = Path(DOWNLOADS_DIR) / "status.csv"
        fieldnames = ["EID", "Status"]
        with status_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(statuses)

        self.logger.info(f"[Status] Wrote {len(statuses)} rows to {status_csv_path}")

if __name__ == "__main__":
    # Configure centralized logging
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting MiscitedDocumentScraper.")

    # Initialize LoginManager (no separate log file)
    login_manager = LoginManager()

    # Initialize the scraper with the login manager
    scraper = MiscitedDocumentScraper(login_manager)

    # Run the scraper
    try:
        asyncio.run(scraper.run())
    except Exception as e:
        logger.critical(f"Critical error occurred: {e}")

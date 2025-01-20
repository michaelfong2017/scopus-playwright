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

import pandas as pd  # Ensure pandas is imported for generate_overall_csv

# Load environment variables
load_dotenv()

SCOPUS_USERNAME = os.getenv("SCOPUS_USERNAME")
SCOPUS_PASSWORD = os.getenv("SCOPUS_PASSWORD")

# Updated directory and file paths
CITING_DOWNLOADS_DIR = "citing_downloads"
REFERENCES_OF_CITING_DOWNLOADS_DIR = "references_of_citing_download"
OVERALL_REFERENCES_CSV = "overall_references_of_citing.csv"
LOG_FILE_PATH = Path(REFERENCES_OF_CITING_DOWNLOADS_DIR) / "references_of_citing_download.log"

MAX_CONCURRENCY = 5
CHUNK_SIZE = 100

# Configure global logging
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)


def query_parser(s, parsing_enabled):
    if not s:
        return ""
    s = s.lower()
    return re.sub(r"[^a-zA-Z0-9]+", " ", s) if parsing_enabled else s


class ReferencesOfCitingScraper:
    def __init__(self, login_manager: LoginManager):
        # Initialize logger for this module
        self.logger = logging.getLogger(__name__)

        if not SCOPUS_USERNAME or not SCOPUS_PASSWORD:
            raise ValueError("SCOPUS_USERNAME or SCOPUS_PASSWORD not set in environment.")
        
        self.citing_articles = []  # List to store citing article details

        self.login_manager = login_manager

    def discover_all_citing_articles(self):
        """
        Discover all citing articles from the citing_downloads directory.
        """
        base_path = Path(CITING_DOWNLOADS_DIR)
        if not base_path.exists():
            self.logger.error(f"No '{CITING_DOWNLOADS_DIR}' folder found. Nothing to do.")
            return
        
        for cited_eid_folder in base_path.iterdir():
            if not cited_eid_folder.is_dir():
                continue
            cited_eid = cited_eid_folder.name
            for miscited_eid_folder in cited_eid_folder.iterdir():
                if not miscited_eid_folder.is_dir():
                    continue
                miscited_eid = miscited_eid_folder.name
                csv_file = miscited_eid_folder / f"{miscited_eid}.csv"
                if not csv_file.exists():
                    self.logger.warning(f"CSV file '{csv_file}' does not exist. Skipping.")
                    continue
                
                with open(csv_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        citing_eid = row.get("EID", "").strip()
                        citing_title = row.get("Title", "").strip()
                        citing_link = row.get("Link", "").strip()
                        if citing_eid:
                            self.citing_articles.append({
                                "CitingEID": citing_eid,
                                "CitingTitle": citing_title,
                                "CitingLink": citing_link
                            })
        
        self.logger.info(f"Discovered {len(self.citing_articles)} citing articles total.")

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

    async def scrape_references_of_citing_article(self, context: BrowserContext, citing_article, sem: asyncio.Semaphore):
        async with sem:
            citing_eid = citing_article["CitingEID"]
            citing_title = citing_article["CitingTitle"]
            citing_link = citing_article["CitingLink"]
            
            folder_path = Path(REFERENCES_OF_CITING_DOWNLOADS_DIR) / citing_eid

            try:
                # Check if folder already exists and contains success/empty signals
                if folder_path.exists():
                    success_file = folder_path / "success.txt"
                    empty_file = folder_path / "empty.txt"
                    if success_file.exists() or empty_file.exists():
                        self.logger.info(f"[CitingEID: {citing_eid}] Folder has success/empty. Skipping re-processing.")
                        return
                    else:
                        self.logger.info(f"[CitingEID: {citing_eid}] Folder exists but no success/empty. Re-processing.")
                else:
                    self.logger.info(f"[CitingEID: {citing_eid}] Creating folder: {folder_path}")
                    folder_path.mkdir(parents=True, exist_ok=True)

                # Build the "references of citing" URL
                references_url = (
                    "https://www-scopus-com.ezproxy.cityu.edu.hk/results/references.uri"
                    f"?src=r&sot=rec&s=CITEID({citing_eid.split('-')[2]})&citingId={citing_eid}"
                )

                page = await context.new_page()
                self.logger.info(f"[CitingEID: {citing_eid}] Navigating to: {references_url}")
                await page.goto(references_url, wait_until="networkidle")

                try:
                    # **NEW STEP: Check the number of references before proceeding**
                    try:
                        # Extract the text content
                        page_title_text = await page.locator(".documentHeader span#pageTitleHeader").inner_text()
                        self.logger.info(f"[CitingEID: {citing_eid}] Page Title Text: '{page_title_text}'")

                        # Use regex to extract the number of references
                        match = re.search(r'(\d+)\s+reference', page_title_text.lower())
                        if match:
                            num_references = int(match.group(1))
                            self.logger.info(f"[CitingEID: {citing_eid}] Number of references found: {num_references}")
                        else:
                            self.logger.warning(f"[CitingEID: {citing_eid}] Could not parse number of references from text: '{page_title_text}'")
                            num_references = 0  # Default to 0 if parsing fails

                        if num_references == 0:
                            # No references found
                            empty_file = folder_path / "empty.txt"
                            empty_file.touch(exist_ok=True)
                            self.logger.info(f"[CitingEID: {citing_eid}] No references found. Created empty.txt.")
                            await page.close()
                            return
                        else:
                            self.logger.info(f"[CitingEID: {citing_eid}] References exist. Proceeding to export.")
                    except TimeoutError:
                        self.logger.warning(f"[CitingEID: {citing_eid}] span#pageTitleHeader not found. Assuming no references.")
                        empty_file = folder_path / "empty.txt"
                        empty_file.touch(exist_ok=True)
                        self.logger.info(f"[CitingEID: {citing_eid}] Created empty.txt due to missing page title.")
                        await page.close()
                        return
                    except Exception as e:
                        self.logger.error(f"[CitingEID: {citing_eid}] Error while checking reference count: {e}")
                        empty_file = folder_path / "empty.txt"
                        empty_file.touch(exist_ok=True)
                        self.logger.info(f"[CitingEID: {citing_eid}] Created empty.txt due to error in reference count check.")
                        await page.close()
                        return

                    # **Proceed only if references exist (num_references >=1)**

                    # 1) Check the box to select all references
                    await page.locator("label[for='mainResults-allPageCheckBox']").dispatch_event('click')
                    self.logger.info(f"[CitingEID: {citing_eid}] 'Select all' checkbox checked.")

                    # 2) Export menu
                    await page.locator("button#export_results").click()
                    self.logger.info(f"[CitingEID: {citing_eid}] Export menu opened.")

                    # 3) Check CSV option
                    await page.locator("label[for='CSV']").click()
                    self.logger.info(f"[CitingEID: {citing_eid}] CSV option selected.")

                    # 4) Trigger export and handle download
                    try:
                        async with page.expect_download(timeout=60000) as download_info:
                            await page.locator("button#exportTrigger").click()
                            self.logger.info(f"[CitingEID: {citing_eid}] Export initiated.")
                        download = await download_info.value
                    except TimeoutError:
                        self.logger.info(f"[CitingEID: {citing_eid}] Export timeout, probably due to number of references > 2000.")
                        async with page.expect_download(timeout=60000) as download_info:
                            await page.locator("button#chunkExportTrigger").click()
                            self.logger.info(f"[CitingEID: {citing_eid}] Export the first 2000 documents clicked.")
                        download = await download_info.value

                    # 5) Save CSV
                    csv_path = folder_path / f"{citing_eid}.csv"
                    await download.save_as(str(csv_path))
                    self.logger.info(f"[CitingEID: {citing_eid}] Downloaded CSV saved to {csv_path}.")

                    # Success
                    success_file = folder_path / "success.txt"
                    success_file.touch(exist_ok=True)
                    self.logger.info(f"[CitingEID: {citing_eid}] Created success.txt.")

                except TimeoutError:
                    # Handle scenarios where references are not found through other means
                    # This might be redundant now but kept for robustness
                    no_results_selector = "span[data-testid='no-results-with-suggestion']"
                    try:
                        await page.wait_for_selector(no_results_selector, timeout=5000)
                        empty_file = folder_path / "empty.txt"
                        empty_file.touch(exist_ok=True)
                        self.logger.info(f"[CitingEID: {citing_eid}] No references found via alternative selector. Created empty.txt.")
                    except TimeoutError:
                        self.logger.error(f"[CitingEID: {citing_eid}] Unknown error occurred during processing.")
                except Exception as e:
                    self.logger.error(f"[CitingEID: {citing_eid}] Error during export flow: {e}")
                finally:
                    await page.close()
            except Exception as e:
                self.logger.error(f"[CitingEID: {citing_eid}] Unexpected error: {e}")

    async def run_scraper(self):
        self.discover_all_citing_articles()
        if not self.citing_articles:
            self.logger.info("No citing articles found. Exiting.")
            return
        
        async with async_playwright() as p:
            try:
                browser, context = await self.login_and_get_context(p)
            except Exception as e:
                self.logger.critical(f"Failed to initialize browser context: {e}")
                return
            
            total = len(self.citing_articles)
            for start_index in range(0, total, CHUNK_SIZE):
                chunk = self.citing_articles[start_index : start_index + CHUNK_SIZE]
                sem = asyncio.Semaphore(MAX_CONCURRENCY)
                
                tasks = [
                    asyncio.create_task(self.scrape_references_of_citing_article(context, citing_article, sem))
                    for citing_article in chunk
                ]
                await asyncio.gather(*tasks)

                self.generate_status_csv()
                self.logger.info(f"[Chunk] Processed up to article {start_index + len(chunk)}/{total}.")

            await browser.close()
            self.logger.info("All chunks completed. Browser closed.")
        
        self.generate_status_csv()
        self.logger.info("Final status CSV written.")
        self.generate_overall_csv()

    def generate_status_csv(self):
        """
        For each CitingEID:
         - if folder doesn't exist => "not_started"
         - if success.txt => "success"
         - if empty.txt => "empty"
         - else => "fail"
        """
        rows_status = []
        for citing_article in self.citing_articles:
            citing_eid = citing_article["CitingEID"]
            
            folder_path = Path(REFERENCES_OF_CITING_DOWNLOADS_DIR) / citing_eid
            if not folder_path.exists():
                rows_status.append({
                    "CitingEID": citing_eid,
                    "Status": "not_started"
                })
                continue
            
            success_file = folder_path / "success.txt"
            empty_file = folder_path / "empty.txt"
            if success_file.exists():
                rows_status.append({
                    "CitingEID": citing_eid,
                    "Status": "success"
                })
            elif empty_file.exists():
                rows_status.append({
                    "CitingEID": citing_eid,
                    "Status": "empty"
                })
            else:
                rows_status.append({
                    "CitingEID": citing_eid,
                    "Status": "fail"
                })

        # Write to status.csv inside the output directory
        status_csv_path = Path(REFERENCES_OF_CITING_DOWNLOADS_DIR) / "status.csv"
        status_csv_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = ["CitingEID", "Status"]
        with status_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_status)

        self.logger.info(f"[Status] Wrote {len(rows_status)} rows to {status_csv_path}")

    def generate_overall_csv(self):
        """
        Generate the overall_references_of_citing.csv by aggregating all references
        from each citing article's CSV.
        """
        all_rows = []
        for citing_article in self.citing_articles:
            citing_eid = citing_article["CitingEID"]
            citing_title = citing_article["CitingTitle"]
            citing_link = citing_article["CitingLink"]
            
            citing_csv_path = Path(REFERENCES_OF_CITING_DOWNLOADS_DIR) / citing_eid / f"{citing_eid}.csv"
            if not citing_csv_path.exists():
                self.logger.warning(f"Citing CSV '{citing_csv_path}' does not exist. Skipping.")
                continue

            try:
                referencing_df = pd.read_csv(citing_csv_path)
            except Exception as e:
                self.logger.error(f"Could not read '{citing_csv_path}'. Error: {e}")
                continue

            for _, ref_row in referencing_df.iterrows():
                reference_title = ref_row.get('Title', '')
                reference_eid = ref_row.get('EID', '')
                if pd.isna(reference_eid):
                    continue
                reference_eid = str(reference_eid).strip()
                # Construct the Reference of Citing Article Link
                reference_link = f"https://www.scopus.com/record/display.url?eid={reference_eid}&origin=resultslist"

                all_rows.append({
                    'Citing Article Title': citing_title,
                    'Citing Article EID': citing_eid,
                    'Citing Article Link': citing_link,
                    'Reference Title': reference_title,
                    'Reference EID': reference_eid,
                    'Reference Link': reference_link
                })

        if all_rows:
            output_df = pd.DataFrame(all_rows, columns=[
                'Citing Article Title',
                'Citing Article EID',
                'Citing Article Link',
                'Reference Title',
                'Reference EID',
                'Reference Link'
            ])

            overall_csv_path = Path(OVERALL_REFERENCES_CSV)
            output_df.to_csv(overall_csv_path, index=False)
            self.logger.info(f"Overall references CSV has been created at '{overall_csv_path}'.")
        else:
            self.logger.info("No references found. Overall CSV not created.")


if __name__ == "__main__":
    # Configure centralized logging
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting ReferencesOfCitingScraper.")

    # Initialize LoginManager (no separate log file)
    login_manager = LoginManager()

    # Initialize the scraper with the login manager
    scraper = ReferencesOfCitingScraper(login_manager)

    # Run the scraper
    try:
        asyncio.run(scraper.run_scraper())
    except Exception as e:
        logger.critical(f"Critical error occurred: {e}")

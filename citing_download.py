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

load_dotenv()

SCOPUS_USERNAME = os.getenv("SCOPUS_USERNAME")
SCOPUS_PASSWORD = os.getenv("SCOPUS_PASSWORD")

MISCITED_DOWNLOADS_DIR = "miscited_downloads"
CITING_DOWNLOADS_DIR = "citing_downloads"
LOG_FILE_PATH = Path(CITING_DOWNLOADS_DIR) / "citing_downloads.log"
MAX_CONCURRENCY = 5
CHUNK_SIZE = 100

# Configure global logging
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def query_parser(s, parsing_enabled):
    if not s:
        return ""
    s = s.lower()
    return re.sub(r"[^a-zA-Z0-9]+", " ", s) if parsing_enabled else s


class CitingDocumentsScraper:
    def __init__(self):
        if not SCOPUS_USERNAME or not SCOPUS_PASSWORD:
            raise ValueError("SCOPUS_USERNAME or SCOPUS_PASSWORD not set in environment.")
        
        self.all_pairs = []

    def discover_all_pairs(self):
        base_path = Path(MISCITED_DOWNLOADS_DIR)
        if not base_path.exists():
            logging.error(f"No '{MISCITED_DOWNLOADS_DIR}' folder found. Nothing to do.")
            return
        
        for subfolder in base_path.iterdir():
            if not subfolder.is_dir():
                continue
            cited_eid = subfolder.name
            csv_file = subfolder / f"{cited_eid}.csv"
            if not csv_file.exists():
                continue
            
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    miscited_eid = row.get("EID", "").strip()
                    if miscited_eid:
                        self.all_pairs.append({
                            "CitedEID": cited_eid,
                            "MiscitedEID": miscited_eid
                        })
        
        logging.info(f"Discovered {len(self.all_pairs)} (CitedEID, MiscitedEID) pairs total.")

    async def login_and_get_context(self, playwright):
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        login_url = (
            "https://lbapp01.lib.cityu.edu.hk/ezlogin/index.aspx?"
            "url=https%3a%2f%2fwww.scopus.com"
        )
        redirect_url_pattern = "https://www-scopus-com.ezproxy.cityu.edu.hk/**"

        logging.info("[Login] Navigating to the login page...")
        await page.goto(login_url)

        logging.info("[Login] Filling in username and password...")
        await page.fill('input[name="cred_userid_inputtext"]', SCOPUS_USERNAME)
        await page.fill('input[name="cred_password_inputtext"]', SCOPUS_PASSWORD)

        logging.info("[Login] Clicking the login button...")
        await page.click('input[value="Login"]')

        logging.info("[Login] Waiting for redirect to Scopus EZproxy...")
        await page.wait_for_url(redirect_url_pattern, timeout=60000)
        logging.info(f"[Login] Redirected to: {page.url}")

        await page.close()
        return browser, context

    async def scrape_single_pair(self, context: BrowserContext, pair, sem: asyncio.Semaphore):
        async with sem:
            cited_eid = pair["CitedEID"]
            miscited_eid = pair["MiscitedEID"]
            
            folder_path = Path(CITING_DOWNLOADS_DIR) / cited_eid / miscited_eid

            try:
                # Check if folder already exists and contains success/empty signals
                if folder_path.exists():
                    success_file = folder_path / "success.txt"
                    empty_file = folder_path / "empty.txt"
                    if success_file.exists() or empty_file.exists():
                        logging.info(f"[{cited_eid} -> {miscited_eid}] Folder has success/empty. Skipping re-processing.")
                        return
                    else:
                        logging.info(f"[{cited_eid} -> {miscited_eid}] Folder exists but no success/empty. Re-processing.")
                else:
                    logging.info(f"[{cited_eid} -> {miscited_eid}] Creating folder: {folder_path}")
                    folder_path.mkdir(parents=True, exist_ok=True)

                # Build the "cited by" URL
                citedby_url = (
                    "https://www-scopus-com.ezproxy.cityu.edu.hk/search/submit/citedby.uri"
                    f"?eid={miscited_eid}&src=s&origin=resultslist"
                )

                page = await context.new_page()
                logging.info(f"[{cited_eid} -> {miscited_eid}] Navigating to: {citedby_url}")
                await page.goto(citedby_url, wait_until="networkidle")

                try:
                    # 1) Check the box
                    await page.locator("label[for='mainResults-allPageCheckBox']").dispatch_event('click')
                    logging.info(f"[{cited_eid} -> {miscited_eid}] 'Select all' checkbox checked.")

                    # 2) Export menu
                    await page.locator("button#export_results").click()
                    logging.info(f"[{cited_eid} -> {miscited_eid}] Export menu opened.")

                    # 3) Check CSV
                    await page.locator("label[for='CSV']").click()
                    logging.info(f"[{cited_eid} -> {miscited_eid}] CSV radio selected.")

                    # 4) Export trigger
                    async with page.expect_download(timeout=0) as download_info:
                        await page.locator("button#exportTrigger").click()
                        logging.info(f"[{cited_eid} -> {miscited_eid}] Export submitted.")
                    download = await download_info.value

                    # 5) Save CSV
                    csv_path = folder_path / f"{miscited_eid}.csv"
                    await download.save_as(str(csv_path))
                    logging.info(f"[{cited_eid} -> {miscited_eid}] Downloaded CSV saved to {csv_path}.")

                    # Success
                    success_file = folder_path / "success.txt"
                    success_file.touch(exist_ok=True)
                    logging.info(f"[{cited_eid} -> {miscited_eid}] Created success.txt.")

                except TimeoutError:
                    # Handle no results found
                    no_results_selector = "span[data-testid='no-results-with-suggestion']"
                    try:
                        await page.wait_for_selector(no_results_selector, timeout=500)
                        empty_file = folder_path / "empty.txt"
                        empty_file.touch(exist_ok=True)
                        logging.info(f"[{cited_eid} -> {miscited_eid}] No citing documents found. Created empty.txt.")
                    except TimeoutError:
                        logging.error(f"[{cited_eid} -> {miscited_eid}] Unknown error occurred during processing.")
                except Exception as e:
                    logging.error(f"[{cited_eid} -> {miscited_eid}] Error during export flow: {e}")
                finally:
                    await page.close()
            except Exception as e:
                logging.error(f"[{cited_eid} -> {miscited_eid}] Unexpected error: {e}")

    async def run(self):
        self.discover_all_pairs()
        if not self.all_pairs:
            return
        
        async with async_playwright() as p:
            browser, context = await self.login_and_get_context(p)
            
            total = len(self.all_pairs)
            for start_index in range(0, total, CHUNK_SIZE):
                chunk = self.all_pairs[start_index : start_index + CHUNK_SIZE]
                sem = asyncio.Semaphore(MAX_CONCURRENCY)
                
                tasks = [
                    asyncio.create_task(self.scrape_single_pair(context, pair, sem))
                    for pair in chunk
                ]
                await asyncio.gather(*tasks)

                self.generate_status_csv()
                logging.info(f"[Chunk] Processed up to pair {start_index + len(chunk)}/{total}.")

            await browser.close()
            logging.info("All chunks completed. Browser closed.")

        self.generate_status_csv()
        logging.info("Final status.csv written.")

    def generate_status_csv(self):
        """
        For each (CitedEID, MiscitedEID):
         - if folder doesn't exist => "not_started"
         - if success.txt => success
         - if empty.txt => empty
         - else => fail
        """
        rows_status = []
        for pair in self.all_pairs:
            cited_eid = pair["CitedEID"]
            miscited_eid = pair["MiscitedEID"]
            
            folder_path = Path(CITING_DOWNLOADS_DIR) / cited_eid / miscited_eid
            if not folder_path.exists():
                rows_status.append({
                    "CitedEID": cited_eid,
                    "MiscitedEID": miscited_eid,
                    "Status": "not_started"
                })
                continue
            
            success_file = folder_path / "success.txt"
            empty_file = folder_path / "empty.txt"
            if success_file.exists():
                rows_status.append({
                    "CitedEID": cited_eid,
                    "MiscitedEID": miscited_eid,
                    "Status": "success"
                })
            elif empty_file.exists():
                rows_status.append({
                    "CitedEID": cited_eid,
                    "MiscitedEID": miscited_eid,
                    "Status": "empty"
                })
            else:
                rows_status.append({
                    "CitedEID": cited_eid,
                    "MiscitedEID": miscited_eid,
                    "Status": "fail"
                })

        status_csv_path = Path(CITING_DOWNLOADS_DIR) / "status.csv"
        status_csv_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = ["CitedEID", "MiscitedEID", "Status"]
        with status_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_status)

        logging.info(f"[Status] Wrote {len(rows_status)} rows to {status_csv_path}")


if __name__ == "__main__":
    scraper = CitingDocumentsScraper()
    asyncio.run(scraper.run())

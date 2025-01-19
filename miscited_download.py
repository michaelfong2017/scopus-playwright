import os
import re
import csv
import asyncio
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

INPUT_CSV_PATH = "eid_with_titles.csv"   # Must have columns: EID, Title
DOWNLOADS_DIR = "miscited_downloads"     # Root folder for all EID folders
MAX_CONCURRENCY = 5                    # Number of concurrent pages
CHUNK_SIZE = 100                        # Update status.csv after every 100 EIDs


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
    def __init__(self):
        if not SCOPUS_USERNAME or not SCOPUS_PASSWORD:
            raise ValueError("SCOPUS_USERNAME or SCOPUS_PASSWORD not set in environment.")

    async def login_and_get_context(self, playwright):
        """
        Launch a fresh Chromium browser in headless mode with a custom User-Agent.
        Then perform the EZproxy + Scopus login flow.
        """
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

        print("[Login] Navigating to the login page...")
        await page.goto(login_url)

        print("[Login] Filling in username and password...")
        await page.fill('input[name="cred_userid_inputtext"]', SCOPUS_USERNAME)
        await page.fill('input[name="cred_password_inputtext"]', SCOPUS_PASSWORD)

        print("[Login] Clicking the login button...")
        await page.click('input[value="Login"]')

        print("[Login] Waiting for redirect to Scopus EZproxy...")
        await page.wait_for_url(redirect_url_pattern, timeout=60000)
        print(f"[Login] Redirected to: {page.url}")

        await page.close()
        return browser, context

    def read_input_csv(self):
        """Load EIDs/Titles from CSV into a list of dicts."""
        if not Path(INPUT_CSV_PATH).exists():
            print(f"CSV file '{INPUT_CSV_PATH}' not found.")
            return []
        rows = []
        with open(INPUT_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        print(f"Read {len(rows)} rows from {INPUT_CSV_PATH}.")
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

            # Check if folder already exists and has success/empty
            if folder_path.exists():
                success_file = folder_path / "success.txt"
                empty_file = folder_path / "empty.txt"
                if success_file.exists() or empty_file.exists():
                    print(f"[{eid}] Folder has success/empty => skipping re-processing.")
                    return
                else:
                    print(f"[{eid}] Folder exists but no success/empty => re-processing.")
            else:
                print(f"[{eid}] Creating folder => {folder_path}")
                folder_path.mkdir(parents=True, exist_ok=True)

            # Build the search URL using the parsed query
            parsed_title = query_parser(raw_title, True)   # parse & lowercase => remove non-alnum
            query_text = f'"{parsed_title}"'              # wrap in double-quotes
            # Now put that into Scopus parameter => s=ALL("{parsed_title}")
            search_url = (
                "https://www-scopus-com.ezproxy.cityu.edu.hk/results/results.uri"
                f"?sort=plf-f&src=dm&s=ALL%28{query_text}%29"
                "&limit=10"
                "&sessionSearchId=placeholder"
                "&origin=searchbasic"
                "&sdt=b"
            )

            page = await context.new_page()
            print(f"[{eid}] Navigating to: {search_url}")
            await page.goto(search_url, wait_until="networkidle")

            # Check if 0 documents => e.g. span[data-testid='no-results-with-suggestion']
            no_results_found = False
            try:
                await page.wait_for_selector("span[data-testid='no-results-with-suggestion']", timeout=500)
                no_results_found = True
            except TimeoutError:
                pass  # Means we didn't see the "no results" element => likely some results

            if no_results_found:
                empty_file = folder_path / "empty.txt"
                empty_file.touch(exist_ok=True)
                print(f"[{eid}] 0 documents => created empty.txt")
                await page.close()
                return

            # Attempt the export
            try:
                # Check "Select all"
                await page.locator("input[aria-label='Select all'][type='checkbox']").check()
                print(f"[{eid}] 'Select all' checkbox checked.")

                # Open export menu
                await page.locator(".export-dropdown button").click()
                print(f"[{eid}] Export menu opened.")

                # Click "Export to CSV"
                await page.locator("button[data-testid='export-to-csv']").click()
                print(f"[{eid}] 'Export to CSV' clicked.")

                # Wait for file download
                async with page.expect_download(timeout=0) as download_info:
                    await page.locator("button[data-testid='submit-export-button']").click()
                    print(f"[{eid}] Export submitted.")
                download = await download_info.value

                # Save CSV
                csv_path = folder_path / f"{eid}.csv"
                await download.save_as(str(csv_path))
                print(f"[{eid}] Downloaded CSV => {csv_path}")

                # Mark success
                success_file = folder_path / "success.txt"
                success_file.touch(exist_ok=True)
                print(f"[{eid}] Created success.txt")

            except Exception as e:
                print(f"[{eid}] Error during export flow: {e}")
                # Folder remains with no success/empty => "fail" in final status

            await page.close()

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
            browser, context = await self.login_and_get_context(p)

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
                print(f"[Chunk] Processed up to row {start_index + len(chunk)} / {total}.\n")

            await browser.close()
            print("All chunks completed. Browser closed.")

        # One final status.csv at the end
        self.generate_status_csv(rows)
        print("Final status.csv written.")

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

        print(f"[Status] Wrote status.csv with {len(statuses)} entries => {status_csv_path}")


if __name__ == "__main__":
    scraper = MiscitedDocumentScraper()
    asyncio.run(scraper.run())

import os
import csv
import asyncio
import logging
from functools import partial
from json.decoder import JSONDecodeError
from pathlib import Path
from dotenv import load_dotenv

# Import the LoginManager from login.py
from login import LoginManager

# Load environment variables
load_dotenv()

SCOPUS_VIA_PROXY: bool = os.getenv("SCOPUS_VIA_PROXY")
SCOPUS_BASE_URL = os.getenv("SCOPUS_BASE_URL") if not SCOPUS_VIA_PROXY else os.getenv("SCOPUS_BASE_URL_VIA_PROXY")

class ScopusScraper:
    def __init__(self, login_manager: LoginManager):
        # Initialize logger for this module
        self.logger = logging.getLogger(__name__)

        # Paths and URLs
        self.base_doc_url = (
            f"{SCOPUS_BASE_URL}gateway/doc-details/documents/"
        )

        # CSV input and output
        self.eid_csv_path = "eid.csv"
        self.output_csv_path = "eid_with_titles.csv"

        # Concurrency and chunk settings
        self.concurrency = 20
        self.chunk_size = 100

        # We'll store {EID -> row_dict} for final output
        self.output_data_dict = {}

        # Initialize LoginManager
        self.login_manager = login_manager

    ######################################################
    # 1) RE-LOGIN (WITH COOLDOWN)
    ######################################################
    async def relogin_and_reload_cookies(self):
        """
        Re-logins using LoginManager if enough time has passed since last re-login.
        """
        await self.login_manager.relogin_and_reload_cookies()

    ######################################################
    # 2) LOAD EXISTING OUTPUT (IF ANY)
    ######################################################
    def load_existing_output_csv(self):
        """
        If self.output_csv_path exists, read it into self.output_data_dict
        keyed by EID. We'll skip re-fetching any EID with a non-empty/non-Error Title.
        """
        if not Path(self.output_csv_path).exists():
            return

        with open(self.output_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                eid = row["EID"]
                self.output_data_dict[eid] = row
        self.logger.info(f"Loaded {len(self.output_data_dict)} existing records from {self.output_csv_path}.")

    ######################################################
    # 3) ASYNC FETCH TITLE (WITH RETRY, RE-LOGIN, AND 404 HANDLING)
    ######################################################
    async def async_fetch_title(self, row, sem):
        """
        Given a row with EID (and other fields), fetch its title with up to 5 attempts.
         - If 403 => re-login (unless we just did) => retry
         - If 404 after final attempt => "404 Not Found"
         - Otherwise => "Error" if we exhaust attempts
        Returns (EID, Title).
        """
        async with sem:
            eid = row["EID"]
            url = f"{self.base_doc_url}{eid}"
            last_status = None

            for attempt in range(1, 6):
                try:
                    loop = asyncio.get_running_loop()
                    response = await loop.run_in_executor(
                        None,
                        partial(self.login_manager.get_session().get, url, timeout=10)
                    )
                    last_status = response.status_code

                    # Check 403 => re-login
                    if last_status == 403:
                        self.logger.warning(f"[{eid}] Attempt {attempt}: 403 Forbidden => re-login if needed.")
                        await self.relogin_and_reload_cookies()
                        await asyncio.sleep(1)
                        continue

                    # If 200, parse the JSON
                    if last_status == 200:
                        try:
                            data = response.json()
                        except JSONDecodeError:
                            self.logger.error(f"[{eid}] Attempt {attempt}: JSONDecodeError.")
                            await self.relogin_and_reload_cookies()
                            await asyncio.sleep(1)
                            continue

                        titles = data.get("titles", [])
                        title = titles[0] if titles else "Title not found"
                        self.logger.info(f"[{eid}] => {title} (attempt {attempt})")
                        return (eid, title)
                    elif last_status == 404:
                        self.logger.info(f"[{eid}] => 404 Not Found (attempt {attempt}).")
                        return (eid, "404 Not Found")
                    else:
                        self.logger.warning(f"[{eid}] Attempt {attempt}: HTTP {last_status}, retrying soon...")
                        await asyncio.sleep(1)

                except Exception as e:
                    self.logger.error(f"[{eid}] Attempt {attempt} => Error: {e}")
                    await asyncio.sleep(1)

            # If we exhausted all attempts:
            if last_status == 404:
                self.logger.info(f"[{eid}] => 404 Not Found (after 5 attempts).")
                return (eid, "404 Not Found")
            else:
                self.logger.error(f"[{eid}] => Failed after 5 attempts => 'Error'")
                return (eid, "Error")

    ######################################################
    # 4) SCRAPE TITLES CONCURRENTLY
    ######################################################
    async def scrape_titles_concurrently(self):
        """
        1. Load EIDs from eid.csv (with columns EID, Abstract, Year, etc.).
        2. Skip any EIDs already in output_data_dict with a good Title.
        3. For remaining EIDs, do concurrent fetch in chunks, add Title to output_data_dict.
        4. Save partial progress after each chunk.
        """
        if not Path(self.eid_csv_path).exists():
            self.logger.error(f"CSV file '{self.eid_csv_path}' not found.")
            return

        # Read input CSV
        input_rows = []
        with open(self.eid_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                input_rows.append(row)
        total_input = len(input_rows)
        self.logger.info(f"Found {total_input} rows in {self.eid_csv_path}.")

        # Build a list of rows we need to process
        rows_to_process = []
        for row in input_rows:
            eid = row["EID"]
            existing = self.output_data_dict.get(eid)
            if existing:
                existing_title = existing.get("Title", "")
                if existing_title and existing_title != "Error":
                    # Already have a valid title, skip
                    continue
            rows_to_process.append(row)

        need_count = len(rows_to_process)
        self.logger.info(f"{need_count} EIDs need fetching (others are already done).")

        if not rows_to_process:
            # Nothing to do
            return

        sem = asyncio.Semaphore(self.concurrency)
        processed_count = 0

        # Process in chunks for partial saving
        for start_idx in range(0, need_count, self.chunk_size):
            chunk = rows_to_process[start_idx : start_idx + self.chunk_size]

            tasks = [asyncio.create_task(self.async_fetch_title(r, sem)) for r in chunk]
            results = await asyncio.gather(*tasks)

            # Update self.output_data_dict with newly fetched titles
            for (eid, title) in results:
                if eid in self.output_data_dict:
                    self.output_data_dict[eid]["Title"] = title
                else:
                    # Create or reuse row
                    matching_row = next((x for x in chunk if x["EID"] == eid), None)
                    if not matching_row:
                        matching_row = {"EID": eid}
                    matching_row["Title"] = title
                    self.output_data_dict[eid] = matching_row

            processed_count += len(results)
            self.logger.info(f"Processed {processed_count}/{need_count} needed EIDs. Saving partial results...")
            self.save_output_csv()

        self.logger.info("Finished all needed EIDs. Final save.")
        self.save_output_csv()

    ######################################################
    # 5) SAVE OUTPUT CSV
    ######################################################
    def save_output_csv(self):
        """
        Writes self.output_data_dict to output_csv_path.
        The columns are forced to be: EID, Abstract, Year, Title (in that order).
        """
        if not self.output_data_dict:
            return

        all_rows = list(self.output_data_dict.values())

        # We only keep EID, Abstract, Year, Title in that order
        fieldnames = ["EID", "Abstract", "Year", "Title"]

        with open(self.output_csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)
        self.logger.info(f"Output saved to {self.output_csv_path}.")

    ######################################################
    # 6) MAIN RUN METHOD
    ######################################################
    async def run(self):
        """
        1. Ensure logged in by checking/loading cookies.
        2. Load existing output_data_dict from CSV (if exists).
        3. Do concurrent scraping for new or "Error" EIDs (with 5 retries each).
        4. Re-login on 403, but only if cooldown has passed.
        5. If 404 after all attempts => '404 Not Found' in Title.
        """
        # Ensure logged in
        await self.login_manager.ensure_logged_in()

        # Load existing data from output_csv (if any)
        self.load_existing_output_csv()

        # Scrape titles (with concurrency)
        await self.scrape_titles_concurrently()

if __name__ == "__main__":
    # Configure centralized logging
    logging.basicConfig(
        filename="eid_with_titles.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting ScopusScraper.")

    # Initialize LoginManager (no separate log file)
    login_manager = LoginManager()

    # Initialize the scraper with the login manager
    scraper = ScopusScraper(login_manager)

    # Run the scraper
    try:
        asyncio.run(scraper.run())
    except Exception as e:
        logger.critical(f"Critical error occurred: {e}")

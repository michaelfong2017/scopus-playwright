# Scopus Playwright
1. Create and activate a Python virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate    # macOS/Linux
# or
.\venv\Scripts\activate     # Windows
```

2. Install playwright, python-dotenv:
```bash
pip install playwright python-dotenv requests
pip install pandas
```

3. Install Playwright browsers:
```bash
playwright install
```

4. Place your .env and eid.csv in the project root folder.

5. Run the .py files
```bash
python download_titles.py

python miscited_download.py

python citing_download.py
```

# Using a persistent Chrome window instead
```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir=chrome_data
```

* Note that a folder `chrome_data` will be created or used where the above command is run.

```py
async with async_playwright() as playwright:
    browser = await playwright.chromium.connect_over_cdp("http://localhost:9222")
```

# Steps Explained (Dataset preparation)
## 1. Scrape the title of each publication by EID
- Login required
- Use the requests library for the fastest scraping because a json endpoint is exposed from Scopus
- But that endpoints does not include any reference list or "citedby" list
- eid.csv -> eid_with_titles.csv
- eid_with_titles.log

## 2. Scrape the miscited documents of each cited document by searching title
- Login required
- Browser emulation required because this is the only way to access the "Secondary documents"
- There is no need to filter pairs here by comparing the similarity between each pair of cited document and miscited document. The reason is that the definition of "similar fields" can be changed in the future. It at most takes 1-2 times more effort for a full scraping without such filtering
- eid_with_titles.csv -> miscited_downloads/
- miscited_downloads/miscited_downloads.log

## 3. Scrape the citing documents of each (pair of cited document and) miscited document
- Login required
- Browser emulation is preferred over using the ScopusSearch API because browser emulation is indeed faster. Also, no CityU internal network is required (even CityU VPN cannot work with ScopusSearch API without applying the InstToken from Scopus)
- miscited_downloads/ -> citing_downloads/
- citing_downloads/citing_downloads.log

## 4. Combine all the above scraped information into the miscitation dataset
- Cited Article Title, Cited Article EID, Cited Article Link, Miscited Article Title, Miscited Article EID, Miscited Article Link, Citing Article Title, Citing Article EID, Citing Article Link
- eid_with_titles.csv + miscited_downloads/ + citing_downloads/ -> miscitations_by_eid/ + overall_miscitations.csv

## 5. (Coming Soon) Scrape the references of each citing document
- The purpose is to later check against F07 - Miscited article not found on reference list of citing article
- Login required
- Store in a separate table
- Browser emulation is preferred over using the AbstractRetrieval API for the same reasons explained when scraping the citing documents
- citing_downloads/ -> references_of_citing_download/ + overall_references_of_citing.csv
- references_of_citing_download/references_of_citing_download.log

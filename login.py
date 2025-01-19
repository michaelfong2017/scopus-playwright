import os
import json
from pathlib import Path
import asyncio
import logging
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Load environment variables (SCOPUS_USERNAME, SCOPUS_PASSWORD)
load_dotenv()

USERNAME = os.getenv("SCOPUS_USERNAME")
PASSWORD = os.getenv("SCOPUS_PASSWORD")

# The URL to go through the EZproxy login flow to reach Scopus
LOGIN_URL = (
    "https://lbapp01.lib.cityu.edu.hk/ezlogin/index.aspx?"
    "url=https%3a%2f%2fwww.scopus.com"
)
REDIRECT_URL_PATTERN = "https://www-scopus-com.ezproxy.cityu.edu.hk/**"

# Where to save cookies for later use in Requests
COOKIES_JSON_PATH = "cookies.json"

# Configure logging
logging.basicConfig(
    filename="eid_with_titles.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

async def playwright_login():
    """
    Logs into Scopus via EZproxy using Playwright, then saves cookies to COOKIES_JSON_PATH.
    """
    if not USERNAME or not PASSWORD:
        logging.error("Environment variables SCOPUS_USERNAME or SCOPUS_PASSWORD not set.")
        raise ValueError("Environment variables SCOPUS_USERNAME or SCOPUS_PASSWORD not set.")

    # Launch Playwright (Chromium) in headless=False to visually debug if needed
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            logging.info("Navigating to the login page...")
            await page.goto(LOGIN_URL)

            logging.info("Filling in the login form...")
            await page.fill('input[name=cred_userid_inputtext]', USERNAME)
            await page.fill('input[name=cred_password_inputtext]', PASSWORD)

            logging.info("Submitting the login form...")
            await page.click("css=input[value='Login']")

            logging.info("Waiting for redirect to the Scopus EZproxy URL...")
            await page.wait_for_url(REDIRECT_URL_PATTERN, timeout=60000)
            logging.info(f"Redirected to: {page.url}")

            # Save cookies
            cookies = await context.cookies()
            if cookies:
                with open(COOKIES_JSON_PATH, "w", encoding="utf-8") as f:
                    json.dump(cookies, f, indent=2)
                logging.info(f"Cookies saved to {COOKIES_JSON_PATH}.")
            else:
                logging.warning("No cookies captured; login may have failed.")
        except Exception as e:
            logging.error(f"An error occurred during login: {e}")
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    """
    If you run login.py directly, it will:
     1) Attempt Playwright login
     2) Save cookies to cookies.json
    """
    logging.info("Starting Playwright login process.")
    try:
        asyncio.run(playwright_login())
    except Exception as e:
        logging.critical(f"Critical error occurred: {e}")

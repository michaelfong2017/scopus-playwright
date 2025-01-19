import os
import json
from pathlib import Path
import asyncio
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


async def playwright_login():
    """
    Logs into Scopus via EZproxy using Playwright, then saves cookies to COOKIES_JSON_PATH.
    """
    if not USERNAME or not PASSWORD:
        raise ValueError(
            "Environment variables SCOPUS_USERNAME or SCOPUS_PASSWORD not set."
        )

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
            print("Navigating to the login page...")
            await page.goto(LOGIN_URL)

            print("Filling in the login form...")
            await page.fill('input[name=cred_userid_inputtext]', USERNAME)
            await page.fill('input[name=cred_password_inputtext]', PASSWORD)

            print("Submitting the login form...")
            await page.click("css=input[value='Login']")

            print("Waiting for redirect to the Scopus EZproxy URL...")
            await page.wait_for_url(REDIRECT_URL_PATTERN, timeout=60000)
            print(f"Redirected to: {page.url}")

            # Save cookies
            cookies = await context.cookies()
            if cookies:
                with open(COOKIES_JSON_PATH, "w", encoding="utf-8") as f:
                    json.dump(cookies, f, indent=2)
                print(f"Cookies saved to {COOKIES_JSON_PATH}.")
            else:
                print("No cookies captured; login may have failed.")
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    """
    If you run login.py directly, it will:
     1) Attempt Playwright login
     2) Save cookies to cookies.json
    """
    asyncio.run(playwright_login())

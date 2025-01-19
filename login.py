import os
import json
import time
import asyncio
import logging
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright
import requests
from requests.cookies import RequestsCookieJar

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

# Default path to save cookies for later use in Requests
DEFAULT_COOKIES_JSON_PATH = "cookies.json"

class LoginManager:
    def __init__(
        self,
        cookies_json_path=DEFAULT_COOKIES_JSON_PATH,
        relogin_cooldown=30.0,
    ):
        """
        Initializes the LoginManager.

        Args:
            cookies_json_path (str): Path to save/load cookies.
            relogin_cooldown (float): Cooldown period in seconds between re-logins.
        """
        self.cookies_json_path = cookies_json_path
        self.relogin_cooldown = relogin_cooldown
        self.last_relogin_time = 0.0
        self.relogin_lock = asyncio.Lock()

        # Initialize logger for this module
        self.logger = logging.getLogger(__name__)

        # Initialize a Requests session with a browser-like User-Agent
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        })

        # Variable to store Playwright-specific cookies in memory
        self.playwright_cookies = []

    async def playwright_login(self):
        """
        Logs into Scopus via EZproxy using Playwright, then saves cookies to self.cookies_json_path.
        """
        if not USERNAME or not PASSWORD:
            self.logger.error("Environment variables SCOPUS_USERNAME or SCOPUS_PASSWORD not set.")
            raise ValueError("Environment variables SCOPUS_USERNAME or SCOPUS_PASSWORD not set.")

        # Launch Playwright (Chromium) in headless mode
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
                self.logger.info("Navigating to the login page...")
                await page.goto(LOGIN_URL)

                self.logger.info("Filling in the login form...")
                await page.fill('input[name=cred_userid_inputtext]', USERNAME)
                await page.fill('input[name=cred_password_inputtext]', PASSWORD)

                self.logger.info("Submitting the login form...")
                await page.click("css=input[value='Login']")

                self.logger.info("Waiting for redirect to the Scopus EZproxy URL...")
                await page.wait_for_url(REDIRECT_URL_PATTERN, timeout=60000)
                self.logger.info(f"Redirected to: {page.url}")

                # Save cookies
                cookies = await context.cookies()
                if cookies:
                    self.playwright_cookies = cookies

                    with open(self.cookies_json_path, "w", encoding="utf-8") as f:
                        json.dump(cookies, f, indent=2)
                    self.logger.info(f"Cookies saved to {self.cookies_json_path}.")
                else:
                    self.logger.warning("No cookies captured; login may have failed.")
            except Exception as e:
                self.logger.error(f"An error occurred during login: {e}")
                raise
            finally:
                await context.close()
                await browser.close()

    def load_cookies_to_session(self):
        """
        Loads cookies from self.cookies_json_path into the session's cookie jar.
        """
        if not Path(self.cookies_json_path).exists():
            self.logger.error("Cookie file not found. Please login first.")
            raise FileNotFoundError("Cookie file not found. Please login first.")

        with open(self.cookies_json_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)

        jar = RequestsCookieJar()
        for cookie in cookies:
            jar.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain", ""),
                path=cookie.get("path", "/")
            )

        self.session.cookies = jar
        self.logger.info("Cookies loaded into session.")

    async def relogin_and_reload_cookies(self):
        """
        Re-logins using Playwright if enough time has passed since the last re-login.
        Protected by self.relogin_lock to ensure only one re-login occurs at a time.
        """
        async with self.relogin_lock:
            now = time.time()
            if (now - self.last_relogin_time) < self.relogin_cooldown:
                self.logger.info("Skipping re-login (cooldown not reached).")
                return

            self.logger.info("Attempting to re-login via Playwright...")
            await self.playwright_login()
            self.load_cookies_to_session()
            self.last_relogin_time = time.time()
            self.logger.info("Re-login completed.")
            return self.playwright_cookies

    def get_session(self):
        """
        Returns the current Requests session with loaded cookies.
        """
        if not self.session.cookies or not self.session.cookies:
            self.load_cookies_to_session()
        return self.session

    async def ensure_logged_in(self):
        """
        Ensures that the session is logged in by checking if cookies are loaded.
        If not, performs login.
        """
        if not Path(self.cookies_json_path).exists():
            self.logger.info("No cookies found. Performing initial login.")
            await self.playwright_login()
        self.load_cookies_to_session()

if __name__ == "__main__":
    """
    If you run login.py directly, it will:
     1) Attempt Playwright login
     2) Save cookies to cookies.json
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting Playwright login process.")
    login_manager = LoginManager()
    try:
        asyncio.run(login_manager.playwright_login())
    except Exception as e:
        logger.critical(f"Critical error occurred: {e}")

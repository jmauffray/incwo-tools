import os
import requests
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class XMLDownloader:
    """Handles downloading XML files from Incwo."""

    BASE_URL = "https://www.incwo.com/555938/customer_products.xml"

    def __init__(self, auth: Optional[str], output_dir: str, dry_run: bool = False):
        self.auth = auth
        self.output_dir = output_dir
        self.dry_run = dry_run
        self.username = None
        self.password = None

        if auth:
            if ':' in auth:
                self.username, self.password = auth.split(':', 1)
            else:
                logger.error("Error: INCWO_AUTH must be in format username:password")
        elif not dry_run:
             logger.warning("No authentication provided. Downloads will likely fail.")

    def fetch_and_save_data(self, max_pages: int):
        """Downloads XML files for each page up to max_pages."""
        if not self.auth and not self.dry_run:
            logger.error("Cannot download without authentication.")
            return

        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except OSError as e:
            logger.error(f"Error creating directory {self.output_dir}: {e}")
            return

        for page_number in range(1, max_pages + 1):
            filename = os.path.join(self.output_dir, f"customer_products_{page_number}.xml")

            if os.path.exists(filename):
                logger.info(f"File {filename} already exists. Skipping.")
                continue

            logger.info(f"Downloading page {page_number}...")
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would download {self.BASE_URL}?page={page_number} to {filename}")
                continue

            self._download_page(page_number, filename)
            
            # Best Practice: Add a small delay between requests
            time.sleep(0.5)

    def _download_page(self, page_number: int, filename: str):
        """Downloads a single page and saves it to a file."""
        url = f"{self.BASE_URL}?page={page_number}"
        
        try:
            response = requests.get(
                url,
                auth=(self.username, self.password), 
                headers={'Content-Type': 'application/xml'}
            )

            response.raise_for_status()

            with open(filename, 'wb') as f:
                f.write(response.content)

            logger.info(f"Successfully saved page {page_number} to {filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"*** ERROR on page {page_number}: {e} ***")

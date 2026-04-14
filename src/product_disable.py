import requests
import logging
from typing import List, Optional
from product import Product

logger = logging.getLogger(__name__)

INCWO_API_URL = "https://www.incwo.com/555938/customer_products/"

class ProductDisable:
    """Handles the deletion of products via the Incwo API."""

    def __init__(self, auth: Optional[str], dry_run: bool = False):
        self.dry_run = dry_run
        self.username = None
        self.password = None
        
        if auth:
            if ':' in auth:
                self.username, self.password = auth.split(':', 1)
            else:
                logger.error("Error: INCWO_AUTH must be in format username:password")
        elif not dry_run:
             logger.warning("No authentication provided. Deletions will likely fail.")

    def disable_products(self, products_to_disable: List[Product]) -> None:
        """Disable the specified products."""
        if not products_to_disable:
            logger.info("No products to disable.")
            return

        logger.info(f"Found {len(products_to_disable)} products to disable.")

        if self.dry_run:
            logger.info("DRY RUN MODE: No disables will be performed.")

        success_count = 0
        fail_count = 0

        for p in products_to_disable:
            if self._disable_single_product(p):
                success_count += 1
            else:
                fail_count += 1

        if not self.dry_run:
            logger.info(f"Disabling complete. Success: {success_count}, Failed: {fail_count}")

    def disable_products_from_factux(self, factux_products_to_disable: List[str],
                                     incwo_products: List[Product]) -> None:
        """Disables the specified products."""
        if not factux_products_to_disable:
            logger.info("No products to disable.")
            return

        logger.info(f"Found {len(factux_products_to_disable)} products to disable.")

        if self.dry_run:
            logger.info("DRY RUN MODE: No disables will be performed.")

        success_count = 0
        fail_count = 0

        for p in incwo_products:
            #print(p.id)
            if int(p.reference) in factux_products_to_disable:
                if self._disable_single_product(p):
                    success_count += 1
                else:
                    fail_count += 1

        if not self.dry_run:
            logger.info(f"Disabling complete. Success: {success_count}, Failed: {fail_count}")

    def _disable_single_product(self, product: Product) -> bool:
        """Disables a single product. Returns True if successful."""
        del_url = f"{INCWO_API_URL}{product.id}.xml"
        msg = f"DISABLE {del_url} (Ref: {product.reference})"

        if self.dry_run:
            logger.info(f"[DRY RUN] {msg}")
            return True

        logger.info(msg)

        if not self.username or not self.password:
            return False

        try:
            #res = requests.delete(del_url, auth=(self.username, self.password), timeout=10)
            
            res = requests.put(del_url,
                                data="<customer_product><is_active>0</is_active></customer_product>",
                                headers={"Content-Type": "application/xml"},
                                auth=(self.username, self.password),
                                timeout=10)
            if res.status_code in [200, 204]:
                logger.info(f"  ✅ Disabled {product.id}")
                return True
            else:
                logger.error(f"  ❌ Failed {product.id}: {res.status_code} {res.text}")
                return False
        except requests.RequestException as e:
            logger.error(f"  ❌ Network Error {product.id}: {e}")
            return False
        except Exception as e:
            logger.error(f"  ❌ Error {product.id}: {e}")
            return False

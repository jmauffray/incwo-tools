from collections import defaultdict
from typing import List
import logging
from product import Product

logger = logging.getLogger(__name__)


class DuplicateFinder:
    """Logic for identifying duplicate products."""

    def find_empty_reference(self, products: List[Product]) -> List[Product]:
        """
        Identifies empty reference in products.
        Returns a list of products that should be disabled.
        """
        none_reference = []
        for p in products:
            if not p.reference:
                logger.warning(f"None reference: {p.id} {p.reference}")
                none_reference.append(p)

        return none_reference

    def find_duplicates(self, products: List[Product]) -> List[Product]:
        """
        Identifies duplicate products based on reference.
        Returns a list of products that should be deleted.
        """
        prods_by_ref = defaultdict(list)
        for p in products:
            if not p.reference:
                continue
            prods_by_ref[p.reference].append(p)

        duplicates_to_delete = []
        logger.info(f"Total unique references: {len(prods_by_ref)}")

        for ref, product_list in prods_by_ref.items():
            if len(product_list) > 1:
                duplicates_to_delete.extend(self._resolve_duplicates(product_list))

        return duplicates_to_delete

    def _resolve_duplicates(self, product_list: List[Product]) -> List[Product]:
        """
        Given a list of products with the same reference, determines which one is the master
        and returns the rest as duplicates to be deleted.
        """
        # Find master: Most recent modified_at, then is_active, then ID
        master = max(product_list, key=lambda p: (
            p.modified_date,
            p.is_active,
            p.id
        ))

        return [p for p in product_list if p.id != master.id]

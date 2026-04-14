from dataclasses import dataclass
from datetime import datetime
from typing import Dict
import logging

logger = logging.getLogger(__name__)

DATE_FORMAT = '%d-%m-%Y-%H-%M'

@dataclass
class Product:
    """Represents a product from the Incwo system."""
    id: str
    reference: str
    name: str
    is_active: str
    modified_at: str

    @property
    def modified_date(self) -> datetime:
        """Parses the modified_at string into a datetime object."""
        try:
            return datetime.strptime(self.modified_at, DATE_FORMAT)
        except ValueError:
            logger.debug(f"Failed to parse date: {self.modified_at}")
            return datetime.min

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'Product':
        """Creates a Product instance from a dictionary."""
        return cls(
            id=data.get('id', ''),
            reference=data.get('reference', ''),
            name=data.get('name', ''),
            is_active=data.get('is_active', '0'),
            modified_at=data.get('modified_at', '')
        )

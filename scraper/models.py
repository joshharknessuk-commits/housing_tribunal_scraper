from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class DocumentRecord:
    """Represents a single tribunal decision document discovered on the web."""

    case_id: str
    title: str
    document_url: str
    tribunal: str
    decision_date: Optional[datetime]
    region: Optional[str] = None
    category: Optional[str] = None
    listing_url: Optional[str] = None
    metadata: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        """Return a serialisable representation suitable for JSON/DB insertion."""

        return {
            "case_id": self.case_id,
            "title": self.title,
            "document_url": self.document_url,
            "tribunal": self.tribunal,
            "decision_date": self.decision_date.isoformat() if self.decision_date else None,
            "region": self.region,
            "category": self.category,
            "listing_url": self.listing_url,
            "metadata": self.metadata,
        }

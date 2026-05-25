"""Pydantic models for Yelp Open Dataset records."""

from __future__ import annotations

import json
import re
from typing import Any

try:
    from pydantic import BaseModel, Field  # noqa: F401
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field  # type: ignore[no-redef]
    _PYDANTIC_V2 = False

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class YelpReview(BaseModel):
    """Represents a single Yelp review record.

    Attributes:
        review_id: Unique 22-character review identifier.
        user_id: Unique 22-character user identifier.
        business_id: Unique 22-character business identifier.
        stars: Star rating (1–5).
        useful: Number of useful votes.
        funny: Number of funny votes.
        cool: Number of cool votes.
        text: Review text body.
        date: Review date string (YYYY-MM-DD).
    """

    review_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    business_id: str = Field(..., min_length=1)
    stars: float = Field(..., ge=1.0, le=5.0)
    useful: int = Field(default=0, ge=0)
    funny: int = Field(default=0, ge=0)
    cool: int = Field(default=0, ge=0)
    text: str = Field(default="")
    date: str = Field(default="")

    def __repr__(self) -> str:
        return (
            f"YelpReview(review_id={self.review_id!r}, "
            f"business_id={self.business_id!r}, stars={self.stars})"
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "YelpReview":
        """Construct from a raw JSON-decoded dict."""
        return cls(**data)

    def to_json(self, indent: int | None = None) -> str:
        """Serialise this review to a JSON string."""
        return json.dumps(self.model_dump() if hasattr(self, "model_dump") else self.dict(), indent=indent, ensure_ascii=False)

    def word_count(self) -> int:
        """Return the number of whitespace-separated words in the review text."""
        return len(self.text.split()) if self.text else 0

    def date_is_valid(self) -> bool:
        """Return True if date matches YYYY-MM-DD format."""
        return bool(_DATE_RE.match(self.date))

    def total_votes(self) -> int:
        """Return the sum of useful + funny + cool votes."""
        return self.useful + self.funny + self.cool

    def is_positive(self) -> bool:
        """Return True if stars >= 4."""
        return self.stars >= 4.0

    def is_negative(self) -> bool:
        """Return True if stars <= 2."""
        return self.stars <= 2.0


class YelpBusiness(BaseModel):
    """Represents a Yelp business record.

    Attributes:
        business_id: Unique 22-character business identifier.
        name: Business name.
        city: City the business is located in.
        state: Two-letter US state code or equivalent.
        stars: Average star rating.
        review_count: Total number of reviews.
        is_open: 1 if the business is open, 0 if closed.
        categories: Comma-separated category string, or None.
        latitude: Geographic latitude, or None.
        longitude: Geographic longitude, or None.
    """

    business_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    city: str = Field(default="")
    state: str = Field(default="")
    stars: float = Field(..., ge=1.0, le=5.0)
    review_count: int = Field(default=0, ge=0)
    is_open: int = Field(default=1)
    categories: str | None = Field(default=None)
    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)

    def __repr__(self) -> str:
        return (
            f"YelpBusiness(business_id={self.business_id!r}, "
            f"name={self.name!r}, stars={self.stars})"
        )

    def to_json(self, indent: int | None = None) -> str:
        """Serialise this business to a JSON string."""
        return json.dumps(self.model_dump() if hasattr(self, "model_dump") else self.dict(), indent=indent, ensure_ascii=False)

    def category_list(self) -> list[str]:
        """Return categories split into a list, or empty list if None."""
        if not self.categories:
            return []
        return [c.strip() for c in self.categories.split(",") if c.strip()]

    def is_highly_rated(self) -> bool:
        """Return True if average stars >= 4.0."""
        return self.stars >= 4.0

    def has_coordinates(self) -> bool:
        """Return True if both latitude and longitude are set."""
        return self.latitude is not None and self.longitude is not None


class YelpUser(BaseModel):
    """Represents a Yelp user record.

    Attributes:
        user_id: Unique 22-character user identifier.
        name: User display name.
        review_count: Total reviews written.
        average_stars: Average star rating across all reviews.
        useful: Total useful votes received.
        funny: Total funny votes received.
        cool: Total cool votes received.
        fans: Number of fans.
    """

    user_id: str = Field(..., min_length=1)
    name: str = Field(default="")
    review_count: int = Field(default=0, ge=0)
    average_stars: float = Field(default=0.0, ge=0.0, le=5.0)
    useful: int = Field(default=0, ge=0)
    funny: int = Field(default=0, ge=0)
    cool: int = Field(default=0, ge=0)
    fans: int = Field(default=0, ge=0)

    def __repr__(self) -> str:
        return (
            f"YelpUser(user_id={self.user_id!r}, "
            f"name={self.name!r}, review_count={self.review_count})"
        )

    def to_json(self, indent: int | None = None) -> str:
        """Serialise this user to a JSON string."""
        return json.dumps(self.model_dump() if hasattr(self, "model_dump") else self.dict(), indent=indent, ensure_ascii=False)

    def total_votes_received(self) -> int:
        """Return the sum of useful + funny + cool votes received."""
        return self.useful + self.funny + self.cool

    def is_elite_candidate(self) -> bool:
        """Return True if user has many reviews and high rating."""
        return self.review_count >= 100 and self.average_stars >= 4.0


class YelpCheckin(BaseModel):
    """Represents a Yelp check-in record.

    Attributes:
        business_id: Business where check-ins occurred.
        date: Comma-separated list of check-in datetime strings.
    """

    business_id: str = Field(..., min_length=1)
    date: str = Field(default="")

    def __repr__(self) -> str:
        return f"YelpCheckin(business_id={self.business_id!r}, count={self.checkin_count()})"

    def checkin_count(self) -> int:
        """Return the number of individual check-in timestamps."""
        if not self.date:
            return 0
        return len([d for d in self.date.split(",") if d.strip()])

    def to_json(self, indent: int | None = None) -> str:
        """Serialise this check-in record to a JSON string."""
        return json.dumps(self.model_dump() if hasattr(self, "model_dump") else self.dict(), indent=indent, ensure_ascii=False)


class YelpPhoto(BaseModel):
    """Represents a Yelp photo record.

    Attributes:
        photo_id: Unique photo identifier.
        business_id: Business the photo belongs to.
        caption: Photo caption text.
        label: Photo category label (e.g. 'food', 'inside', 'outside').
    """

    photo_id: str = Field(..., min_length=1)
    business_id: str = Field(..., min_length=1)
    caption: str = Field(default="")
    label: str = Field(default="")

    def __repr__(self) -> str:
        return (
            f"YelpPhoto(photo_id={self.photo_id!r}, "
            f"business_id={self.business_id!r}, label={self.label!r})"
        )

    def has_caption(self) -> bool:
        """Return True if the photo has a non-empty caption."""
        return bool(self.caption.strip())

    def to_json(self, indent: int | None = None) -> str:
        """Serialise this photo record to a JSON string."""
        return json.dumps(self.model_dump() if hasattr(self, "model_dump") else self.dict(), indent=indent, ensure_ascii=False)

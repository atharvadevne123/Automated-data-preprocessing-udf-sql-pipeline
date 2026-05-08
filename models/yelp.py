"""Pydantic models for Yelp Open Dataset records."""

from __future__ import annotations

from typing import Any

try:
    from pydantic import BaseModel, Field  # noqa: F401
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field  # type: ignore[no-redef]
    _PYDANTIC_V2 = False


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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "YelpReview":
        """Construct from a raw JSON-decoded dict."""
        return cls(**data)

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
    """

    business_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    city: str = Field(default="")
    state: str = Field(default="")
    stars: float = Field(..., ge=1.0, le=5.0)
    review_count: int = Field(default=0, ge=0)
    is_open: int = Field(default=1)
    categories: str | None = Field(default=None)

    def category_list(self) -> list[str]:
        """Return categories split into a list, or empty list if None."""
        if not self.categories:
            return []
        return [c.strip() for c in self.categories.split(",") if c.strip()]

    def is_highly_rated(self) -> bool:
        """Return True if average stars >= 4.0."""
        return self.stars >= 4.0


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

    def is_elite_candidate(self) -> bool:
        """Return True if user has many reviews and high rating."""
        return self.review_count >= 100 and self.average_stars >= 4.0

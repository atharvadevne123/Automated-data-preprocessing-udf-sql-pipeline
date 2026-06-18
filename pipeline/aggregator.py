"""Statistics aggregator for Yelp pipeline records."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BusinessStats:
    """Accumulated statistics for a single business.

    Attributes:
        business_id: The business identifier.
        review_count: Total reviews seen.
        star_sum: Sum of all star ratings.
        useful_sum: Sum of useful votes.
        positive_count: Reviews with stars >= 4.
        negative_count: Reviews with stars <= 2.
    """

    business_id: str
    review_count: int = 0
    star_sum: float = 0.0
    useful_sum: int = 0
    positive_count: int = 0
    negative_count: int = 0

    @property
    def average_stars(self) -> float:
        """Mean star rating; 0.0 if no reviews."""
        return self.star_sum / self.review_count if self.review_count > 0 else 0.0

    def merge(self, other: BusinessStats) -> None:
        """Merge *other* into this instance in-place.

        Args:
            other: Another BusinessStats for the same business_id.
        """
        self.review_count += other.review_count
        self.star_sum += other.star_sum
        self.useful_sum += other.useful_sum
        self.positive_count += other.positive_count
        self.negative_count += other.negative_count

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary dict."""
        return {
            "business_id": self.business_id,
            "review_count": self.review_count,
            "average_stars": round(self.average_stars, 3),
            "useful_sum": self.useful_sum,
            "positive_count": self.positive_count,
            "negative_count": self.negative_count,
        }


@dataclass
class GlobalStats:
    """Pipeline-wide aggregated statistics.

    Attributes:
        total_records: Total records processed.
        star_distribution: Count of records per integer star rating (1-5).
        sentiment_counts: Count per sentiment label.
    """

    total_records: int = 0
    star_distribution: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    sentiment_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary dict."""
        return {
            "total_records": self.total_records,
            "star_distribution": dict(self.star_distribution),
            "sentiment_counts": dict(self.sentiment_counts),
        }


class StatsAggregator:
    """Accumulate per-business and global statistics from processed records.

    Example::

        agg = StatsAggregator()
        for record in records:
            agg.add(record)
        summary = agg.global_stats().to_dict()
    """

    def __init__(self) -> None:
        self._businesses: dict[str, BusinessStats] = {}
        self._global = GlobalStats()

    def _update_business(self, bid: str, stars: float, useful: int) -> None:
        """Create or update per-business stats for *bid*.

        Args:
            bid: Business identifier string (non-empty).
            stars: Star rating for this review.
            useful: Useful vote count for this review.
        """
        if bid not in self._businesses:
            self._businesses[bid] = BusinessStats(business_id=bid)
        bstats = self._businesses[bid]
        bstats.review_count += 1
        bstats.star_sum += stars
        bstats.useful_sum += useful
        if stars >= 4.0:
            bstats.positive_count += 1
        elif stars <= 2.0:
            bstats.negative_count += 1

    def add(self, record: dict[str, Any]) -> None:
        """Ingest a single record and update all accumulators.

        Args:
            record: Dict that may contain ``business_id``, ``stars``,
                ``useful``, and ``sentiment_label`` keys.
        """
        self._global.total_records += 1

        stars = float(record.get("stars", 0))
        star_key = max(1, min(5, int(round(stars))))
        self._global.star_distribution[star_key] += 1

        label = record.get("sentiment_label", "unknown")
        self._global.sentiment_counts[label] += 1

        bid = record.get("business_id", "")
        if bid:
            self._update_business(bid, stars, int(record.get("useful", 0)))

    def add_batch(self, records: list[dict[str, Any]]) -> None:
        """Ingest a list of records.

        Args:
            records: List of record dicts.
        """
        for record in records:
            self.add(record)

    def global_stats(self) -> GlobalStats:
        """Return accumulated global statistics."""
        return self._global

    def business_stats(self, business_id: str) -> BusinessStats | None:
        """Return stats for a specific business, or None if not seen.

        Args:
            business_id: Business identifier to look up.
        """
        return self._businesses.get(business_id)

    def top_businesses(self, n: int = 10) -> list[BusinessStats]:
        """Return the top-N businesses by average star rating (min 5 reviews).

        Args:
            n: Maximum number of businesses to return.

        Returns:
            Sorted list of BusinessStats, highest average stars first.
        """
        eligible = [b for b in self._businesses.values() if b.review_count >= 5]
        eligible.sort(key=lambda b: b.average_stars, reverse=True)
        return eligible[:n]

    def all_business_stats(self) -> list[dict[str, Any]]:
        """Return all business stats as a list of dicts."""
        return [b.to_dict() for b in self._businesses.values()]

    def filter_businesses(
        self,
        min_reviews: int = 0,
        min_avg_stars: float = 0.0,
        max_avg_stars: float = 5.0,
    ) -> list[BusinessStats]:
        """Return businesses matching the given criteria.

        Args:
            min_reviews: Minimum number of reviews required (inclusive).
            min_avg_stars: Minimum average star rating (inclusive).
            max_avg_stars: Maximum average star rating (inclusive).

        Returns:
            List of matching BusinessStats sorted by average stars descending.
        """
        result = [
            b
            for b in self._businesses.values()
            if b.review_count >= min_reviews and min_avg_stars <= b.average_stars <= max_avg_stars
        ]
        result.sort(key=lambda b: b.average_stars, reverse=True)
        return result

    def median_stars(self) -> float:
        """Return the median star rating across all processed records.

        Returns:
            Median value, or 0.0 if no records have been added.
        """
        dist = self._global.star_distribution
        if not dist:
            return 0.0
        stars_list: list[float] = []
        for star, count in dist.items():
            stars_list.extend([float(star)] * count)
        stars_list.sort()
        n = len(stars_list)
        mid = n // 2
        if n % 2 == 1:
            return stars_list[mid]
        return (stars_list[mid - 1] + stars_list[mid]) / 2.0

    def mean_stars(self) -> float:
        """Return the mean star rating across all processed records."""
        dist = self._global.star_distribution
        if not dist:
            return 0.0
        total = sum(star * count for star, count in dist.items())
        n = sum(dist.values())
        return total / n if n > 0 else 0.0

    def merge(self, other: StatsAggregator) -> None:
        """Merge all statistics from *other* into this aggregator in-place.

        Useful for combining results from parallel aggregation workers.

        Args:
            other: Another StatsAggregator whose data will be folded in.
        """
        self._global.total_records += other._global.total_records
        for star, count in other._global.star_distribution.items():
            self._global.star_distribution[star] += count
        for label, count in other._global.sentiment_counts.items():
            self._global.sentiment_counts[label] += count
        for bid, bstats in other._businesses.items():
            if bid in self._businesses:
                self._businesses[bid].merge(bstats)
            else:
                self._businesses[bid] = bstats

    def percentile_stars(self, p: float) -> float:
        """Return the *p*-th percentile star rating across all processed records.

        Args:
            p: Percentile in [0, 100].

        Returns:
            Percentile value, or 0.0 if no records have been added.

        Raises:
            ValueError: If *p* is not in [0, 100].
        """
        if not (0.0 <= p <= 100.0):
            raise ValueError(f"percentile p must be in [0, 100], got {p}")
        dist = self._global.star_distribution
        if not dist:
            return 0.0
        stars_list: list[float] = []
        for star, count in dist.items():
            stars_list.extend([float(star)] * count)
        stars_list.sort()
        n = len(stars_list)
        if n == 0:
            return 0.0
        idx = (p / 100.0) * (n - 1)
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return stars_list[lo] * (1 - frac) + stars_list[hi] * frac

    def stddev_stars(self) -> float:
        """Return the population standard deviation of star ratings.

        Returns:
            Standard deviation, or 0.0 if fewer than 2 records have been added.
        """
        dist = self._global.star_distribution
        if not dist:
            return 0.0
        total_count = sum(dist.values())
        if total_count < 2:
            return 0.0
        mean = self.mean_stars()
        variance = sum(count * (float(star) - mean) ** 2 for star, count in dist.items()) / total_count
        return variance**0.5

    def reset(self) -> None:
        """Clear all accumulated state."""
        self._businesses.clear()
        self._global = GlobalStats()
        logger.info("StatsAggregator reset.")

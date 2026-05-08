"""Tests for YelpReview, YelpBusiness, and YelpUser Pydantic models."""

from __future__ import annotations

import pytest

from models.yelp import YelpBusiness, YelpReview, YelpUser


class TestYelpReview:
    def _make(self, **kwargs):
        defaults = {"review_id": "r1", "user_id": "u1", "business_id": "b1", "stars": 3.0}
        return YelpReview(**{**defaults, **kwargs})

    def test_basic_creation(self):
        r = self._make()
        assert r.review_id == "r1"
        assert r.stars == 3.0

    @pytest.mark.parametrize("stars,expected", [(5.0, True), (4.0, True), (3.0, False), (1.0, False)])
    def test_is_positive(self, stars, expected):
        assert self._make(stars=stars).is_positive() == expected

    @pytest.mark.parametrize("stars,expected", [(1.0, True), (2.0, True), (3.0, False), (5.0, False)])
    def test_is_negative(self, stars, expected):
        assert self._make(stars=stars).is_negative() == expected

    def test_total_votes(self):
        r = self._make(useful=3, funny=2, cool=1)
        assert r.total_votes() == 6

    def test_total_votes_default_zero(self):
        assert self._make().total_votes() == 0

    def test_from_dict(self):
        data = {"review_id": "abc", "user_id": "u1", "business_id": "b1", "stars": 4.5}
        r = YelpReview.from_dict(data)
        assert r.review_id == "abc"
        assert r.stars == 4.5

    def test_stars_boundary_low(self):
        r = self._make(stars=1.0)
        assert r.stars == 1.0

    def test_stars_boundary_high(self):
        r = self._make(stars=5.0)
        assert r.stars == 5.0

    def test_invalid_stars_too_low(self):
        with pytest.raises(Exception):
            self._make(stars=0.5)

    def test_invalid_stars_too_high(self):
        with pytest.raises(Exception):
            self._make(stars=5.5)

    def test_text_defaults_empty(self):
        assert self._make().text == ""

    def test_date_defaults_empty(self):
        assert self._make().date == ""


class TestYelpBusiness:
    def _make(self, **kwargs):
        defaults = {"business_id": "b1", "name": "Test Biz", "stars": 4.0}
        return YelpBusiness(**{**defaults, **kwargs})

    def test_basic_creation(self):
        b = self._make()
        assert b.business_id == "b1"

    def test_category_list_splits_correctly(self):
        b = self._make(categories="Pizza, Italian, Fast Food")
        cats = b.category_list()
        assert len(cats) == 3
        assert "Pizza" in cats

    def test_category_list_empty_when_none(self):
        b = self._make(categories=None)
        assert b.category_list() == []

    def test_category_list_empty_string(self):
        b = self._make(categories="")
        assert b.category_list() == []

    @pytest.mark.parametrize("stars,expected", [(4.0, True), (5.0, True), (3.9, False), (1.0, False)])
    def test_is_highly_rated(self, stars, expected):
        assert self._make(stars=stars).is_highly_rated() == expected

    def test_is_open_default(self):
        assert self._make().is_open == 1

    def test_review_count_default(self):
        assert self._make().review_count == 0


class TestYelpUser:
    def _make(self, **kwargs):
        defaults = {"user_id": "u1"}
        return YelpUser(**{**defaults, **kwargs})

    def test_basic_creation(self):
        u = self._make()
        assert u.user_id == "u1"

    @pytest.mark.parametrize(
        "count,avg,expected",
        [(100, 4.0, True), (99, 4.5, False), (200, 3.9, False), (150, 4.5, True)],
    )
    def test_is_elite_candidate(self, count, avg, expected):
        u = self._make(review_count=count, average_stars=avg)
        assert u.is_elite_candidate() == expected

    def test_fans_default_zero(self):
        assert self._make().fans == 0

    def test_name_default_empty(self):
        assert self._make().name == ""

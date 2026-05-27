"""Second set of extended tests for Pydantic Yelp models — validation/edge cases."""

from __future__ import annotations

import pytest

from models.yelp import YelpBusiness, YelpReview, YelpUser


class TestYelpReviewValidation:
    @pytest.mark.parametrize("stars", [1, 2, 3, 4, 5, 1.0, 4.5, 5.0])
    def test_valid_stars_accepted(self, stars):
        r = YelpReview(review_id="r1", user_id="u1", business_id="b1", stars=stars)
        assert r.stars == float(stars)

    @pytest.mark.parametrize("stars", [0, 0.9, 5.1, 6, -1])
    def test_invalid_stars_rejected(self, stars):
        with pytest.raises(Exception):
            YelpReview(review_id="r1", user_id="u1", business_id="b1", stars=stars)

    def test_defaults_for_optional_fields(self):
        r = YelpReview(review_id="r1", user_id="u1", business_id="b1", stars=3)
        assert r.useful == 0
        assert r.funny == 0
        assert r.cool == 0
        assert r.text == ""
        assert r.date == ""

    def test_from_dict_round_trip(self):
        data = {
            "review_id": "abc123",
            "user_id": "user1",
            "business_id": "biz1",
            "stars": 4,
            "useful": 2,
            "funny": 1,
            "cool": 0,
            "text": "Great place",
            "date": "2023-01-15",
        }
        r = YelpReview.from_dict(data)
        assert r.review_id == "abc123"
        assert r.useful == 2
        assert r.text == "Great place"

    @pytest.mark.parametrize(
        "stars,is_pos,is_neg",
        [
            (5.0, True, False),
            (4.0, True, False),
            (3.0, False, False),
            (2.0, False, True),
            (1.0, False, True),
        ],
    )
    def test_sentiment_flags(self, stars, is_pos, is_neg):
        r = YelpReview(review_id="r", user_id="u", business_id="b", stars=stars)
        assert r.is_positive() == is_pos
        assert r.is_negative() == is_neg

    @pytest.mark.parametrize(
        "useful,funny,cool,expected_total",
        [
            (0, 0, 0, 0),
            (1, 2, 3, 6),
            (10, 0, 5, 15),
            (100, 100, 100, 300),
        ],
    )
    def test_total_votes(self, useful, funny, cool, expected_total):
        r = YelpReview(
            review_id="r",
            user_id="u",
            business_id="b",
            stars=3,
            useful=useful,
            funny=funny,
            cool=cool,
        )
        assert r.total_votes() == expected_total


class TestYelpBusinessValidation:
    def test_basic_construction(self):
        b = YelpBusiness(business_id="b1", name="Cafe X", stars=4.0)
        assert b.name == "Cafe X"

    @pytest.mark.parametrize("stars", [1.0, 2.5, 3.0, 4.5, 5.0])
    def test_valid_stars(self, stars):
        b = YelpBusiness(business_id="b1", name="X", stars=stars)
        assert b.stars == stars

    def test_is_highly_rated_true(self):
        b = YelpBusiness(business_id="b1", name="Top", stars=4.5, review_count=50)
        assert b.is_highly_rated() is True

    def test_is_highly_rated_false_low_stars(self):
        b = YelpBusiness(business_id="b1", name="Low", stars=3.0, review_count=50)
        assert b.is_highly_rated() is False

    def test_category_list_with_categories(self):
        b = YelpBusiness(business_id="b1", name="X", stars=4.0, categories="Italian, Pizza, Restaurants")
        cats = b.category_list()
        assert "Italian" in cats
        assert "Pizza" in cats

    def test_category_list_none_returns_empty(self):
        b = YelpBusiness(business_id="b1", name="X", stars=4.0, categories=None)
        assert b.category_list() == []


class TestYelpUserValidation:
    def test_basic_construction(self):
        u = YelpUser(user_id="u1", name="Alice", review_count=10, average_stars=4.0)
        assert u.name == "Alice"

    @pytest.mark.parametrize(
        "review_count,avg_stars,expected",
        [
            (100, 4.0, True),
            (100, 4.5, True),
            (99, 4.5, False),
            (200, 3.9, False),
            (0, 5.0, False),
        ],
    )
    def test_is_elite_candidate(self, review_count, avg_stars, expected):
        u = YelpUser(
            user_id="u1",
            name="Bob",
            review_count=review_count,
            average_stars=avg_stars,
        )
        assert u.is_elite_candidate() == expected

    def test_defaults(self):
        u = YelpUser(user_id="u1", name="Bob", review_count=5, average_stars=3.0)
        assert u.useful == 0
        assert u.funny == 0
        assert u.cool == 0
        assert u.fans == 0

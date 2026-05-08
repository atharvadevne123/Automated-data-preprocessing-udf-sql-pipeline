"""Extended parametrized tests for Yelp Pydantic models."""

from __future__ import annotations

import pytest

from models.yelp import YelpBusiness, YelpReview, YelpUser


class TestYelpReviewEdgeCases:
    @pytest.mark.parametrize("stars", [1.5, 2.3, 3.7, 4.1, 4.9])
    def test_fractional_stars_accepted(self, stars):
        r = YelpReview(review_id="r", user_id="u", business_id="b", stars=stars)
        assert r.stars == stars

    @pytest.mark.parametrize("useful,funny,cool,expected", [
        (0, 0, 0, 0), (1, 0, 0, 1), (5, 3, 2, 10), (100, 50, 25, 175)
    ])
    def test_total_votes_various(self, useful, funny, cool, expected):
        r = YelpReview(review_id="r", user_id="u", business_id="b", stars=3.0,
                       useful=useful, funny=funny, cool=cool)
        assert r.total_votes() == expected

    def test_from_dict_with_extra_fields_ignored(self):
        data = {
            "review_id": "r1", "user_id": "u1", "business_id": "b1",
            "stars": 3.0, "extra_field": "should be ignored",
        }
        r = YelpReview.from_dict({k: v for k, v in data.items() if k != "extra_field"})
        assert r.review_id == "r1"

    @pytest.mark.parametrize("stars,pos,neg", [
        (5.0, True, False), (4.0, True, False), (3.0, False, False),
        (2.0, False, True), (1.0, False, True),
    ])
    def test_pos_neg_consistency(self, stars, pos, neg):
        r = YelpReview(review_id="r", user_id="u", business_id="b", stars=stars)
        assert r.is_positive() == pos
        assert r.is_negative() == neg


class TestYelpBusinessEdgeCases:
    @pytest.mark.parametrize("cats,expected_len", [
        ("Pizza", 1),
        ("Pizza, Italian", 2),
        ("Pizza, Italian, Fast Food, Delivery", 4),
        (None, 0),
        ("", 0),
        ("  Sushi  ,  Japanese  ", 2),
    ])
    def test_category_list_length(self, cats, expected_len):
        b = YelpBusiness(business_id="b", name="Test", stars=3.0, categories=cats)
        assert len(b.category_list()) == expected_len

    @pytest.mark.parametrize("stars", [1.0, 2.5, 3.0, 4.5, 5.0])
    def test_various_stars_accepted(self, stars):
        b = YelpBusiness(business_id="b", name="Test", stars=stars)
        assert b.stars == stars

    def test_is_open_flag(self):
        b_open = YelpBusiness(business_id="b", name="Test", stars=3.0, is_open=1)
        b_closed = YelpBusiness(business_id="b", name="Test", stars=3.0, is_open=0)
        assert b_open.is_open == 1
        assert b_closed.is_open == 0


class TestYelpUserEdgeCases:
    @pytest.mark.parametrize("count,avg,expected", [
        (0, 5.0, False), (100, 3.9, False), (99, 4.5, False), (100, 4.0, True),
        (1000, 5.0, True), (100, 4.0, True),
    ])
    def test_elite_candidate_boundary(self, count, avg, expected):
        u = YelpUser(user_id="u", review_count=count, average_stars=avg)
        assert u.is_elite_candidate() == expected

    def test_all_fields_default_zero(self):
        u = YelpUser(user_id="u")
        assert u.review_count == 0
        assert u.useful == 0
        assert u.funny == 0
        assert u.cool == 0
        assert u.fans == 0
        assert u.average_stars == 0.0

    def test_name_can_be_set(self):
        u = YelpUser(user_id="u", name="Alice")
        assert u.name == "Alice"

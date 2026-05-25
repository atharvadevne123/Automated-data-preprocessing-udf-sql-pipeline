"""Tests for new model features: __repr__, to_json(), word_count(), YelpCheckin, YelpPhoto."""

from __future__ import annotations

import json

import pytest

from models.yelp import YelpBusiness, YelpCheckin, YelpPhoto, YelpReview, YelpUser


class TestYelpReviewEnhancements:
    def make_review(self, **kwargs):
        defaults = {
            "review_id": "r1",
            "user_id": "u1",
            "business_id": "b1",
            "stars": 4.0,
            "text": "Great food and service!",
            "date": "2023-06-15",
        }
        defaults.update(kwargs)
        return YelpReview(**defaults)

    def test_repr_contains_review_id(self):
        r = self.make_review()
        assert "r1" in repr(r)

    def test_repr_contains_stars(self):
        r = self.make_review(stars=5.0)
        assert "5.0" in repr(r)

    def test_to_json_is_valid(self):
        r = self.make_review()
        parsed = json.loads(r.to_json())
        assert parsed["review_id"] == "r1"

    def test_to_json_indent(self):
        r = self.make_review()
        s = r.to_json(indent=2)
        assert "\n" in s

    def test_word_count(self):
        r = self.make_review(text="Great food and service!")
        assert r.word_count() == 4

    def test_word_count_empty(self):
        r = self.make_review(text="")
        assert r.word_count() == 0

    def test_date_is_valid_true(self):
        r = self.make_review(date="2023-06-15")
        assert r.date_is_valid() is True

    def test_date_is_valid_false(self):
        r = self.make_review(date="June 15 2023")
        assert r.date_is_valid() is False

    @pytest.mark.parametrize("text,expected", [
        ("one", 1), ("one two three", 3), ("", 0),
    ])
    def test_word_count_parametrized(self, text, expected):
        r = self.make_review(text=text)
        assert r.word_count() == expected


class TestYelpBusinessEnhancements:
    def make_biz(self, **kwargs):
        defaults = {"business_id": "biz1", "name": "Café A", "stars": 4.0}
        defaults.update(kwargs)
        return YelpBusiness(**defaults)

    def test_repr_contains_name(self):
        b = self.make_biz()
        assert "Café A" in repr(b)

    def test_to_json_is_valid(self):
        b = self.make_biz()
        parsed = json.loads(b.to_json())
        assert parsed["business_id"] == "biz1"

    def test_has_coordinates_true(self):
        b = self.make_biz(latitude=37.7, longitude=-122.4)
        assert b.has_coordinates() is True

    def test_has_coordinates_false(self):
        b = self.make_biz()
        assert b.has_coordinates() is False


class TestYelpUserEnhancements:
    def make_user(self, **kwargs):
        defaults = {"user_id": "u1", "name": "Alice"}
        defaults.update(kwargs)
        return YelpUser(**defaults)

    def test_repr_contains_user_id(self):
        u = self.make_user()
        assert "u1" in repr(u)

    def test_to_json_is_valid(self):
        u = self.make_user()
        parsed = json.loads(u.to_json())
        assert parsed["user_id"] == "u1"

    def test_total_votes_received(self):
        u = self.make_user(useful=3, funny=1, cool=2)
        assert u.total_votes_received() == 6


class TestYelpCheckin:
    def test_checkin_count_empty(self):
        c = YelpCheckin(business_id="b1", date="")
        assert c.checkin_count() == 0

    def test_checkin_count_single(self):
        c = YelpCheckin(business_id="b1", date="2023-01-01 10:00:00")
        assert c.checkin_count() == 1

    def test_checkin_count_multiple(self):
        c = YelpCheckin(business_id="b1", date="2023-01-01, 2023-01-02, 2023-01-03")
        assert c.checkin_count() == 3

    def test_repr(self):
        c = YelpCheckin(business_id="b1", date="2023-01-01")
        assert "b1" in repr(c)

    def test_to_json(self):
        c = YelpCheckin(business_id="b1", date="2023-01-01")
        parsed = json.loads(c.to_json())
        assert parsed["business_id"] == "b1"


class TestYelpPhoto:
    def test_has_caption_true(self):
        p = YelpPhoto(photo_id="p1", business_id="b1", caption="Delicious burger")
        assert p.has_caption() is True

    def test_has_caption_false(self):
        p = YelpPhoto(photo_id="p1", business_id="b1", caption="")
        assert p.has_caption() is False

    def test_repr(self):
        p = YelpPhoto(photo_id="p1", business_id="b1", label="food")
        assert "p1" in repr(p)

    def test_to_json(self):
        p = YelpPhoto(photo_id="p1", business_id="b1")
        parsed = json.loads(p.to_json())
        assert parsed["photo_id"] == "p1"

    @pytest.mark.parametrize("label", ["food", "inside", "outside", "drink", "menu"])
    def test_label_parametrized(self, label):
        p = YelpPhoto(photo_id="p1", business_id="b1", label=label)
        assert p.label == label

"""Tests for YelpTip model in models/yelp.py."""

from __future__ import annotations

import json

import pytest

from models.yelp import YelpTip


class TestYelpTip:
    def _make_tip(self, **kwargs: object) -> YelpTip:
        defaults = {
            "user_id": "user_001",
            "business_id": "biz_001",
            "text": "Great place!",
            "date": "2024-01-15",
            "compliment_count": 3,
        }
        defaults.update(kwargs)
        return YelpTip(**defaults)  # type: ignore[arg-type]

    def test_creation(self) -> None:
        tip = self._make_tip()
        assert tip.user_id == "user_001"
        assert tip.business_id == "biz_001"

    def test_default_text(self) -> None:
        tip = YelpTip(user_id="u", business_id="b")
        assert tip.text == ""

    def test_default_compliment_count(self) -> None:
        tip = YelpTip(user_id="u", business_id="b")
        assert tip.compliment_count == 0

    def test_word_count(self) -> None:
        tip = self._make_tip(text="Hello world foo")
        assert tip.word_count() == 3

    def test_word_count_empty(self) -> None:
        tip = self._make_tip(text="")
        assert tip.word_count() == 0

    def test_is_valid_date_true(self) -> None:
        tip = self._make_tip(date="2024-06-17")
        assert tip.is_valid_date()

    def test_is_valid_date_false(self) -> None:
        tip = self._make_tip(date="not-a-date")
        assert not tip.is_valid_date()

    def test_from_dict(self) -> None:
        data = {"user_id": "u", "business_id": "b", "text": "ok", "date": "2023-01-01"}
        tip = YelpTip.from_dict(data)
        assert tip.text == "ok"

    def test_to_json(self) -> None:
        tip = self._make_tip()
        raw = tip.to_json()
        parsed = json.loads(raw)
        assert parsed["user_id"] == "user_001"

    def test_repr(self) -> None:
        tip = self._make_tip()
        r = repr(tip)
        assert "YelpTip" in r
        assert "user_001" in r

    def test_negative_compliment_raises(self) -> None:
        with pytest.raises(Exception):
            YelpTip(user_id="u", business_id="b", compliment_count=-1)

    def test_empty_user_id_raises(self) -> None:
        with pytest.raises(Exception):
            YelpTip(user_id="", business_id="b")

    def test_to_json_indent(self) -> None:
        tip = self._make_tip()
        raw = tip.to_json(indent=2)
        assert "\n" in raw

    @pytest.mark.parametrize("text,expected_count", [("one", 1), ("one two", 2), ("", 0), ("a b c d e", 5)])
    def test_word_count_parametrized(self, text: str, expected_count: int) -> None:
        tip = self._make_tip(text=text)
        assert tip.word_count() == expected_count

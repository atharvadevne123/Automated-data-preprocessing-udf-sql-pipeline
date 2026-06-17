"""Tests for --sections flag in scripts/generate_report.py."""

from __future__ import annotations

import json

import pytest

from pipeline.aggregator import StatsAggregator
from scripts.generate_report import _ALL_SECTIONS, build_report


def _make_agg(n: int = 3) -> StatsAggregator:
    agg = StatsAggregator()
    for i in range(n):
        agg.add({"business_id": f"biz{i}", "stars": (i % 5) + 1, "text": "review"})
    return agg


class TestBuildReportSections:
    def test_default_includes_all_sections(self) -> None:
        agg = _make_agg()
        report = build_report(agg)
        assert set(report.keys()) == _ALL_SECTIONS

    def test_summary_only(self) -> None:
        agg = _make_agg()
        report = build_report(agg, sections={"summary"})
        assert "summary" in report
        assert "star_distribution" not in report
        assert "top_businesses" not in report

    def test_star_distribution_only(self) -> None:
        agg = _make_agg()
        report = build_report(agg, sections={"star_distribution"})
        assert "star_distribution" in report
        assert "summary" not in report

    def test_top_businesses_only(self) -> None:
        agg = _make_agg()
        report = build_report(agg, sections={"top_businesses"})
        assert "top_businesses" in report
        assert "summary" not in report

    def test_two_sections(self) -> None:
        agg = _make_agg()
        report = build_report(agg, sections={"summary", "star_distribution"})
        assert "summary" in report
        assert "star_distribution" in report
        assert "top_businesses" not in report

    def test_empty_sections(self) -> None:
        agg = _make_agg()
        report = build_report(agg, sections=set())
        assert report == {}

    def test_summary_has_required_keys(self) -> None:
        agg = _make_agg(5)
        report = build_report(agg, sections={"summary"})
        assert "total_records" in report["summary"]
        assert "unique_businesses" in report["summary"]
        assert "average_reviews_per_business" in report["summary"]

    def test_top_n_respected(self) -> None:
        agg = _make_agg(10)
        report = build_report(agg, top_n=2, sections={"top_businesses"})
        assert len(report["top_businesses"]) <= 2

    @pytest.mark.parametrize("section", sorted(_ALL_SECTIONS))
    def test_each_section_individually(self, section: str) -> None:
        agg = _make_agg()
        report = build_report(agg, sections={section})
        assert section in report

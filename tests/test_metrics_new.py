"""Tests for ValidationMetrics and PipelineRunMetrics in utils.metrics."""

from __future__ import annotations

import pytest

from utils.metrics import PipelineRunMetrics, ValidationMetrics


class TestValidationMetrics:
    def test_default_values(self):
        v = ValidationMetrics()
        assert v.total == 0
        assert v.valid == 0
        assert v.invalid == 0
        assert v.skipped == 0

    def test_error_rate_zero_when_no_records(self):
        assert ValidationMetrics().error_rate == 0.0

    def test_error_rate_calculated(self):
        v = ValidationMetrics(total=10, valid=8, invalid=2)
        assert v.error_rate == pytest.approx(0.2)

    def test_valid_rate_calculated(self):
        v = ValidationMetrics(total=10, valid=8, invalid=2)
        assert v.valid_rate == pytest.approx(0.8)

    def test_valid_rate_zero_when_no_records(self):
        assert ValidationMetrics().valid_rate == 0.0

    def test_to_dict_has_all_keys(self):
        v = ValidationMetrics(total=5, valid=4, invalid=1)
        d = v.to_dict()
        assert "total" in d
        assert "valid" in d
        assert "invalid" in d
        assert "skipped" in d
        assert "error_rate" in d
        assert "valid_rate" in d

    def test_to_dict_values(self):
        v = ValidationMetrics(total=100, valid=90, invalid=10, skipped=5)
        d = v.to_dict()
        assert d["total"] == 100
        assert d["error_rate"] == pytest.approx(0.1)

    @pytest.mark.parametrize(
        "total,invalid,expected",
        [
            (10, 0, 0.0),
            (10, 10, 1.0),
            (10, 5, 0.5),
            (0, 0, 0.0),
        ],
    )
    def test_error_rate_parametrized(self, total, invalid, expected):
        v = ValidationMetrics(total=total, invalid=invalid)
        assert v.error_rate == pytest.approx(expected)


class TestPipelineRunMetrics:
    def test_default_values(self):
        p = PipelineRunMetrics(pipeline_name="test")
        assert p.input_records == 0
        assert p.output_records == 0
        assert p.elapsed_sec == 0.0

    def test_throughput_zero_when_no_elapsed(self):
        p = PipelineRunMetrics(pipeline_name="t", output_records=100, elapsed_sec=0.0)
        assert p.throughput == 0.0

    def test_throughput_calculated(self):
        p = PipelineRunMetrics(pipeline_name="t", output_records=1000, elapsed_sec=2.0)
        assert p.throughput == pytest.approx(500.0)

    def test_drop_rate_zero_when_no_input(self):
        p = PipelineRunMetrics(pipeline_name="t")
        assert p.drop_rate == 0.0

    def test_drop_rate_calculated(self):
        p = PipelineRunMetrics(pipeline_name="t", input_records=100, output_records=80)
        assert p.drop_rate == pytest.approx(0.2)

    def test_drop_rate_zero_when_all_kept(self):
        p = PipelineRunMetrics(pipeline_name="t", input_records=50, output_records=50)
        assert p.drop_rate == 0.0

    def test_to_dict_has_all_keys(self):
        p = PipelineRunMetrics(pipeline_name="test", input_records=100, output_records=90)
        d = p.to_dict()
        assert "pipeline_name" in d
        assert "input_records" in d
        assert "output_records" in d
        assert "throughput_records_per_sec" in d
        assert "drop_rate" in d
        assert "validation" in d

    def test_validation_nested_in_to_dict(self):
        vm = ValidationMetrics(total=100, valid=90, invalid=10)
        p = PipelineRunMetrics(pipeline_name="t", validation=vm)
        d = p.to_dict()
        assert d["validation"]["total"] == 100

    def test_pipeline_name_in_dict(self):
        p = PipelineRunMetrics(pipeline_name="my_pipeline")
        assert p.to_dict()["pipeline_name"] == "my_pipeline"

    @pytest.mark.parametrize(
        "in_r,out_r,expected_drop",
        [
            (100, 100, 0.0),
            (100, 0, 1.0),
            (100, 50, 0.5),
        ],
    )
    def test_drop_rate_parametrized(self, in_r, out_r, expected_drop):
        p = PipelineRunMetrics(pipeline_name="t", input_records=in_r, output_records=out_r)
        assert p.drop_rate == pytest.approx(expected_drop)

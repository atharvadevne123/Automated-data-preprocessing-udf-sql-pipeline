"""Tests for newly added validators: date, business_id, coordinates."""

from __future__ import annotations

import pytest

from utils.validators import (
    ValidationError,
    validate_business_id_format,
    validate_coordinates,
    validate_date_format,
)


class TestValidateDateFormat:
    def test_valid_date(self):
        assert validate_date_format("2023-01-15") == "2023-01-15"

    def test_invalid_format_no_dashes(self):
        with pytest.raises(ValidationError):
            validate_date_format("20230115")

    def test_invalid_format_wrong_separator(self):
        with pytest.raises(ValidationError):
            validate_date_format("2023/01/15")

    def test_empty_string(self):
        with pytest.raises(ValidationError):
            validate_date_format("")

    def test_non_string(self):
        with pytest.raises(ValidationError):
            validate_date_format(20230115)  # type: ignore[arg-type]

    def test_partial_date(self):
        with pytest.raises(ValidationError):
            validate_date_format("2023-01")

    @pytest.mark.parametrize("date", ["2020-02-29", "2023-12-31", "2000-01-01"])
    def test_valid_dates_parametrized(self, date):
        assert validate_date_format(date) == date


class TestValidateBusinessIdFormat:
    def test_valid_id(self):
        assert validate_business_id_format("abc123") == "abc123"

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError):
            validate_business_id_format("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValidationError):
            validate_business_id_format("   ")

    def test_non_string_raises(self):
        with pytest.raises(ValidationError):
            validate_business_id_format(None)  # type: ignore[arg-type]

    @pytest.mark.parametrize("bid", ["id1", "ABCDEFGHIJKLMNOPQRSTUV", "xyz-123"])
    def test_valid_ids_parametrized(self, bid):
        assert validate_business_id_format(bid) == bid


class TestValidateCoordinates:
    def test_valid_coordinates(self):
        lat, lon = validate_coordinates(37.7749, -122.4194)
        assert lat == pytest.approx(37.7749)
        assert lon == pytest.approx(-122.4194)

    def test_latitude_too_large(self):
        with pytest.raises(ValidationError):
            validate_coordinates(91.0, 0.0)

    def test_latitude_too_small(self):
        with pytest.raises(ValidationError):
            validate_coordinates(-91.0, 0.0)

    def test_longitude_too_large(self):
        with pytest.raises(ValidationError):
            validate_coordinates(0.0, 181.0)

    def test_longitude_too_small(self):
        with pytest.raises(ValidationError):
            validate_coordinates(0.0, -181.0)

    def test_non_numeric_raises(self):
        with pytest.raises(ValidationError):
            validate_coordinates("not", "numeric")  # type: ignore[arg-type]

    def test_boundary_values(self):
        assert validate_coordinates(90.0, 180.0) == (90.0, 180.0)
        assert validate_coordinates(-90.0, -180.0) == (-90.0, -180.0)

    @pytest.mark.parametrize("lat,lon", [(0, 0), (45, 90), (-45, -90)])
    def test_parametrized_valid(self, lat, lon):
        result_lat, result_lon = validate_coordinates(lat, lon)
        assert result_lat == lat
        assert result_lon == lon

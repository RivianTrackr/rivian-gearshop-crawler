"""Tests for availability.py — HTML/JSON-LD availability inference."""

import json
from unittest.mock import patch, MagicMock

import pytest

from availability import (
    infer_availability_from_html,
    get_avail_html_checks,
    reset_avail_state,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset global state before each test."""
    reset_avail_state()
    yield
    reset_avail_state()


def _make_html(json_ld_data=None, button_text="Add to Cart"):
    """Build a minimal product page HTML with optional JSON-LD and button."""
    parts = ["<html><head>"]
    if json_ld_data is not None:
        parts.append(
            f'<script type="application/ld+json">{json.dumps(json_ld_data)}</script>'
        )
    parts.append("</head><body>")
    parts.append(f'<button name="add" type="submit">{button_text}</button>')
    parts.append("</body></html>")
    return "".join(parts)


def _mock_response(html, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.text = html
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    return resp


class TestInferAvailabilityFromHtml:
    @patch("availability.requests.get")
    def test_in_stock_from_json_ld(self, mock_get):
        json_ld = {
            "@type": "Product",
            "offers": [{
                "url": "https://gearshop.rivian.com/products/test?variant=111",
                "availability": "https://schema.org/InStock",
            }],
        }
        mock_get.return_value = _mock_response(_make_html(json_ld))

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is True

    @patch("availability.requests.get")
    def test_out_of_stock_from_json_ld(self, mock_get):
        json_ld = {
            "@type": "Product",
            "offers": [{
                "url": "https://gearshop.rivian.com/products/test?variant=111",
                "availability": "https://schema.org/OutOfStock",
            }],
        }
        mock_get.return_value = _mock_response(_make_html(json_ld))

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is False

    @patch("availability.requests.get")
    def test_sold_out_button(self, mock_get):
        mock_get.return_value = _mock_response(
            _make_html(json_ld_data=None, button_text="Sold Out")
        )

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is False

    @patch("availability.requests.get")
    def test_notify_me_button(self, mock_get):
        mock_get.return_value = _mock_response(
            _make_html(json_ld_data=None, button_text="Notify Me When Available")
        )

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is False

    @patch("availability.requests.get")
    def test_no_data_returns_none(self, mock_get):
        mock_get.return_value = _mock_response("<html><body>No data</body></html>")

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is None

    @patch("availability.requests.get")
    def test_caching(self, mock_get):
        mock_get.return_value = _mock_response(
            _make_html(json_ld_data=None, button_text="Sold Out")
        )

        # First call
        result1 = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        # Second call should use cache
        result2 = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )

        assert result1 == result2
        assert mock_get.call_count == 1  # Only one HTTP request

    @patch("availability.requests.get")
    def test_html_check_counter(self, mock_get):
        mock_get.return_value = _mock_response("<html><body></body></html>")

        assert get_avail_html_checks() == 0
        infer_availability_from_html("p1", 1, "https://example.com", {})
        assert get_avail_html_checks() == 1
        infer_availability_from_html("p2", 2, "https://example.com", {})
        assert get_avail_html_checks() == 2

    @patch("availability.requests.get")
    def test_http_error_returns_none(self, mock_get):
        mock_get.side_effect = Exception("Connection refused")

        result = infer_availability_from_html(
            "test", 111, "https://gearshop.rivian.com", {}
        )
        assert result is None

    def test_reset_state(self):
        reset_avail_state()
        assert get_avail_html_checks() == 0


class TestAllOffersOutOfStock:
    """Test when all offers in JSON-LD are out of stock."""

    @patch("availability.requests.get")
    def test_all_out_of_stock(self, mock_get):
        json_ld = {
            "@type": "Product",
            "offers": [
                {"availability": "https://schema.org/OutOfStock"},
                {"availability": "https://schema.org/OutOfStock"},
            ],
        }
        mock_get.return_value = _mock_response(_make_html(json_ld))

        result = infer_availability_from_html(
            "test", 999, "https://gearshop.rivian.com", {}
        )
        assert result is False

    @patch("availability.requests.get")
    def test_any_in_stock(self, mock_get):
        json_ld = {
            "@type": "Product",
            "offers": [
                {"availability": "https://schema.org/OutOfStock"},
                {"availability": "https://schema.org/InStock"},
            ],
        }
        mock_get.return_value = _mock_response(_make_html(json_ld))

        result = infer_availability_from_html(
            "test", 999, "https://gearshop.rivian.com", {}
        )
        assert result is True

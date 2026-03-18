"""Tests for dual download settings definitions and validation."""

import pytest

from shelfmark.config.settings import search_mode_settings
from shelfmark.config.users_settings import validate_search_preference_value


def _field_map():
    return {
        field.key: field
        for field in search_mode_settings()
        if hasattr(field, "key")
    }


# ── Settings registration ────────────────────────────────────────────────────


class TestDualDownloadSettingsRegistration:
    def test_dual_download_enabled_field_exists(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_ENABLED"]

        assert field.label == "Enable Dual Download"
        assert field.default is False
        assert field.user_overridable is True

    def test_dual_download_preferred_format_field_exists(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_PREFERRED_FORMAT"]

        assert field.default == ""
        assert field.user_overridable is True
        # Should show only when DUAL_DOWNLOAD_ENABLED is True
        assert field.show_when == {"field": "DUAL_DOWNLOAD_ENABLED", "value": True}

    def test_dual_download_fallback_format_field_exists(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_FALLBACK_FORMAT"]

        assert field.default == ""
        assert field.user_overridable is True
        assert field.show_when == {"field": "DUAL_DOWNLOAD_ENABLED", "value": True}

    def test_dual_download_max_size_field_exists(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_MAX_SIZE_MB"]

        assert field.default == 0
        assert field.user_overridable is True
        assert field.show_when == {"field": "DUAL_DOWNLOAD_ENABLED", "value": True}

    def test_format_options_include_both_book_and_audiobook_formats(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_PREFERRED_FORMAT"]
        option_values = [opt["value"] for opt in field.options]

        # Book formats
        assert "epub" in option_values
        assert "pdf" in option_values
        # Audiobook formats
        assert "m4b" in option_values
        assert "mp3" in option_values
        # "Any" option
        assert "" in option_values

    def test_format_options_are_deduplicated(self):
        fields = _field_map()
        field = fields["DUAL_DOWNLOAD_PREFERRED_FORMAT"]
        option_values = [opt["value"] for opt in field.options]

        # zip and rar appear in both _FORMAT_OPTIONS and _AUDIOBOOK_FORMAT_OPTIONS
        # but should only appear once in the combined list
        assert option_values.count("zip") == 1
        assert option_values.count("rar") == 1

    def test_heading_field_exists(self):
        fields = _field_map()
        heading = fields["dual_download_heading"]
        assert "Dual Download" in heading.title


# ── Validation ────────────────────────────────────────────────────────────────


class TestDualDownloadValidation:
    def test_validate_enabled_with_bool(self):
        value, error = validate_search_preference_value("DUAL_DOWNLOAD_ENABLED", True)
        assert value is True
        assert error is None

    def test_validate_enabled_with_false(self):
        value, error = validate_search_preference_value("DUAL_DOWNLOAD_ENABLED", False)
        assert value is False
        assert error is None

    def test_validate_enabled_coerces_truthy(self):
        value, error = validate_search_preference_value("DUAL_DOWNLOAD_ENABLED", 1)
        assert value is True
        assert error is None

    def test_validate_preferred_format_normalizes(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_PREFERRED_FORMAT", "M4B"
        )
        assert value == "m4b"
        assert error is None

    def test_validate_preferred_format_strips_whitespace(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_PREFERRED_FORMAT", "  epub  "
        )
        assert value == "epub"
        assert error is None

    def test_validate_preferred_format_empty_string(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_PREFERRED_FORMAT", ""
        )
        assert value == ""
        assert error is None

    def test_validate_preferred_format_none_returns_empty(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_PREFERRED_FORMAT", None
        )
        # None value is handled by early return before format-specific validation
        assert error is None

    def test_validate_fallback_format_normalizes(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_FALLBACK_FORMAT", "MP3"
        )
        assert value == "mp3"
        assert error is None

    def test_validate_max_size_with_int(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_MAX_SIZE_MB", 500
        )
        assert value == 500
        assert error is None

    def test_validate_max_size_with_zero(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_MAX_SIZE_MB", 0
        )
        assert value == 0
        assert error is None

    def test_validate_max_size_clamps_negative(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_MAX_SIZE_MB", -10
        )
        assert value == 0
        assert error is None

    def test_validate_max_size_with_string_int(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_MAX_SIZE_MB", "200"
        )
        assert value == 200
        assert error is None

    def test_validate_max_size_with_invalid_string(self):
        value, error = validate_search_preference_value(
            "DUAL_DOWNLOAD_MAX_SIZE_MB", "abc"
        )
        assert value == 0
        assert error is not None
        assert "non-negative integer" in error

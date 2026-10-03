"""Tests for the minimum adapter request interval option."""

from unittest.mock import create_autospec, patch

import pytest
import voluptuous as vol
from homeassistant import data_entry_flow
from homeassistant.core import HomeAssistant
from pykumo import KumoCloudAccount
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kumo.config_flow import EDIT_KEY, EDIT_TIMEOUT
from custom_components.kumo.const import (
    CONF_MIN_REQUEST_INTERVAL,
    DEFAULT_MIN_REQUEST_INTERVAL,
    DOMAIN,
)

_TIMEOUT_PAGE = {
    "connect_timeout": 1.2,
    "response_timeout": 8,
    "scan_interval": 60,
    "post_command_refresh_delay": 2.0,
}


async def _open_timeout_page(hass, options=None):
    entry = MockConfigEntry(
        domain=DOMAIN, data={"username": "u", "password": "p"}, options=options or {}
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {EDIT_KEY: EDIT_TIMEOUT}
    )
    assert result["step_id"] == "timeout_settings"
    return entry, result


def _default(result, key):
    for marker in result["data_schema"].schema:
        if marker == key:
            return marker.default()
    raise AssertionError(f"{key} not in form")


async def test_form_defaults_to_pykumo_interval(hass: HomeAssistant):
    _, result = await _open_timeout_page(hass)
    assert _default(result, CONF_MIN_REQUEST_INTERVAL) == DEFAULT_MIN_REQUEST_INTERVAL


async def test_form_shows_saved_interval(hass: HomeAssistant):
    _, result = await _open_timeout_page(hass, {CONF_MIN_REQUEST_INTERVAL: 0.5})
    assert _default(result, CONF_MIN_REQUEST_INTERVAL) == 0.5


async def test_saving_interval(hass: HomeAssistant):
    entry, result = await _open_timeout_page(hass)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {**_TIMEOUT_PAGE, CONF_MIN_REQUEST_INTERVAL: 0}
    )
    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_MIN_REQUEST_INTERVAL] == 0


async def test_interval_out_of_range_rejected(hass: HomeAssistant):
    _, result = await _open_timeout_page(hass)
    with pytest.raises(vol.Invalid):
        await hass.config_entries.options.async_configure(
            result["flow_id"], {**_TIMEOUT_PAGE, CONF_MIN_REQUEST_INTERVAL: 6}
        )


async def _setup_and_capture_make_pykumos(hass, options, spec=None):
    """Set up the entry with make_pykumos replaced by a mock shaped like spec
    (by default the real one, whose signature setup inspects)."""
    make = create_autospec(spec or KumoCloudAccount.make_pykumos, return_value={})
    entry = MockConfigEntry(
        domain=DOMAIN, data={"username": "u", "password": "p"}, options=options
    )
    entry.add_to_hass(hass)
    with (
        patch("pykumo.KumoCloudAccount.try_setup", return_value=True),
        patch("pykumo.KumoCloudAccount.make_pykumos", make),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    make.assert_called_once()
    return make.call_args


async def test_setup_passes_interval_to_pykumo(hass: HomeAssistant):
    call = await _setup_and_capture_make_pykumos(
        hass, {CONF_MIN_REQUEST_INTERVAL: 0.75}
    )
    assert call.kwargs["min_request_interval"] == 0.75


async def test_setup_leaves_pykumo_default_when_unset(hass: HomeAssistant):
    call = await _setup_and_capture_make_pykumos(hass, {})
    assert "min_request_interval" not in call.kwargs


async def test_setup_ignores_interval_with_older_pykumo(hass: HomeAssistant, caplog):
    def make_pykumos(self, timeouts=None, init_update_status=True):
        """Signature of pykumo releases from before request throttling."""

    call = await _setup_and_capture_make_pykumos(
        hass, {CONF_MIN_REQUEST_INTERVAL: 0.75}, spec=make_pykumos
    )
    assert "min_request_interval" not in call.kwargs
    assert "can't throttle adapter requests" in caplog.text

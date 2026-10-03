"""Tests for the debug traffic capture option."""

import json
import logging
import os
from unittest.mock import patch

from homeassistant import data_entry_flow
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kumo.config_flow import EDIT_DEBUG, EDIT_KEY
from custom_components.kumo.const import (
    CONF_DEBUG_REDACT_SECRETS,
    CONF_DEBUG_TRAFFIC_LOG,
    CONF_SCAN_INTERVAL,
    DOMAIN,
)
from custom_components.kumo.debug_log import TRAFFIC_LOGGER_NAME, debug_log_path


def _read_events(path):
    with open(path, encoding="utf-8") as log_file:
        return [json.loads(line) for line in log_file if line.strip()]


async def _setup_entry(hass, options):
    entry = MockConfigEntry(
        domain=DOMAIN, data={"username": "u", "password": "p"}, options=options
    )
    entry.add_to_hass(hass)
    with patch("pykumo.KumoCloudAccount.try_setup", return_value=True):
        assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


async def test_options_debug_step_keeps_other_options(hass: HomeAssistant):
    """Saving the debug page must not wipe the timeout settings."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={"username": "u", "password": "p"},
        options={CONF_SCAN_INTERVAL: 30},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {EDIT_KEY: EDIT_DEBUG}
    )
    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["step_id"] == "debug_settings"
    assert result["description_placeholders"]["log_path"] == debug_log_path(hass)

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_DEBUG_TRAFFIC_LOG: True, CONF_DEBUG_REDACT_SECRETS: False},
    )
    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_SCAN_INTERVAL: 30,
        CONF_DEBUG_TRAFFIC_LOG: True,
        CONF_DEBUG_REDACT_SECRETS: False,
    }


async def test_options_timeout_step_keeps_debug_option(hass: HomeAssistant):
    """Saving the timeout page must not turn debug capture off."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={"username": "u", "password": "p"},
        options={CONF_DEBUG_TRAFFIC_LOG: True},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {EDIT_KEY: "Timeouts"}
    )
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            "connect_timeout": 1.2,
            "response_timeout": 8,
            CONF_SCAN_INTERVAL: 45,
            "post_command_refresh_delay": 2.0,
        },
    )
    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_DEBUG_TRAFFIC_LOG] is True
    assert entry.options[CONF_SCAN_INTERVAL] == 45


async def test_capture_writes_traffic_until_unload(hass: HomeAssistant, tmp_path):
    """With the option on, pykumo.traffic records land in the file."""
    hass.config.config_dir = str(tmp_path)
    logger = logging.getLogger(TRAFFIC_LOGGER_NAME)

    entry = await _setup_entry(hass, {CONF_DEBUG_TRAFFIC_LOG: True})
    assert logger.isEnabledFor(logging.DEBUG)
    assert logger.propagate is False

    # Stand-in for an event reported by pykumo.
    logger.debug('{"channel": "local", "direction": "recv", "body": {"r": {}}}')

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert not logger.handlers
    assert logger.level == logging.NOTSET
    assert logger.propagate is True

    events = _read_events(debug_log_path(hass))
    assert [e.get("event") for e in events] == [
        "capture_started",
        None,
        "capture_stopped",
    ]
    assert events[0]["redact_secrets"] is True
    assert events[1] == {"channel": "local", "direction": "recv", "body": {"r": {}}}


async def test_reload_does_not_duplicate_records(hass: HomeAssistant, tmp_path):
    """Reloading (as an options change does) leaves one handler attached."""
    hass.config.config_dir = str(tmp_path)
    logger = logging.getLogger(TRAFFIC_LOGGER_NAME)

    entry = await _setup_entry(hass, {CONF_DEBUG_TRAFFIC_LOG: True})
    with patch("pykumo.KumoCloudAccount.try_setup", return_value=True):
        assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert len(logger.handlers) == 1

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert not logger.handlers


async def test_capture_off_by_default(hass: HomeAssistant, tmp_path):
    """Without the option, nothing is attached and no file is created."""
    hass.config.config_dir = str(tmp_path)
    await _setup_entry(hass, {})

    assert not logging.getLogger(TRAFFIC_LOGGER_NAME).handlers
    assert not os.path.exists(debug_log_path(hass))

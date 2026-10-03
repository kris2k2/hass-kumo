"""Tests for offering fan speeds a unit doesn't declare."""

from unittest.mock import MagicMock, patch

from homeassistant import data_entry_flow
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.kumo.climate import KumoThermostat
from custom_components.kumo.config_flow import EDIT_FAN_SPEEDS, EDIT_KEY
from custom_components.kumo.const import CONF_UNDECLARED_FAN_SPEEDS, DOMAIN

DECLARED = ["quiet", "low", "powerful", "auto"]
EVERY = ["superQuiet", "quiet", "low", "powerful", "superPowerful", "auto"]


def _get_fan_speeds(include_undeclared=False):
    return EVERY if include_undeclared else DECLARED


def _thermostat(options=None, get_fan_speeds=_get_fan_speeds):
    """A thermostat on 3-speed unit S1 with the given entry options."""
    device = MagicMock()
    device.get_name.return_value = "Apartment"
    device.get_serial.return_value = "S1"
    device.has_profile.return_value = True
    device.get_fan_speeds.side_effect = get_fan_speeds
    device.get_vane_directions.return_value = []
    device.has_vane_direction.return_value = False
    device.get_supported_modes.return_value = ["off", "cool"]
    coordinator = MagicMock()
    coordinator.get_device.return_value = device
    coordinator.config_entry = (
        None if options is None else MockConfigEntry(domain=DOMAIN, options=options)
    )
    return KumoThermostat(coordinator)


def test_declared_speeds_by_default():
    assert _thermostat({}).fan_modes == DECLARED


def test_declared_speeds_without_config_entry():
    assert _thermostat(None).fan_modes == DECLARED


def test_undeclared_speeds_for_chosen_unit():
    entity = _thermostat({CONF_UNDECLARED_FAN_SPEEDS: ["S1"]})
    assert entity.fan_modes == EVERY


def test_other_units_choice_leaves_this_one_alone():
    entity = _thermostat({CONF_UNDECLARED_FAN_SPEEDS: ["S2"]})
    assert entity.fan_modes == DECLARED


def test_older_pykumo_without_the_argument():
    """Older pykumo releases can't be asked, so take what they give."""

    def old_get_fan_speeds():
        return EVERY

    entity = _thermostat({CONF_UNDECLARED_FAN_SPEEDS: ["S1"]}, old_get_fan_speeds)
    assert entity.fan_modes == EVERY


_CACHE = [
    {},
    {},
    {
        "children": [
            {
                "zoneTable": {
                    "S1": {"label": "Apartment"},
                    "S2": {"label": "Bedroom"},
                }
            }
        ]
    },
]


async def _open_fan_speeds_page(hass, options=None):
    entry = MockConfigEntry(
        domain=DOMAIN, data={"username": "u", "password": "p"}, options=options or {}
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    with patch("custom_components.kumo.config_flow.load_json", return_value=_CACHE):
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], {EDIT_KEY: EDIT_FAN_SPEEDS}
        )
    assert result["step_id"] == "fan_speeds"
    return entry, result


def _default(result):
    for marker in result["data_schema"].schema:
        if marker == CONF_UNDECLARED_FAN_SPEEDS:
            return marker.default()
    raise AssertionError("option not in form")


async def test_form_lists_units_and_defaults_to_none(hass: HomeAssistant):
    _, result = await _open_fan_speeds_page(hass)
    assert _default(result) == []
    validator = result["data_schema"].schema[CONF_UNDECLARED_FAN_SPEEDS]
    assert validator.options == {"S1": "Apartment", "S2": "Bedroom"}


async def test_form_drops_units_no_longer_on_the_account(hass: HomeAssistant):
    _, result = await _open_fan_speeds_page(
        hass, {CONF_UNDECLARED_FAN_SPEEDS: ["S1", "GONE"]}
    )
    assert _default(result) == ["S1"]


async def test_saving_keeps_other_options(hass: HomeAssistant):
    entry, result = await _open_fan_speeds_page(hass, {"scan_interval": 30})
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_UNDECLARED_FAN_SPEEDS: ["S2"]}
    )
    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert entry.options == {"scan_interval": 30, CONF_UNDECLARED_FAN_SPEEDS: ["S2"]}

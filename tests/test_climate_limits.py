"""Tests for the thermostat's min/max setpoints."""

from unittest.mock import MagicMock

from homeassistant.components.climate.const import DEFAULT_MAX_TEMP, DEFAULT_MIN_TEMP
from homeassistant.core import HomeAssistant
from homeassistant.util.unit_system import METRIC_SYSTEM, US_CUSTOMARY_SYSTEM

from custom_components.kumo.climate import KumoThermostat


def _thermostat(hass, limits=None, supports_limits=True):
    spec = [
        "get_name",
        "get_serial",
        "has_profile",
        "get_mode",
    ]
    if supports_limits:
        spec.append("get_setpoint_limits")
    device = MagicMock(spec=spec)
    device.get_name.return_value = "Apartment"
    device.get_serial.return_value = "S1"
    device.has_profile.return_value = False
    if supports_limits:
        device.get_setpoint_limits.return_value = limits
    coordinator = MagicMock()
    coordinator.get_device.return_value = device
    entity = KumoThermostat(coordinator)
    entity.hass = hass
    return entity


async def test_limits_from_unit_in_celsius(hass: HomeAssistant):
    hass.config.units = METRIC_SYSTEM
    entity = _thermostat(hass, limits=(19.5, 30))
    assert entity.min_temp == 19.5
    assert entity.max_temp == 30


async def test_limits_from_unit_in_fahrenheit(hass: HomeAssistant):
    hass.config.units = US_CUSTOMARY_SYSTEM
    entity = _thermostat(hass, limits=(19.5, 30))
    # Mitsubishi's own conversion: 19.5 °C shows as 67 °F.
    assert entity.min_temp == 67
    assert entity.max_temp == 86


async def test_default_limits_until_profile_read(hass: HomeAssistant):
    hass.config.units = METRIC_SYSTEM
    entity = _thermostat(hass, limits=None)
    assert entity.min_temp == DEFAULT_MIN_TEMP
    assert entity.max_temp == DEFAULT_MAX_TEMP


async def test_default_limits_with_older_pykumo(hass: HomeAssistant):
    hass.config.units = METRIC_SYSTEM
    entity = _thermostat(hass, supports_limits=False)
    assert entity.min_temp == DEFAULT_MIN_TEMP
    assert entity.max_temp == DEFAULT_MAX_TEMP

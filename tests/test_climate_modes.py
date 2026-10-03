"""Tests for the thermostat's hvac_modes."""

from unittest.mock import MagicMock

from homeassistant.components.climate import ClimateEntityFeature, HVACMode

from custom_components.kumo.climate import KumoThermostat


def _thermostat(dry=True, heat=False, vent=True, auto=False):
    device = MagicMock()
    device.get_name.return_value = "Apartment"
    device.get_serial.return_value = "S1"
    device.has_profile.return_value = True
    device.get_fan_speeds.return_value = ["quiet", "low", "powerful", "auto"]
    device.get_vane_directions.return_value = []
    device.has_vane_direction.return_value = False
    set_modes(device, dry=dry, heat=heat, vent=vent, auto=auto)
    coordinator = MagicMock()
    coordinator.get_device.return_value = device
    return KumoThermostat(coordinator), device


def set_modes(device, dry, heat, vent, auto):
    device.has_dry_mode.return_value = dry
    device.has_heat_mode.return_value = heat
    device.has_vent_mode.return_value = vent
    device.has_auto_mode.return_value = auto


def _has_range(entity):
    return bool(
        entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE_RANGE
    )


def test_cooling_only_unit():
    entity, _ = _thermostat()
    assert entity.hvac_modes == [
        HVACMode.OFF,
        HVACMode.COOL,
        HVACMode.DRY,
        HVACMode.FAN_ONLY,
    ]
    assert not _has_range(entity)


def test_cooling_only_unit_reported_with_auto():
    # Older pykumo releases report auto on units without heat mode.
    entity, _ = _thermostat(auto=True)
    assert HVACMode.HEAT_COOL not in entity.hvac_modes
    assert not _has_range(entity)


def test_heat_pump():
    entity, _ = _thermostat(heat=True, auto=True)
    assert entity.hvac_modes == [
        HVACMode.OFF,
        HVACMode.COOL,
        HVACMode.DRY,
        HVACMode.HEAT,
        HVACMode.FAN_ONLY,
        HVACMode.HEAT_COOL,
    ]
    assert _has_range(entity)


def test_mode_found_later_keeps_order_in_a_new_list():
    entity, device = _thermostat(dry=False)
    before = entity.hvac_modes
    set_modes(device, dry=True, heat=False, vent=True, auto=False)
    entity._refresh_capabilities()
    assert entity.hvac_modes == [
        HVACMode.OFF,
        HVACMode.COOL,
        HVACMode.DRY,
        HVACMode.FAN_ONLY,
    ]
    assert entity.hvac_modes is not before


def test_mode_not_dropped_by_a_later_poll():
    entity, device = _thermostat()
    set_modes(device, dry=False, heat=False, vent=False, auto=False)
    entity._refresh_capabilities()
    assert HVACMode.DRY in entity.hvac_modes
    assert HVACMode.FAN_ONLY in entity.hvac_modes

"""Tests for the thermostat's hvac_modes."""

from unittest.mock import MagicMock

from homeassistant.components.climate import ClimateEntityFeature, HVACMode

from custom_components.kumo.climate import KumoThermostat

_SPEC = [
    "get_name",
    "get_serial",
    "has_profile",
    "get_fan_speeds",
    "get_vane_directions",
    "has_vane_direction",
    "has_dry_mode",
    "has_heat_mode",
    "has_vent_mode",
    "has_auto_mode",
]


def _thermostat(modes=None, older_pykumo=None):
    """A thermostat on a unit offering modes (pykumo's names).

    older_pykumo instead gives has_*_mode() answers from a pykumo without
    get_supported_modes(), e.g. {"dry": True, "auto": True}.
    """
    spec = _SPEC if older_pykumo is not None else [*_SPEC, "get_supported_modes"]
    device = MagicMock(spec=spec)
    device.get_name.return_value = "Apartment"
    device.get_serial.return_value = "S1"
    device.has_profile.return_value = True
    device.get_fan_speeds.return_value = ["quiet", "low", "powerful", "auto"]
    device.get_vane_directions.return_value = []
    device.has_vane_direction.return_value = False
    if older_pykumo is not None:
        for mode in ("dry", "heat", "vent", "auto"):
            getattr(device, f"has_{mode}_mode").return_value = older_pykumo.get(
                mode, False
            )
    else:
        device.get_supported_modes.return_value = modes
    coordinator = MagicMock()
    coordinator.get_device.return_value = device
    return KumoThermostat(coordinator), device


def _has_range(entity):
    return bool(
        entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE_RANGE
    )


COOLING_ONLY = [HVACMode.OFF, HVACMode.COOL, HVACMode.DRY, HVACMode.FAN_ONLY]


def test_cooling_only_unit():
    entity, _ = _thermostat(["off", "cool", "dry", "vent"])
    assert entity.hvac_modes == COOLING_ONLY
    assert not _has_range(entity)


def test_heat_pump():
    entity, _ = _thermostat(["off", "cool", "dry", "heat", "vent", "auto"])
    assert entity.hvac_modes == [
        HVACMode.OFF,
        HVACMode.COOL,
        HVACMode.DRY,
        HVACMode.HEAT,
        HVACMode.FAN_ONLY,
        HVACMode.HEAT_COOL,
    ]
    assert _has_range(entity)


def test_defaults_until_modes_known():
    entity, _ = _thermostat(None)
    assert entity.hvac_modes == [HVACMode.OFF, HVACMode.COOL]


def test_follows_changes_from_the_app():
    entity, device = _thermostat(["off", "cool", "dry", "heat", "vent", "auto"])
    device.get_supported_modes.return_value = ["off", "cool", "vent"]
    entity._refresh_capabilities()
    assert entity.hvac_modes == [HVACMode.OFF, HVACMode.COOL, HVACMode.FAN_ONLY]
    assert not _has_range(entity)


def test_older_pykumo_cooling_only_unit_reported_with_auto():
    entity, _ = _thermostat(older_pykumo={"dry": True, "vent": True, "auto": True})
    assert entity.hvac_modes == COOLING_ONLY
    assert not _has_range(entity)


def test_older_pykumo_heat_pump():
    entity, _ = _thermostat(
        older_pykumo={"dry": True, "heat": True, "vent": True, "auto": True}
    )
    assert HVACMode.HEAT_COOL in entity.hvac_modes
    assert _has_range(entity)

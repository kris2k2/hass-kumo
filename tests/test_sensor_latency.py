"""Tests for the adapter latency sensor."""

from unittest.mock import MagicMock

from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from homeassistant.const import EntityCategory, UnitOfTime
from homeassistant.core import HomeAssistant

from custom_components.kumo import sensor
from custom_components.kumo.const import DOMAIN, KUMO_DATA, KUMO_DATA_COORDINATORS


def _coordinator(device):
    coordinator = MagicMock()
    coordinator.get_device.return_value = device
    coordinator.get_available.return_value = True
    return coordinator


def _device(latency=None, supports_latency=True):
    spec = ["get_name", "get_serial"]
    if supports_latency:
        spec.append("get_request_latency")
    device = MagicMock(spec=spec)
    device.get_name.return_value = "Den"
    device.get_serial.return_value = "S1"
    if supports_latency:
        device.get_request_latency.return_value = latency
    return device


def test_latency_sensor_reports_average_and_samples():
    device = _device(
        {"last": 41.2, "average": 38.5, "min": 30.1, "max": 52.0, "samples": 10}
    )
    entity = sensor.KumoAdapterLatency(_coordinator(device))

    assert entity.name == "Den Adapter Latency"
    assert entity.unique_id == "S1-adapter-latency"
    assert entity.native_value == 38.5
    assert entity.extra_state_attributes == {
        "last_ms": 41.2,
        "min_ms": 30.1,
        "max_ms": 52.0,
        "samples": 10,
    }
    assert entity.device_class == SensorDeviceClass.DURATION
    assert entity.native_unit_of_measurement == UnitOfTime.MILLISECONDS
    assert entity.state_class == SensorStateClass.MEASUREMENT
    assert entity.entity_category == EntityCategory.DIAGNOSTIC


def test_latency_sensor_empty_before_first_request():
    entity = sensor.KumoAdapterLatency(_coordinator(_device(None)))
    assert entity.native_value is None
    assert entity.extra_state_attributes is None


async def _added_entity_types(hass, device):
    entry = MagicMock(entry_id="entry")
    account = MagicMock()
    account.get_all_units.return_value = ["S1"]
    account.get_kumo_stations.return_value = []
    settings = MagicMock()
    settings.get_account.return_value = account
    hass.data[DOMAIN] = {
        "entry": {
            KUMO_DATA: settings,
            KUMO_DATA_COORDINATORS: {"S1": _coordinator(device)},
        }
    }
    added = []
    await sensor.async_setup_entry(
        hass, entry, lambda ents, _update: added.extend(ents)
    )
    return {type(entity) for entity in added}


async def test_latency_sensor_added_when_pykumo_supports_it(hass: HomeAssistant):
    types = await _added_entity_types(hass, _device())
    assert sensor.KumoAdapterLatency in types


async def test_latency_sensor_skipped_with_older_pykumo(hass: HomeAssistant):
    types = await _added_entity_types(hass, _device(supports_latency=False))
    assert sensor.KumoAdapterLatency not in types
    assert sensor.KumoWifiSignal in types

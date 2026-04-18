"""Constants for the Kumo integration."""

from datetime import timedelta
from typing import Final

from homeassistant.const import Platform

DEFAULT_NAME = "Kumo"
DOMAIN = "kumo"
KUMO_DATA = "data"
KUMO_DATA_COORDINATORS = "coordinators"
KUMO_CONFIG_CACHE = "kumo_cache.json"
CONF_PREFER_CACHE = "prefer_cache"
CONF_CONNECT_TIMEOUT = "connect_timeout"
CONF_RESPONSE_TIMEOUT = "response_timeout"
CONF_POOL_CONNECTIONS = "pool_connections"
CONF_POOL_MAXSIZE = "pool_maxsize"
MAX_AVAILABILITY_TRIES = 3 # How many times we will attempt to update from a kumo before marking it unavailable

# Default HTTP pool settings
DEFAULT_POOL_CONNECTIONS = 10
DEFAULT_POOL_MAXSIZE = 20

DHCP_DISCOVERED_KEY = f"{DOMAIN}_dhcp_discovered"

PLATFORMS: Final = [Platform.CLIMATE, Platform.SENSOR]

SCAN_INTERVAL = timedelta(seconds=60)

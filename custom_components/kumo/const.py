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
CONF_SCAN_INTERVAL = "scan_interval"
DEFAULT_SCAN_INTERVAL = 60  # seconds
CONF_POST_COMMAND_REFRESH_DELAY = "post_command_refresh_delay"
DEFAULT_POST_COMMAND_REFRESH_DELAY = 2.0  # seconds
# Minimum idle time between requests to one adapter; 0 turns throttling off.
CONF_MIN_REQUEST_INTERVAL = "min_request_interval"
try:
    from pykumo.const import (
        UNIT_MIN_REQUEST_INTERVAL_SECONDS as DEFAULT_MIN_REQUEST_INTERVAL,
    )
except ImportError:  # pykumo releases from before request throttling
    DEFAULT_MIN_REQUEST_INTERVAL = 0.25  # seconds
CONF_DEBUG_TRAFFIC_LOG = "debug_traffic_log"
CONF_DEBUG_REDACT_SECRETS = "debug_redact_secrets"
DEBUG_LOG_DIR = "kumo_debug"
DEBUG_LOG_FILE = "kumo_traffic.jsonl"
DEBUG_LOG_MAX_BYTES = 10 * 1024 * 1024
DEBUG_LOG_BACKUP_COUNT = 5
MAX_AVAILABILITY_TRIES = 3  # How many times we will attempt to update from a kumo before marking it unavailable

DHCP_DISCOVERED_KEY = f"{DOMAIN}_dhcp_discovered"

PLATFORMS: Final = [Platform.CLIMATE, Platform.SENSOR]

SCAN_INTERVAL = timedelta(seconds=DEFAULT_SCAN_INTERVAL)

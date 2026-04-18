"""Support for Mitsubishi KumoCloud devices."""
import logging
from typing import Optional

import homeassistant.helpers.config_validation as cv
import pykumo
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.util.json import load_json
from homeassistant.helpers.json import save_json

from .coordinator import KumoDataUpdateCoordinator
from .const import (
    CONF_CONNECT_TIMEOUT,
    CONF_PREFER_CACHE,
    CONF_RESPONSE_TIMEOUT,
    CONF_POOL_CONNECTIONS,
    CONF_POOL_MAXSIZE,
    DEFAULT_POOL_CONNECTIONS,
    DEFAULT_POOL_MAXSIZE,
    DHCP_DISCOVERED_KEY,
    DOMAIN,
    KUMO_CONFIG_CACHE,
    KUMO_DATA,
    KUMO_DATA_COORDINATORS,
    PLATFORMS,
)
from .http_session import http_session, get_http_session, KumoHttpSession

_LOGGER = logging.getLogger(__name__)

# Global flag to ensure patch is applied only once
_pykumo_patched = False


def _patch_pykumo(pool_connections=None, pool_maxsize=None):
    """Patch pykumo to use shared HTTP session for connection pooling."""
    global _pykumo_patched
    if _pykumo_patched:
        return

    try:
        import pykumo.py_kumo_base as pkb
        import pykumo.py_kumo_cloud_account as pkc
        import pykumo.py_kumo_cloud_account_v3 as pkc3
        import requests

        # Store original request functions
        _original_post = requests.post
        _original_get = requests.get

        def _pooled_post(url, **kwargs):
            """Use shared session for POST requests."""
            session = get_http_session().session
            _LOGGER.info("POST %s via pooled session", url)
            try:
                return session.post(url, **kwargs)
            except Exception as e:
                _LOGGER.warning("POST request failed for %s: %s", url, e)
                raise

        def _pooled_get(url, **kwargs):
            """Use shared session for GET requests."""
            session = get_http_session().session
            _LOGGER.info("GET %s via pooled session", url)
            try:
                return session.get(url, **kwargs)
            except Exception as e:
                _LOGGER.warning("GET request failed for %s: %s", url, e)
                raise

        # Patch PyKumoBase._request to use shared session
        # Original signature: _request(self, post_data) - builds URL internally
        _original_request = pkb.PyKumoBase._request

        def _patched_request(self, post_data):
            """Use shared session instead of creating new one per request."""
            if not self._address:
                _LOGGER.warning("Unit %s address not set", self._name)
                return {}
            url = "http://" + self._address + "/api"
            token = self._token(post_data)
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
            }
            token_param = {"m": token}  # Key is 'm', not 'token'!
            
            session = get_http_session().session
            _LOGGER.info("PyKumoBase._request: PUT %s", url)
            try:
                response = session.put(
                    url,
                    headers=headers,
                    data=post_data,
                    params=token_param,
                    timeout=self._timeouts,
                )
                return response.json()
            except Exception as e:
                _LOGGER.warning("Request to %s failed: %s", url, e)
                return {}

        pkb.PyKumoBase._request = _patched_request

        # Patch KumoCloudV3 - replace global requests.post/get with pooled versions
        # KumoCloudV3 uses direct requests.post() and requests.get() calls
        if hasattr(pkc3, 'KumoCloudV3'):
            # Patch the module-level requests functions that KumoCloudV3 uses
            pkc3.requests.post = _pooled_post
            pkc3.requests.get = _pooled_get
            _LOGGER.info("Patched pykumo.py_kumo_cloud_account_v3 requests functions")

        _pykumo_patched = True
        _LOGGER.info("Patched pykumo to use shared HTTP session with connection pooling")
    except Exception as e:
        _LOGGER.warning("Failed to patch pykumo for connection pooling: %s", e)


# Apply patch after import
_patch_pykumo()


CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_USERNAME): cv.string,
                vol.Required(CONF_PASSWORD): cv.string,
                vol.Optional(CONF_PREFER_CACHE, default=False): cv.boolean,
                vol.Optional(CONF_CONNECT_TIMEOUT): float,
                vol.Optional(CONF_RESPONSE_TIMEOUT): float,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


class KumoCloudSettings:
    """Hold object representing KumoCloud account."""

    def __init__(self, account, domain_config, domain_options):
        """Init KumoCloudAccount object."""
        self._account = account
        self._domain_config = domain_config
        self._domain_options = domain_options
        self._setup_tries = 0

    def get_account(self):
        """Retrieve account."""
        return self._account

    def get_domain_config(self):
        """Retrieve domain config."""
        return self._domain_config

    def get_domain_options(self):
        """Retrieve domain config."""
        return self._domain_options

    def get_raw_json(self):
        """Retrieve raw JSON config from account."""
        return self._account.get_raw_json()

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Setup Kumo Entry"""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault(entry.entry_id, {})
    username = entry.data.get(CONF_USERNAME)
    password = entry.data.get(CONF_PASSWORD)
    prefer_cache = entry.data.get(CONF_PREFER_CACHE)

    # Try V3 API first (Comfort app API), fall back to V2 if it fails
    candidate_ips = hass.data.get(DHCP_DISCOVERED_KEY, {})
    account = await async_kumo_setup_v3(hass, username, password, candidate_ips)
    if not account:
        _LOGGER.info("V3 setup failed, falling back to V2 API")
        account = await async_kumo_setup_v2(hass, prefer_cache, username, password)
    if not account:
        account = await async_kumo_setup_v2(hass, not prefer_cache, username, password)

    if account:
        hass.data[DOMAIN][entry.entry_id][KUMO_DATA] = KumoCloudSettings(account, entry.data, entry.options)

        # Create a data coordinator for each Kumo device
        hass.data[DOMAIN][entry.entry_id].setdefault(KUMO_DATA_COORDINATORS, {})
        coordinators = hass.data[DOMAIN][entry.entry_id][KUMO_DATA_COORDINATORS]
        connect_timeout = float(
            entry.options.get(CONF_CONNECT_TIMEOUT, "1.2")
        )
        response_timeout = float(
            entry.options.get(CONF_RESPONSE_TIMEOUT, "8")
        )
        timeouts = (connect_timeout, response_timeout)

        # Initialize HTTP session with connection pooling BEFORE making pykumo devices
        # Session must be configured before any HTTP calls
        pool_connections = int(
            entry.options.get(CONF_POOL_CONNECTIONS, DEFAULT_POOL_CONNECTIONS)
        )
        pool_maxsize = int(
            entry.options.get(CONF_POOL_MAXSIZE, DEFAULT_POOL_MAXSIZE)
        )
        
        # Reconfigure the HTTP session with new pool settings
        # This recreates the session with the correct pool configuration
        http_session.configure(pool_connections=pool_connections, pool_maxsize=pool_maxsize)
        _LOGGER.info(
            "HTTP session pool configured: pool_connections=%d, pool_maxsize=%d",
            pool_connections,
            pool_maxsize,
        )

        pykumos = await hass.async_add_executor_job(account.make_pykumos, timeouts, True)
        for device in pykumos.values():
            if device.get_serial() not in coordinators:
                coordinators[device.get_serial()] = KumoDataUpdateCoordinator(hass, device)

        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        return True

    _LOGGER.warning("Could not load config from KumoCloud (V3 or V2)")
    return False

async def async_kumo_setup_v3(hass: HomeAssistant, username: str, password: str, candidate_ips: dict = None) -> Optional[pykumo.KumoCloudAccount]:
    """Attempt setup using V3 API (Comfort app).

    Loads any cached kumo_dict first so device addresses are preserved.
    """
    cached_dict = await hass.async_add_executor_job(
        load_json, hass.config.path(KUMO_CONFIG_CACHE)
    )
    if cached_dict and isinstance(cached_dict, list) and len(cached_dict) >= 3:
        account = pykumo.KumoCloudAccount(username, password, kumo_dict=cached_dict)
    else:
        account = pykumo.KumoCloudAccount(username, password)

    try:
        setup_success = await hass.async_add_executor_job(
            account.try_setup_v3_only, candidate_ips or {}
        )
    except (ConnectionError, OSError) as err:
        _LOGGER.warning("V3 setup failed due to network error, will fall back to V2: %s", err)
        return None

    if setup_success:
        await hass.async_add_executor_job(
            save_json, hass.config.path(KUMO_CONFIG_CACHE), account.get_raw_json()
        )
        _LOGGER.info("Loaded config from V3 API (Comfort app)")
        return account

    return None

async def async_kumo_setup_v2(hass: HomeAssistant, prefer_cache: bool, username: str, password: str) -> Optional[pykumo.KumoCloudAccount]:
    """Attempt to load data from cache or V2 Kumo Cloud API."""
    if prefer_cache:
        cached_json = await hass.async_add_executor_job(
            load_json, hass.config.path(KUMO_CONFIG_CACHE)
        ) or {"fetched": False}
        account = pykumo.KumoCloudAccount(username, password, kumo_dict=cached_json)
    else:
        account = pykumo.KumoCloudAccount(username, password)

    setup_success = await hass.async_add_executor_job(account.try_setup)

    if setup_success:
        if prefer_cache:
            _LOGGER.info("Loaded config from local cache")
        else:
            await hass.async_add_executor_job(
                save_json, hass.config.path(KUMO_CONFIG_CACHE), account.get_raw_json()
            )
            _LOGGER.info("Loaded config from KumoCloud V2 server")

        return account

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Unload Entry"""

    for platform in PLATFORMS:
        all_ok = True
        unload_ok = await hass.config_entries.async_forward_entry_unload(entry, platform)
        if not unload_ok:
            all_ok = False

    # Close HTTP session to release connections
    http_session.close()
    _LOGGER.debug("Closed HTTP session on unload")

    return all_ok

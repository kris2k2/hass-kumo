"""Capture raw Kumo API traffic to a file for protocol discovery.

pykumo reports every adapter, cloud REST and Socket.IO exchange to the
``pykumo.traffic`` logger. When the debug option is on, this module points
that logger at a rotating JSON-lines file under the Home Assistant config
directory. Writes go through a queue so the file I/O happens on a dedicated
listener thread rather than in the threads talking to the adapters.
"""

from __future__ import annotations

import json
import logging
import os
import queue
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

import pykumo
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant

from .const import (
    CONF_DEBUG_REDACT_SECRETS,
    CONF_DEBUG_TRAFFIC_LOG,
    DEBUG_LOG_BACKUP_COUNT,
    DEBUG_LOG_DIR,
    DEBUG_LOG_FILE,
    DEBUG_LOG_MAX_BYTES,
)

try:
    from pykumo import traffic as pykumo_traffic
except ImportError:  # pykumo predates traffic capture
    pykumo_traffic = None

_LOGGER = logging.getLogger(__name__)
TRAFFIC_LOGGER_NAME = "pykumo.traffic"

# The capture currently attached to the pykumo.traffic logger, if any.
_active_capture: TrafficCapture | None = None


def debug_log_path(hass: HomeAssistant) -> str:
    """Return the path of the traffic capture file."""
    return hass.config.path(DEBUG_LOG_DIR, DEBUG_LOG_FILE)


class TrafficCapture:
    """Route pykumo.traffic records to a rotating file."""

    def __init__(self, path: str, redact_secrets: bool) -> None:
        self.path = path
        self.redact_secrets = redact_secrets
        self._logger = logging.getLogger(TRAFFIC_LOGGER_NAME)
        self._queue_handler: QueueHandler | None = None
        self._listener: QueueListener | None = None
        self._saved_level = logging.NOTSET
        self._saved_propagate = True

    def start(self) -> None:
        """Open the file and begin capturing. Does blocking I/O.

        Replaces any capture still attached (e.g. one left behind when an
        unload failed) so records are never written twice.
        """
        global _active_capture  # pylint: disable=global-statement
        if _active_capture is not None:
            _active_capture.stop()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        file_handler = RotatingFileHandler(
            self.path,
            maxBytes=DEBUG_LOG_MAX_BYTES,
            backupCount=DEBUG_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter("%(message)s"))

        log_queue: queue.SimpleQueue = queue.SimpleQueue()
        self._queue_handler = QueueHandler(log_queue)
        self._listener = QueueListener(log_queue, file_handler)
        self._listener.start()

        if pykumo_traffic is not None:
            pykumo_traffic.set_redact_secrets(self.redact_secrets)
        self._saved_level = self._logger.level
        self._saved_propagate = self._logger.propagate
        self._logger.addHandler(self._queue_handler)
        self._logger.setLevel(logging.DEBUG)
        # Keep the (large, frequent) traffic records out of home-assistant.log.
        self._logger.propagate = False

        _active_capture = self
        self._write_marker(
            "capture_started",
            pykumo_version=getattr(pykumo, "__version__", "unknown"),
            home_assistant_version=HA_VERSION,
            redact_secrets=self.redact_secrets,
            pykumo_supports_capture=pykumo_traffic is not None,
        )

    def stop(self) -> None:
        """Stop capturing and close the file. Does blocking I/O."""
        global _active_capture  # pylint: disable=global-statement
        if self._queue_handler is None:
            return
        if _active_capture is self:
            _active_capture = None
        self._write_marker("capture_stopped")
        self._logger.removeHandler(self._queue_handler)
        self._logger.setLevel(self._saved_level)
        self._logger.propagate = self._saved_propagate
        if pykumo_traffic is not None:
            pykumo_traffic.set_redact_secrets(True)
        self._queue_handler = None
        if self._listener is not None:
            self._listener.stop()
            for handler in self._listener.handlers:
                handler.close()
            self._listener = None

    def _write_marker(self, event: str, **fields) -> None:
        """Write a session boundary line in the same format as traffic events."""
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "channel": "meta",
            "direction": "info",
            "event": event,
            **fields,
        }
        self._logger.debug("%s", json.dumps(record), extra={"kumo_traffic": record})


async def async_start_traffic_capture(
    hass: HomeAssistant, entry: ConfigEntry
) -> TrafficCapture | None:
    """Start capturing if the entry's options ask for it.

    The capture stops when the entry unloads (including after an options
    change, which reloads the entry), so toggling the option takes effect
    immediately.
    """
    if not entry.options.get(CONF_DEBUG_TRAFFIC_LOG, False):
        return None

    capture = TrafficCapture(
        debug_log_path(hass),
        bool(entry.options.get(CONF_DEBUG_REDACT_SECRETS, True)),
    )
    try:
        await hass.async_add_executor_job(capture.start)
    except OSError as err:
        _LOGGER.error("Could not open Kumo debug log %s: %s", capture.path, err)
        return None

    async def _async_stop() -> None:
        await hass.async_add_executor_job(capture.stop)

    entry.async_on_unload(_async_stop)

    if pykumo_traffic is None:
        _LOGGER.warning(
            "Kumo debug traffic logging is enabled, but the installed pykumo "
            "(%s) does not report traffic; only session markers will be written "
            "to %s",
            getattr(pykumo, "__version__", "unknown"),
            capture.path,
        )
    else:
        _LOGGER.warning(
            "Kumo debug traffic logging is enabled (secrets %s); writing to %s. "
            "Turn it off under the integration's options when you are done",
            "redacted" if capture.redact_secrets else "NOT redacted",
            capture.path,
        )
    return capture

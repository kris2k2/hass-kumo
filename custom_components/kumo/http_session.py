"""HTTP session manager with connection pooling for Kumo integration."""

import logging
from contextlib import contextmanager
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_LOGGER = logging.getLogger(__name__)

# Default pool configuration
DEFAULT_POOL_CONNECTIONS = 10
DEFAULT_POOL_MAXSIZE = 20
DEFAULT_RETRY_TOTAL = 3
DEFAULT_RETRY_BACKOFF_FACTOR = 0.1


class KumoHttpSession:
    """Shared HTTP session with connection pooling for Kumo devices.

    This class provides a reusable requests.Session with TCP connection
    pooling to prevent overwhelming the Kumo controller with new TCP
    connections on every API call.
    """

    def __init__(
        self,
        pool_connections: int = DEFAULT_POOL_CONNECTIONS,
        pool_maxsize: int = DEFAULT_POOL_MAXSIZE,
        retry_total: int = DEFAULT_RETRY_TOTAL,
        retry_backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    ):
        """Initialize the HTTP session with connection pooling.

        Args:
            pool_connections: Number of connection pools to cache.
            pool_maxsize: Maximum number of connections per pool.
            retry_total: Total number of retries for failed requests.
            retry_backoff_factor: Backoff factor for retry delays.
        """
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._retry_total = retry_total
        self._retry_backoff_factor = retry_backoff_factor
        self._session: Optional[requests.Session] = None
        self._closed = False

    @property
    def session(self) -> requests.Session:
        """Get or create the shared session."""
        if self._session is None:
            self._session = self._create_session()
            _LOGGER.info(
                "Created HTTP session with pool_connections=%d, pool_maxsize=%d",
                self._pool_connections,
                self._pool_maxsize,
            )
        return self._session

    def configure(
        self,
        pool_connections: Optional[int] = None,
        pool_maxsize: Optional[int] = None,
    ) -> None:
        """Reconfigure the HTTP session with new pool settings.
        
        If the session already exists, it will be closed and recreated
        with the new configuration.
        """
        if pool_connections is not None:
            self._pool_connections = pool_connections
        if pool_maxsize is not None:
            self._pool_maxsize = pool_maxsize
        
        # Close existing session if open
        if self._session is not None:
            try:
                self._session.close()
            except Exception as e:
                _LOGGER.warning("Error closing session: %s", e)
            self._session = None
            _LOGGER.debug("Recreating HTTP session with new pool settings")
        
        # Access session property to trigger recreation with new settings
        try:
            _ = self.session
        except Exception as e:
            _LOGGER.error("Failed to create HTTP session: %s", e)
            raise

    def _create_session(self) -> requests.Session:
        """Create a new session with connection pooling."""
        session = requests.Session()

        # Configure retry strategy
        retry = Retry(
            total=self._retry_total,
            backoff_factor=self._retry_backoff_factor,
            status_forcelist=[500, 502, 503, 504],
        )

        # Mount HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=self._pool_connections,
            pool_maxsize=self._pool_maxsize,
            max_retries=retry,
        )

        # Mount for both HTTP and HTTPS
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def close(self) -> None:
        """Close the session and release resources."""
        if self._session is not None and not self._closed:
            self._session.close()
            self._closed = True
            _LOGGER.debug("Closed HTTP session")

    @contextmanager
    def session_context(self):
        """Context manager for session lifecycle."""
        try:
            yield self.session
        finally:
            self.close()

    def is_available(self) -> bool:
        """Check if session is available."""
        return self._session is not None and not self._closed


# Global singleton instance
http_session = KumoHttpSession()


def get_http_session() -> KumoHttpSession:
    """Get the global HTTP session instance."""
    return http_session
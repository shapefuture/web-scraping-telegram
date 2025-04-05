import asyncio
import os
import logging
import signal
import sys
import time
import pickle
import shutil
import glob
import atexit
from pathlib import Path
from telethon import TelegramClient
from telethon.sessions import StringSession # Removed MemorySession import
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from config_loader import AppConfig # Assuming AppConfig holds API_ID, API_HASH, PHONE etc.

logger = logging.getLogger(__name__)

# CustomSession class removed as StringSession is preferred for persistence.

class SessionManager:
    """Manages session file paths and locking to prevent multiple instances."""
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls, config: AppConfig):
        # Make SessionManager a singleton based on config's base_path
        base_path = Path(config.base_path).resolve()
        if cls._instance is None or cls._instance._base_path != base_path:
             cls._instance = super(SessionManager, cls).__new__(cls)
             cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: AppConfig):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._config = config
            self._base_path = Path(config.base_path).resolve()
            self._session_file_path = self._base_path / config.session_file
            self._lock_file_path = self._base_path / f"{config.session_file}.lock"
            self._backup_dir = self._base_path / 'session_backups' # Specific backup dir
            self._lock_fd = None
            self._initialized = True
            self._setup_file_handling()

    def _setup_file_handling(self):
        """Set up directories and acquire lock file."""
        try:
            self._session_file_path.parent.mkdir(parents=True, exist_ok=True)
            self._backup_dir.mkdir(exist_ok=True)

            # Handle potentially stale lock file before acquiring
            self._handle_stale_lock()

            # Acquire exclusive lock file
            try:
                # Use file descriptor for locking
                self._lock_fd = os.open(str(self._lock_file_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                logger.info(f"Acquired lock file: {self._lock_file_path}")
            except FileExistsError:
                logger.error(f"Lock file {self._lock_file_path} exists after stale check. Another instance is likely running.")
                raise RuntimeError(f"Cannot acquire lock file {self._lock_file_path}, another instance may be running.")
            except Exception as e:
                 logger.error(f"Error creating lock file {self._lock_file_path}: {e}")
                 raise

            # Register cleanup using atexit and signal handlers
            atexit.register(self.cleanup)
            try:
                 for sig in (signal.SIGINT, signal.SIGTERM):
                      signal.signal(sig, self._signal_handler)
            except ValueError:
                 logger.warning("Could not set signal handlers (might be running in a non-main thread).")

            logger.info("SessionManager file handling setup complete.")

        except Exception as e:
            logger.error(f"Error setting up SessionManager file handling: {e}", exc_info=True)
            self.cleanup() # Attempt cleanup if setup fails
            raise

    def _handle_stale_lock(self):
        """Check for and remove stale lock files."""
        if self._lock_file_path.exists():
            logger.warning(f"Existing lock file found: {self._lock_file_path}. Checking if stale...")
            try:
                lock_age = time.time() - self._lock_file_path.stat().st_mtime
                stale_threshold = 60 # seconds
                if lock_age > stale_threshold:
                    logger.warning(f"Lock file is older than {stale_threshold} seconds ({lock_age:.0f}s). Assuming stale and removing.")
                    self._lock_file_path.unlink()
                else:
                    logger.error(f"Lock file is recent ({lock_age:.0f}s old). Another instance might be running.")
                    raise RuntimeError(f"Recent lock file found: {self._lock_file_path}. Another instance may be running.")
            except FileNotFoundError:
                logger.debug("Lock file disappeared during stale check (race condition?).")
            except Exception as e:
                logger.error(f"Error checking/removing stale lock file {self._lock_file_path}: {e}. Manual intervention might be required.")
                raise RuntimeError(f"Error handling potentially stale lock file: {e}")
        else:
            logger.debug("No lock file found during stale check.")

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        logger.warning(f"Received signal {signal.Signals(signum).name}, initiating cleanup...")
        self.cleanup() # Run synchronous parts of cleanup
        sys.exit(128 + signum) # Standard exit code for signals

    def cleanup(self):
        """Release lock file and clean up resources (synchronous parts)."""
        logger.info("SessionManager synchronous cleanup initiated...")
        lock_fd = getattr(self, '_lock_fd', None)
        if lock_fd is not None:
            try:
                os.close(lock_fd)
                self._lock_fd = None
                logger.debug("Closed lock file descriptor.")
            except OSError as e:
                logger.error(f"Error closing lock file descriptor: {e}")

        lock_file_path = getattr(self, '_lock_file_path', None)
        if lock_file_path and lock_file_path.exists():
            try:
                lock_file_path.unlink()
                logger.info(f"Removed lock file: {lock_file_path}")
            except OSError as e:
                logger.error(f"Error removing lock file {lock_file_path}: {e}")
        else:
             logger.debug("Lock file already removed or path not set.")
        logger.info("SessionManager synchronous cleanup finished.")

    @property
    def session_file(self) -> str:
        """Get the full path to the session file."""
        return str(self._session_file_path)

    @property
    def backup_directory(self) -> Path:
         """Get the path to the session backup directory."""
         return self._backup_dir

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class TelegramClientManager:
    """Manages the Telegram client lifecycle, connection, and authentication."""
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls, config: AppConfig):
        if cls._instance is None:
            cls._instance = super(TelegramClientManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: AppConfig):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._config = config
            self._session_manager = SessionManager(config)
            self._client: Optional[TelegramClient] = None
            self._is_connected = False
            self._connection_lock = asyncio.Lock()
            self._auth_lock = asyncio.Lock()
            self._proxy = self._configure_proxy()
            self._initialized = True
            logger.info("TelegramClientManager initialized.")

    def _configure_proxy(self):
        """Configure proxy settings from environment variables."""
        proxy_url = os.getenv('TELEGRAM_PROXY')
        if proxy_url:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(proxy_url)
                scheme = parsed.scheme.lower() if parsed.scheme else 'socks5'
                host = parsed.hostname
                port = parsed.port
                username = parsed.username
                password = parsed.password

                if not host or not port:
                     raise ValueError("Proxy URL must include host and port")

                if scheme in ['socks5', 'http']:
                     proxy_config = (scheme, host, port, True, username, password)
                     logger.info(f"Proxy configured: {scheme}://{host}:{port}")
                     return proxy_config
                else:
                     logger.warning(f"Unsupported proxy scheme: {scheme}. Ignoring proxy.")
                     return None
            except Exception as e:
                logger.error(f"Failed to parse TELEGRAM_PROXY URL '{proxy_url}': {e}")
                return None
        return None

    async def get_client(self) -> TelegramClient:
        """Get an initialized and connected Telegram client."""
        async with self._connection_lock:
            if self._client and self._is_connected:
                try:
                    if not self._client.is_connected():
                         logger.warning("Client disconnected unexpectedly, attempting reconnect...")
                         self._is_connected = False
                    else:
                         # Removed get_me() check
                         return self._client
                except ConnectionError:
                     logger.warning("Connection error detected during status check, attempting reconnect...")
                     self._is_connected = False
                except Exception as e:
                     logger.warning(f"Error checking client status ({type(e).__name__}), attempting reconnect... Error: {e}")
                     self._is_connected = False

            if not self._client or not self._is_connected:
                logger.info("Client not initialized or disconnected. Initializing...")
                await self._initialize_client()
                if not self._client:
                     raise ConnectionError("Failed to initialize Telegram client after retries.")
            return self._client

    async def _initialize_client(self):
        """Initialize and connect the Telegram client with retries."""
        retries = self._config.max_retries
        delay = self._config.initial_retry_delay

        for attempt in range(retries):
            try:
                session_path = self._session_manager.session_file
                logger.info(f"Attempting to connect (Attempt {attempt + 1}/{retries}). Session: {session_path}")

                session_string = None
                session_file = Path(session_path)
                if session_file.exists():
                    try:
                        session_string = session_file.read_text().strip()
                        logger.debug("Loaded session string from file.")
                    except Exception as e:
                        logger.warning(f"Could not read session file {session_path}: {e}. Starting new session.")
                        session_string = None

                telethon_session = StringSession(session_string)

                client_kwargs = {
                    'session': telethon_session,
                    'api_id': self._config.api_id,
                    'api_hash': self._config.api_hash,
                    'connection_retries': 2,
                    'retry_delay': 1,
                    'auto_reconnect': True,
                    'request_retries': 2,
                    'timeout': 30,
                    'base_logger': logging.getLogger('telethon')
                }

                if self._proxy:
                    client_kwargs['proxy'] = self._proxy

                self._client = TelegramClient(**client_kwargs)
                await self._client.connect()

                if await self._client.is_user_authorized():
                    logger.info("Telegram client connected and authorized.")
                    self._is_connected = True
                    self._save_session_string(telethon_session.save())
                    return
                else:
                    logger.info("Client not authorized. Starting authentication flow...")
                    await self._handle_authentication()
                    if await self._client.is_user_authorized():
                         logger.info("Authentication successful.")
                         self._is_connected = True
                         self._save_session_string(telethon_session.save())
                         return
                    else:
                         logger.error("Authentication failed after attempting sign-in.")
                         await self.disconnect()
                         raise ConnectionAbortedError("Authentication failed. Please check code/password/2FA.")

            except FloodWaitError as e:
                wait_time = e.seconds + 5
                logger.warning(f"FloodWaitError: Waiting {wait_time} seconds before retry {attempt + 2}...")
                await asyncio.sleep(wait_time)
                continue

            except (ConnectionError, asyncio.TimeoutError, OSError) as e:
                logger.error(f"Connection error (Attempt {attempt + 1}/{retries}): {type(e).__name__} - {e}")
                if attempt < retries - 1:
                    sleep_time = delay * (2 ** attempt)
                    logger.info(f"Retrying connection in {sleep_time:.1f} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error("Max connection retries reached. Failed to connect.")
                    self._is_connected = False
                    raise ConnectionError(f"Failed to connect after {retries} attempts: {e}")

            except ConnectionAbortedError:
                 raise
            except Exception as e:
                logger.error(f"Unexpected error initializing client (Attempt {attempt + 1}/{retries}): {e}", exc_info=True)
                if attempt < retries - 1:
                    sleep_time = delay * (2 ** attempt)
                    logger.info(f"Retrying connection in {sleep_time:.1f} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error("Max retries reached after unexpected error. Failed to initialize.")
                    self._is_connected = False
                    raise ConnectionError(f"Failed to initialize client after {retries} attempts due to unexpected error: {e}")

        self._is_connected = False
        logger.critical("Client initialization loop completed without success or explicit failure.")
        raise ConnectionError("Failed to initialize Telegram client for an unknown reason.")

    async def _handle_authentication(self):
        """Handle the interactive Telegram authentication process."""
        if not self._client:
             logger.error("Client not initialized, cannot authenticate.")
             return

        async with self._auth_lock:
            try:
                 logger.info(f"Sending authentication code to phone number: {self._config.phone}")
                 logger.warning("Authentication requires interactive input (code/password).")
                 await self._client.send_code_request(self._config.phone)

                 code = input("Enter the Telegram authentication code you received: ").strip()
                 if not code:
                     raise ValueError("Authentication code cannot be empty.")

                 try:
                     await self._client.sign_in(self._config.phone, code)
                     logger.info("Signed in successfully using code.")
                 except SessionPasswordNeededError:
                     logger.info("Two-factor authentication (2FA) password needed.")
                     password = input("Enter your Telegram 2FA password: ").strip()
                     if not password:
                          raise ValueError("2FA password cannot be empty.")
                     await self._client.sign_in(password=password)
                     logger.info("Signed in successfully using 2FA password.")

            except FloodWaitError as e:
                 wait_time = e.seconds
                 logger.error(f"Authentication rate limited. Please wait {wait_time // 60} minutes and {wait_time % 60} seconds.")
                 raise
            except (ValueError, TypeError) as e:
                 logger.error(f"Invalid input during authentication: {e}")
                 raise ConnectionAbortedError(f"Authentication input error: {e}")
            except Exception as e:
                 logger.error(f"An unexpected error occurred during authentication: {e}", exc_info=True)
                 raise ConnectionAbortedError(f"Unexpected authentication error: {e}")
            finally:
                 if 'code' in locals(): del code
                 if 'password' in locals(): del password

    def _save_session_string(self, session_string: str):
        """Save the session string to the file."""
        session_file = Path(self._session_manager.session_file)
        try:
            session_file.parent.mkdir(parents=True, exist_ok=True)
            session_file.write_text(session_string)
            logger.debug(f"Session string saved to {session_file}")
        except Exception as e:
            logger.error(f"Failed to save session string to {session_file}: {e}")

    async def disconnect(self):
        """Disconnect the client gracefully."""
        async with self._connection_lock:
            if self._client and self._is_connected:
                logger.info("Disconnecting Telegram client...")
                try:
                    await self._client.disconnect()
                    logger.info("Telegram client disconnected successfully.")
                except Exception as e:
                    logger.error(f"Error during Telegram client disconnection: {e}", exc_info=True)
                finally:
                    self._is_connected = False
                    self._client = None
            else:
                 logger.debug("Client already disconnected or not initialized.")

    async def execute_with_retry(self, func, *args, **kwargs):
        """Execute a Telethon function with retry logic for common errors."""
        if not self._client:
            await self.get_client()
            if not self._client:
                 raise ConnectionError("Telegram client is not available.")

        retries = self._config.max_retries
        delay = self._config.initial_retry_delay

        for attempt in range(retries):
            try:
                if not self._is_connected or not self._client.is_connected():
                     logger.warning("Client disconnected before execution, attempting reconnect...")
                     await self.get_client()
                     if not self._client or not self._is_connected:
                          raise ConnectionError("Reconnect failed, cannot execute function.")

                result = await func(*args, **kwargs)
                return result

            except FloodWaitError as e:
                wait_time = e.seconds + 2
                logger.warning(f"FloodWaitError during '{func.__name__}': Waiting {wait_time} seconds (Attempt {attempt + 1}/{retries}).")
                if attempt < retries - 1:
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for FloodWaitError in '{func.__name__}'.")
                    raise

            except (ConnectionError, asyncio.TimeoutError, OSError) as e:
                logger.warning(f"Connection error during '{func.__name__}' (Attempt {attempt + 1}/{retries}): {type(e).__name__} - {e}")
                if attempt < retries - 1:
                    sleep_time = delay * (2 ** attempt)
                    logger.info(f"Retrying '{func.__name__}' in {sleep_time:.1f} seconds...")
                    await asyncio.sleep(sleep_time)
                    await self.disconnect()
                    await self.get_client()
                else:
                    logger.error(f"Max retries reached for connection error in '{func.__name__}'.")
                    raise

            except Exception as e:
                 logger.error(f"Unexpected error during '{func.__name__}' (Attempt {attempt + 1}/{retries}): {type(e).__name__} - {e}", exc_info=True)
                 if attempt < retries - 1:
                      sleep_time = delay * (2 ** attempt)
                      logger.info(f"Retrying '{func.__name__}' after unexpected error in {sleep_time:.1f} seconds...")
                      await asyncio.sleep(sleep_time)
                 else:
                      logger.error(f"Max retries reached for unexpected error in '{func.__name__}'.")
                      raise

        raise RuntimeError(f"Failed to execute '{func.__name__}' after {retries} retries.")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.get_client()
        return self._client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

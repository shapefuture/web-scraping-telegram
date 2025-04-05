import os
import re
import json
import logging
from dotenv import load_dotenv
from typing import List, Optional
from dataclasses import dataclass, field

# Ensure find_dotenv is imported if not already (it should be based on previous state)
from dotenv import load_dotenv, find_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
# Removed verbose loading and pre-check print statements
load_dotenv(override=True)


# Define expected headers here so they are configurable if needed via AppConfig
DEFAULT_EXPECTED_HEADERS = [
    'Date', 'Channel', 'Position', 'Email', 'What They Offer',
    'Application Link', 'Telegram Link', 'Salary', 'High Salary',
    'Schedule Type', 'Job Type', 'Fit %'
]
DEFAULT_FIT_KEYWORDS = ['python', 'javascript', 'react', 'node', 'web', 'full-stack', 'backend', 'frontend', 'remote', 'developer', 'engineer', 'software']
DEFAULT_SALARY_THRESHOLD = 100000

@dataclass
class AppConfig:
    """Application configuration data structure."""
    api_id: int
    api_hash: str
    phone: str
    google_sheet_id: str
    google_credentials_json: str
    channels: List[str]
    check_interval_hours: int = 1
    worksheet_name: str = 'Channel Messages' # Default worksheet name
    session_file: str = 'telegram_monitor.session' # Default session file name
    log_file: str = 'telegram_monitor.log'
    processed_messages_file: str = 'processed_messages.pkl'
    base_path: str = os.getcwd() # Default to current working directory
    expected_headers: List[str] = field(default_factory=lambda: list(DEFAULT_EXPECTED_HEADERS)) # Use list() to avoid modifying default

    # Constants with defaults, potentially overridden by env vars
    max_queue_size: int = 1000
    max_batch_size: int = 50
    queue_processing_interval: int = 30 # Seconds
    max_retries: int = 3
    initial_retry_delay: int = 1 # Seconds
    memory_limit_mb: int = 500
    circuit_breaker_threshold: int = 10
    circuit_breaker_timeout: int = 60 # Seconds
    message_store_key: Optional[str] = None # For encryption, loaded from env

    # Add defaults from utils.py
    salary_threshold: int = DEFAULT_SALARY_THRESHOLD
    fit_keywords: List[str] = field(default_factory=lambda: list(DEFAULT_FIT_KEYWORDS)) # Use list()

    # Message store settings
    message_store_save_interval: int = 300 # Seconds (5 minutes)
    message_store_max_backups: int = 5

    # Delay between processing channels
    channel_process_delay: int = 1 # Seconds


    def __post_init__(self):
        """Perform validation after initialization."""
        self._validate()

    def _validate(self):
        """Validate configuration values."""
        if not isinstance(self.api_id, int) or self.api_id <= 0:
            raise ValueError("API_ID must be a positive integer")
        if not (len(self.api_hash) == 32 and self.api_hash.isalnum()):
            raise ValueError("Invalid API_HASH format (expected 32 alphanumeric chars)")
        # Removed phone number debug print
        if not re.match(r'^\+\d{8,15}$', self.phone):
            raise ValueError("Phone number must be in international format (+XXX...)")
        if not self.google_sheet_id or len(self.google_sheet_id) < 10:
             raise ValueError("Invalid Google Sheet ID format")
        try:
            # Basic JSON structure check for service account
            creds = json.loads(self.google_credentials_json)
            if not isinstance(creds, dict) or \
               creds.get('type') != 'service_account' or \
               'private_key' not in creds or \
               'client_email' not in creds:
                 logger.warning("GOOGLE_CREDENTIALS_JSON seems incomplete or not a service account type.")
        except json.JSONDecodeError:
            raise ValueError("Invalid GOOGLE_CREDENTIALS_JSON format")
        if not isinstance(self.channels, list):
             raise ValueError("Channels must be a list of strings")
        for channel in self.channels:
            if not isinstance(channel, str) or not channel.strip():
                 raise ValueError("Invalid channel name found in list")
            # Optional: Add warning for non-standard channel formats
            # if not channel.startswith('@') and not channel.startswith('https://t.me/'):
            #     logger.warning(f"Channel '{channel}' does not start with '@' or 'https://t.me/'")
        if not isinstance(self.check_interval_hours, int) or self.check_interval_hours < 1:
            logger.warning("CHECK_INTERVAL_HOURS is less than 1, setting to 1")
            self.check_interval_hours = 1
        if not isinstance(self.worksheet_name, str) or not self.worksheet_name:
             raise ValueError("Worksheet name cannot be empty")
        if not isinstance(self.expected_headers, list) or not all(isinstance(h, str) for h in self.expected_headers):
             raise ValueError("expected_headers must be a list of strings")
        if not isinstance(self.fit_keywords, list) or not all(isinstance(k, str) for k in self.fit_keywords):
             raise ValueError("fit_keywords must be a list of strings")


        # Validate constants
        if self.max_queue_size <= 0: raise ValueError("max_queue_size must be positive")
        if self.max_batch_size <= 0: raise ValueError("max_batch_size must be positive")
        if self.queue_processing_interval <= 0: raise ValueError("queue_processing_interval must be positive")
        if self.max_retries < 0: raise ValueError("max_retries cannot be negative")
        if self.initial_retry_delay <= 0: raise ValueError("initial_retry_delay must be positive")
        if self.memory_limit_mb <= 0: raise ValueError("memory_limit_mb must be positive")
        if self.circuit_breaker_threshold <= 0: raise ValueError("circuit_breaker_threshold must be positive")
        if self.circuit_breaker_timeout <= 0: raise ValueError("circuit_breaker_timeout must be positive")
        if self.salary_threshold < 0: raise ValueError("salary_threshold cannot be negative")
        if self.message_store_save_interval <= 0: raise ValueError("message_store_save_interval must be positive")
        if self.message_store_max_backups < 0: raise ValueError("message_store_max_backups cannot be negative")
        if self.channel_process_delay < 0: raise ValueError("channel_process_delay cannot be negative")


def _get_env_var(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Helper to get environment variables."""
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value

def load_config() -> AppConfig:
    """Loads configuration from environment variables and returns an AppConfig object."""
    logger.info("Loading configuration...")
    try:
        # Required variables
        api_id_str = _get_env_var('API_ID', required=True)
        api_hash = _get_env_var('API_HASH', required=True)
        phone = _get_env_var('PHONE', required=True)
        google_sheet_id = _get_env_var('GOOGLE_SHEET_ID', required=True)
        google_credentials_json = _get_env_var('GOOGLE_CREDENTIALS_JSON', required=True)

        # Validate API ID early
        try:
            api_id = int(api_id_str)
        except ValueError:
            raise ValueError("API_ID must be an integer.")

        # Optional variables with defaults defined in AppConfig
        channels_raw = _get_env_var('CHANNELS', default=None) # Default handled later
        channels = [c.strip() for c in channels_raw.split(',') if c.strip()] if channels_raw else []

        # Load optional vars, using AppConfig defaults if not set or invalid
        check_interval_hours = int(_get_env_var('CHECK_INTERVAL_HOURS', str(AppConfig.check_interval_hours)) or AppConfig.check_interval_hours)
        worksheet_name = _get_env_var('WORKSHEET_NAME', AppConfig.worksheet_name)
        session_file = _get_env_var('SESSION_FILE', AppConfig.session_file)
        log_file = _get_env_var('LOG_FILE', AppConfig.log_file)
        processed_messages_file = _get_env_var('PROCESSED_MESSAGES_FILE', AppConfig.processed_messages_file)
        base_path = _get_env_var('BASE_PATH', AppConfig.base_path) # Uses os.getcwd() default now

        # Load constants from env vars, falling back to AppConfig defaults
        # Add try-except for integer conversions
        try:
            max_queue_size = int(_get_env_var('MAX_QUEUE_SIZE', str(AppConfig.max_queue_size)))
            max_batch_size = int(_get_env_var('MAX_BATCH_SIZE', str(AppConfig.max_batch_size)))
            queue_processing_interval = int(_get_env_var('QUEUE_PROCESSING_INTERVAL', str(AppConfig.queue_processing_interval)))
            max_retries = int(_get_env_var('MAX_RETRIES', str(AppConfig.max_retries)))
            initial_retry_delay = int(_get_env_var('INITIAL_RETRY_DELAY', str(AppConfig.initial_retry_delay)))
            memory_limit_mb = int(_get_env_var('MEMORY_LIMIT_MB', str(AppConfig.memory_limit_mb)))
            circuit_breaker_threshold = int(_get_env_var('CIRCUIT_BREAKER_THRESHOLD', str(AppConfig.circuit_breaker_threshold)))
            circuit_breaker_timeout = int(_get_env_var('CIRCUIT_BREAKER_TIMEOUT', str(AppConfig.circuit_breaker_timeout)))
            salary_threshold = int(_get_env_var('SALARY_THRESHOLD', str(AppConfig.salary_threshold)))
            message_store_save_interval = int(_get_env_var('MESSAGE_STORE_SAVE_INTERVAL', str(AppConfig.message_store_save_interval)))
            message_store_max_backups = int(_get_env_var('MESSAGE_STORE_MAX_BACKUPS', str(AppConfig.message_store_max_backups)))
            channel_process_delay = int(_get_env_var('CHANNEL_PROCESS_DELAY', str(AppConfig.channel_process_delay))) # Load channel delay
        except ValueError as e:
            raise ValueError(f"Invalid integer value in environment variable for constants: {e}")

        message_store_key = _get_env_var('MESSAGE_STORE_KEY') # Optional, default is None in AppConfig

        # Load fit keywords from env var if present, otherwise use default
        fit_keywords_raw = _get_env_var('FIT_KEYWORDS', default=None)
        # Use the predefined default list as fallback
        fit_keywords = [k.strip() for k in fit_keywords_raw.split(',') if k.strip()] if fit_keywords_raw else DEFAULT_FIT_KEYWORDS

        # Load expected headers from env var if present, otherwise use default
        expected_headers_raw = _get_env_var('EXPECTED_HEADERS', default=None)
        # Use the predefined default list as fallback
        expected_headers = [h.strip() for h in expected_headers_raw.split(',') if h.strip()] if expected_headers_raw else DEFAULT_EXPECTED_HEADERS


        # Create AppConfig instance - validation happens in __post_init__
        config = AppConfig(
            api_id=api_id, # Already converted
            api_hash=api_hash,
            phone=phone,
            google_sheet_id=google_sheet_id,
            google_credentials_json=google_credentials_json,
            channels=channels,
            check_interval_hours=check_interval_hours,
            worksheet_name=worksheet_name,
            session_file=session_file,
            log_file=log_file,
            processed_messages_file=processed_messages_file,
            base_path=base_path,
            # Pass loaded constants
            max_queue_size=max_queue_size,
            max_batch_size=max_batch_size,
            queue_processing_interval=queue_processing_interval,
            max_retries=max_retries,
            initial_retry_delay=initial_retry_delay,
            memory_limit_mb=memory_limit_mb,
            circuit_breaker_threshold=circuit_breaker_threshold,
            circuit_breaker_timeout=circuit_breaker_timeout,
            message_store_key=message_store_key,
            salary_threshold=salary_threshold,
            fit_keywords=fit_keywords,
            expected_headers=expected_headers,
            message_store_save_interval=message_store_save_interval,
            message_store_max_backups=message_store_max_backups,
            channel_process_delay=channel_process_delay # Add channel delay
        )
        logger.info("Configuration loaded successfully.")
        # Log loaded config excluding sensitive details
        log_loaded_config(config)
        return config

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise SystemExit(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading configuration: {e}", exc_info=True)
        raise SystemExit("Failed to load configuration.")

def log_loaded_config(config: AppConfig):
    """Logs the loaded configuration, masking sensitive values."""
    logger.info("--- Loaded Configuration ---")
    logger.info(f"  API_ID: {config.api_id}")
    logger.info(f"  API_HASH: {'*' * len(config.api_hash) if config.api_hash else 'Not Set'}")
    logger.info(f"  PHONE: {config.phone[:4]}{'*' * (len(config.phone) - 7)}{config.phone[-3:]}") # Mask phone number
    logger.info(f"  GOOGLE_SHEET_ID: {config.google_sheet_id[:4]}{'*' * (len(config.google_sheet_id) - 8)}{config.google_sheet_id[-4:]}") # Mask Sheet ID
    logger.info(f"  GOOGLE_CREDENTIALS_JSON: {'Loaded' if config.google_credentials_json else 'Not Set'}")
    logger.info(f"  CHANNELS: {config.channels}") # Maybe truncate if very long?
    logger.info(f"  CHECK_INTERVAL_HOURS: {config.check_interval_hours}")
    logger.info(f"  WORKSHEET_NAME: {config.worksheet_name}")
    logger.info(f"  SESSION_FILE: {config.session_file}")
    logger.info(f"  LOG_FILE: {config.log_file}")
    logger.info(f"  PROCESSED_MESSAGES_FILE: {config.processed_messages_file}")
    logger.info(f"  BASE_PATH: {config.base_path}")
    logger.info(f"  MAX_QUEUE_SIZE: {config.max_queue_size}")
    logger.info(f"  MAX_BATCH_SIZE: {config.max_batch_size}")
    logger.info(f"  QUEUE_PROCESSING_INTERVAL: {config.queue_processing_interval}s")
    logger.info(f"  MAX_RETRIES: {config.max_retries}")
    logger.info(f"  INITIAL_RETRY_DELAY: {config.initial_retry_delay}s")
    logger.info(f"  MEMORY_LIMIT_MB: {config.memory_limit_mb}MB")
    logger.info(f"  CIRCUIT_BREAKER_THRESHOLD: {config.circuit_breaker_threshold}")
    logger.info(f"  CIRCUIT_BREAKER_TIMEOUT: {config.circuit_breaker_timeout}s")
    logger.info(f"  MESSAGE_STORE_KEY: {'Set' if config.message_store_key else 'Not Set (Encryption Disabled)'}")
    logger.info(f"  SALARY_THRESHOLD: {config.salary_threshold}")
    logger.info(f"  FIT_KEYWORDS: {config.fit_keywords}")
    logger.info(f"  EXPECTED_HEADERS: {config.expected_headers}")
    logger.info(f"  MSG_STORE_SAVE_INTERVAL: {config.message_store_save_interval}s")
    logger.info(f"  MSG_STORE_MAX_BACKUPS: {config.message_store_max_backups}")
    logger.info(f"  CHANNEL_PROCESS_DELAY: {config.channel_process_delay}s")
    logger.info("--------------------------")

# Example usage:
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     try:
#         config = load_config()
#         print("Config loaded successfully:")
#         # print(config) # Avoid printing sensitive data directly
#     except SystemExit as e:
#         print(f"Failed to load config: {e}")

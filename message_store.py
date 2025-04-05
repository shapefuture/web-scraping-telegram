import asyncio
import logging
import os
import pickle
import time
import base64
from datetime import datetime
from pathlib import Path
import shutil # Added
import tempfile # Added
from typing import Set, Optional # Optional added

import aiofiles
# Ensure cryptography is installed: pip install cryptography
try:
    from cryptography.fernet import Fernet, InvalidToken
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    Fernet = None # Define Fernet as None if cryptography is not installed
    InvalidToken = None
    CRYPTOGRAPHY_AVAILABLE = False
    logging.getLogger(__name__).warning("Cryptography library not found. MessageStore encryption disabled. Install with 'pip install cryptography'")

from config_loader import AppConfig

logger = logging.getLogger(__name__)

class MessageStore:
    """Handles persistence and state management for processed message IDs with optional encryption."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.base_path = Path(config.base_path).resolve()
        self.messages_file = self.base_path / config.processed_messages_file
        self.backup_dir = self.base_path / 'message_backups'
        self._lock = asyncio.Lock()
        self._messages: Set[int] = set()
        self._last_save_time = 0
        # Use values from config
        self._save_interval = config.message_store_save_interval
        self._max_backups = config.message_store_max_backups
        self._encryption_key = self._get_encryption_key() # Returns None if key missing/invalid or crypto lib missing
        self._fernet: Optional[Fernet] = None

        if self._encryption_key and CRYPTOGRAPHY_AVAILABLE:
            try:
                self._fernet = Fernet(self._encryption_key)
                logger.info("MessageStore encryption enabled.")
            except Exception as e:
                 logger.error(f"Failed to initialize Fernet cipher with provided key: {e}. Encryption will be disabled.")
                 self._fernet = None
                 # Optionally raise an error here if encryption is considered mandatory
                 # raise ValueError("Failed to initialize encryption key.") from e
        elif CRYPTOGRAPHY_AVAILABLE and not self._encryption_key:
             # If crypto is available but key is missing, warn strongly or raise error
             logger.error("MESSAGE_STORE_KEY is not set in config or environment. Cannot enable encryption.")
             # raise ValueError("MESSAGE_STORE_KEY is required for encryption.") # Make encryption mandatory
             logger.warning("Proceeding with unencrypted message store. SET MESSAGE_STORE_KEY for security.")
        elif not CRYPTOGRAPHY_AVAILABLE:
             logger.warning("Cryptography library not installed. Message store will be unencrypted.")
        # No 'else' needed, covers all cases

        self.backup_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"MessageStore initialized. File: {self.messages_file}")

    def _get_encryption_key(self) -> Optional[bytes]:
        """
        Get and validate the encryption key from config/environment.
        Returns a URL-safe base64 encoded key suitable for Fernet, or None.
        """
        if not CRYPTOGRAPHY_AVAILABLE:
            return None # Cannot use encryption without the library

        key_str = self.config.message_store_key or os.getenv('MESSAGE_STORE_KEY')
        if not key_str:
            # Key is missing, return None (caller decides whether to raise error or proceed unencrypted)
            return None

        # Assume key_str is the raw secret key. We need to derive a Fernet key.
        # IMPORTANT: For production, use a proper key derivation function (like PBKDF2HMAC)
        # if the input key_str is a password. If key_str is already a high-entropy key,
        # ensure it's 32 bytes. Here, we'll use a simple (less secure) approach
        # for demonstration, assuming key_str needs encoding.
        try:
            # Use first 32 bytes of the key string, encode to bytes, then base64 encode
            raw_key = key_str.encode('utf-8')
            # Ensure raw key is 32 bytes (pad or truncate - truncation is safer)
            if len(raw_key) < 32:
                 # Padding is generally insecure for cryptographic keys
                 # raise ValueError("MESSAGE_STORE_KEY must be at least 32 bytes long.")
                 logger.warning("MESSAGE_STORE_KEY is less than 32 bytes. Padding with null bytes (insecure).")
                 raw_key = raw_key.ljust(32, b'\0')
            elif len(raw_key) > 32:
                 logger.warning("MESSAGE_STORE_KEY is longer than 32 bytes. Truncating.")
                 raw_key = raw_key[:32]

            fernet_key = base64.urlsafe_b64encode(raw_key)
            # Test if the key is valid for Fernet
            Fernet(fernet_key)
            return fernet_key
        except Exception as e:
            logger.error(f"Error processing MESSAGE_STORE_KEY: {e}. Check its format and length. Encryption disabled.")
            return None

    def _encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data using Fernet if enabled."""
        if self._fernet:
            try:
                return self._fernet.encrypt(data)
            except Exception as e:
                 logger.error(f"Encryption failed unexpectedly: {e}", exc_info=True)
                 # Don't return raw data if encryption was expected but failed.
                 raise ValueError("Failed to encrypt message store data.") from e
        return data # Return raw data if encryption is disabled

    def _decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using Fernet if enabled."""
        if self._fernet:
            try:
                return self._fernet.decrypt(encrypted_data)
            except InvalidToken:
                 logger.error("Decryption failed: Invalid token (key mismatch or data corruption).")
                 raise # Re-raise specific error for handling
            except Exception as e:
                logger.error(f"Decryption failed: {e}")
                raise # Re-raise for handling
        return encrypted_data # No encryption

    async def load(self):
        """Load processed message IDs from the file."""
        async with self._lock:
            logger.debug(f"Attempting to load messages from {self.messages_file}")
            if not self.messages_file.exists():
                logger.info("No existing messages file found. Starting with an empty set.")
                self._messages = set()
                return self._messages

            try:
                async with aiofiles.open(self.messages_file, 'rb') as f:
                    raw_data = await f.read()

                decrypted_data = self._decrypt_data(raw_data) # Raises InvalidToken on failure if encrypted

                # WARNING: Unpickling data can be insecure if the source file is compromised.
                # Consider using a safer format like JSON if feasible.
                loaded_messages = pickle.loads(decrypted_data)

                if isinstance(loaded_messages, set):
                    self._messages = loaded_messages
                    logger.info(f"Successfully loaded {len(self._messages)} processed message IDs.")
                else:
                    logger.warning(f"Loaded data is not a set (type: {type(loaded_messages)}). Discarding and starting fresh.")
                    self._messages = set()
                    # Consider backing up the invalid file
                    self._backup_invalid_file("invalid_type")

            except FileNotFoundError:
                 logger.info("Messages file not found on load (race condition?). Starting fresh.")
                 self._messages = set()
            except (pickle.UnpicklingError, EOFError, TypeError, ValueError) as e:
                logger.error(f"Failed to unpickle message data from {self.messages_file} (corrupted?): {e}")
                self._messages = set()
                self._backup_invalid_file("unpickle_error")
            except InvalidToken: # Specific decryption error
                 logger.error(f"Failed to decrypt {self.messages_file}. Key might have changed or file is corrupted.")
                 self._messages = set()
                 self._backup_invalid_file("decryption_error")
            except Exception as e:
                logger.error(f"Unexpected error loading messages from {self.messages_file}: {e}", exc_info=True)
                self._messages = set() # Start fresh on unknown errors
                self._backup_invalid_file("load_error")

            return self._messages

    def _backup_invalid_file(self, reason: str):
        """Create a backup of the problematic messages file."""
        if self.messages_file.exists():
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = self.messages_file.with_suffix(f".invalid_{reason}_{timestamp}")
                shutil.move(str(self.messages_file), str(backup_path))
                logger.warning(f"Moved problematic messages file to {backup_path}")
            except Exception as e:
                logger.error(f"Could not back up problematic messages file {self.messages_file}: {e}")


    async def save(self, force: bool = False):
        """Save the current set of processed message IDs to the file."""
        current_time = time.monotonic()
        if not force and current_time - self._last_save_time < self._save_interval:
            # logger.debug("Skipping periodic save, interval not reached.")
            return

        async with self._lock:
            logger.debug(f"Attempting to save {len(self._messages)} message IDs...")
            temp_file_path = None
            try:
                # Serialize data using pickle
                raw_data = pickle.dumps(self._messages)
                # Encrypt data if enabled (raises ValueError on failure)
                data_to_write = self._encrypt_data(raw_data)

                # Write to a temporary file first for atomicity
                # Use aiofiles within an executor for sync file operations like delete/rename
                loop = asyncio.get_running_loop()
                temp_dir = self.messages_file.parent
                # Create a temporary file in the same directory
                fd, temp_file_path_str = await loop.run_in_executor(
                    None, tempfile.mkstemp, ".tmp", self.messages_file.name + '_', temp_dir
                )
                temp_file_path = Path(temp_file_path_str)

                # Write data asynchronously
                async with aiofiles.open(fd, 'wb') as f: # Open using file descriptor
                    await f.write(data_to_write)

                # Atomically replace the original file with the temporary file
                await loop.run_in_executor(None, os.replace, temp_file_path, self.messages_file)
                temp_file_path = None # Indicate successful move

                self._last_save_time = current_time
                logger.info(f"Saved {len(self._messages)} message IDs to {self.messages_file}")

                # Create backup (after successful primary save)
                # Corrected: Pass the actual data that was written
                await self._create_backup(data_to_write)

            except Exception as e:
                logger.error(f"Error saving messages to {self.messages_file}: {e}", exc_info=True)
            finally:
                # Clean up temporary file if it still exists (i.e., rename failed)
                if temp_file_path and temp_file_path.exists():
                    loop = asyncio.get_running_loop()
                    try:
                        await loop.run_in_executor(None, os.remove, temp_file_path)
                        logger.warning(f"Removed temporary save file due to error: {temp_file_path}")
                    except OSError as rm_err:
                        logger.error(f"Error removing temporary save file {temp_file_path}: {rm_err}")


    async def _create_backup(self, data_to_backup: bytes):
        """Create a timestamped backup file."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = self.backup_dir / f'{self.messages_file.stem}_{timestamp}{self.messages_file.suffix}'
            async with aiofiles.open(backup_file, 'wb') as f:
                await f.write(data_to_backup)
            logger.debug(f"Created backup: {backup_file.name}")

            # Clean up old backups
            await self._cleanup_old_backups()

        except Exception as e:
            logger.error(f"Error creating message backup: {e}")

    async def _cleanup_old_backups(self):
        """Remove oldest backup files if exceeding the limit."""
        try:
            # Use glob to find backup files matching the pattern
            backup_pattern = f'{self.messages_file.stem}_*{self.messages_file.suffix}'
            backups = sorted(
                [f for f in self.backup_dir.glob(backup_pattern) if f.is_file()],
                key=os.path.getmtime
            )

            # Use configured max backups
            if len(backups) > self._max_backups:
                num_to_delete = len(backups) - self._max_backups
                logger.debug(f"Found {len(backups)} backups, removing {num_to_delete} oldest...")
                loop = asyncio.get_running_loop() # Get loop for executor
                for i in range(num_to_delete):
                    try:
                        # Run synchronous os.remove in an executor
                        await loop.run_in_executor(None, os.remove, str(backups[i]))
                        logger.debug(f"Removed old backup: {backups[i].name}")
                    except OSError as e:
                        logger.warning(f"Error removing backup file {backups[i].name}: {e}")
        except Exception as e:
            logger.error(f"Error during backup cleanup: {e}")

    def add(self, message_id: int):
        """Add a message ID to the processed set."""
        # No lock needed for adding to a set if reads don't happen concurrently with writes
        # But save() is async and locked, so adding should be fine.
        self._messages.add(message_id)

    def __contains__(self, message_id: int) -> bool:
        """Check if a message ID has been processed."""
        # Reading from set is thread-safe/async-safe
        return message_id in self._messages

    async def cleanup(self):
        """Perform final save on cleanup."""
        logger.info("MessageStore cleanup: performing final save...")
        await self.save(force=True)
        # Optional: Add cleanup for very old messages from the set itself
        # await self._cleanup_old_messages_from_set()
        logger.info("MessageStore cleanup complete.")

    async def __aenter__(self):
        """Async context manager entry: Load messages."""
        await self.load()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Perform cleanup."""
        await self.cleanup()

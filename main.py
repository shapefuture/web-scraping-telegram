import asyncio
import logging
import signal
import sys
import os
import gc
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Import refactored components
from config_loader import load_config, AppConfig
from utils import parse_job_vacancy
from telegram_client_manager import TelegramClientManager
from sheets_manager import SheetManager
from queue_manager import QueueManager
from message_store import MessageStore # Moved import to top level

# Global logger instance
logger = logging.getLogger(__name__)

def setup_logging(config: AppConfig):
    """Set up logging configuration with file rotation."""
    log_level = logging.INFO # Or load from config if needed
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Ensure log directory exists
    log_file_path = Path(config.base_path) / config.log_file
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    logging.basicConfig(level=log_level, format=log_format, handlers=[logging.StreamHandler(sys.stdout)])

    # Add rotating file handler
    # Rotate log file when it reaches 5MB, keep 3 backup logs
    file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(file_handler) # Add handler to root logger

    # Optional: Set higher level for noisy libraries
    logging.getLogger('telethon').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    logging.getLogger('oauth2client').setLevel(logging.WARNING)
    logging.getLogger('gspread').setLevel(logging.WARNING)

    logger.info("Logging setup complete.")


async def process_channel_messages(client_manager: TelegramClientManager, client, channel_entity, message_store, queue_manager, config: AppConfig):
    """Fetch and process messages from a single channel."""
    channel_title = getattr(channel_entity, 'title', str(channel_entity.id))
    logger.info(f"Processing messages for channel: {channel_title}")
    processed_count = 0
    new_jobs_found = 0

    try:
        # Fetch messages using the client manager's retry mechanism
        # TODO: Consider using iter_messages with offset_id for robustness against missed messages
        # Current limit=100 might miss messages in high-volume channels between checks.
        # Limit could be made configurable via AppConfig.
        fetch_limit = 100 # Example limit
        logger.debug(f"Fetching last {fetch_limit} messages from {channel_title}...")
        messages = await client_manager.execute_with_retry(
            client.get_messages,
            channel_entity,
            limit=fetch_limit
        )

        if not messages:
            logger.info(f"No messages found or retrieved for channel: {channel_title}")
            return

        logger.debug(f"Retrieved {len(messages)} messages from {channel_title}")

        for message in messages:
            if not message or not message.id:
                 logger.debug("Skipping empty or invalid message object.")
                 continue

            # Check if message already processed using MessageStore
            if message.id in message_store:
                # logger.debug(f"Skipping already processed message ID {message.id} from {channel_title}")
                continue

            processed_count += 1
            if message.text:
                # Parse the message text using the utility function
                # Pass message timestamp and configurable parameters
                job_data = parse_job_vacancy(
                    text=message.text,
                    channel_title=channel_title,
                    message_timestamp=message.date, # Use message timestamp
                    salary_threshold=config.salary_threshold,
                    fit_keywords=config.fit_keywords
                )

                if job_data and job_data.get('position'): # Ensure a position was found
                    logger.info(f"Found potential job: '{job_data['position']}' in {channel_title} (Msg ID: {message.id})")
                    # Add the job data dictionary to the queue manager
                    await queue_manager.add_to_queue(job_data)
                    new_jobs_found += 1
                # else:
                    # logger.debug(f"Message ID {message.id} from {channel_title} did not parse as a valid job or lacked position.")

            # Add message ID to store regardless of whether it was a job, to avoid re-processing
            message_store.add(message.id)

        logger.info(f"Finished processing {processed_count} new messages for {channel_title}. Found {new_jobs_found} potential jobs.")

        # Removed message_store.save() from here - will save at the end of monitor_channels

    except Exception as e:
        logger.error(f"Error processing messages for channel {channel_title}: {e}", exc_info=True)
        # Continue to the next channel


async def monitor_channels(client_manager: TelegramClientManager, message_store, queue_manager: QueueManager, config: AppConfig):
    """Monitor configured Telegram channels for new messages."""
    logger.info(f"Starting channel monitoring run. Monitoring {len(config.channels)} channels.")
    client = await client_manager.get_client() # Ensure client is ready

    # Process channels sequentially or concurrently? Sequential is simpler for rate limits.
    for channel_identifier in config.channels:
        try:
            logger.debug(f"Getting entity for channel identifier: {channel_identifier}")
            # Get channel entity using client manager's retry mechanism
            channel_entity = await client_manager.execute_with_retry(
                client.get_entity,
                channel_identifier
            )

            if not channel_entity:
                 logger.warning(f"Could not find entity for channel: {channel_identifier}. Skipping.")
                 continue

            # Process messages for this channel
            # Pass client_manager instance
            await process_channel_messages(client_manager, client, channel_entity, message_store, queue_manager, config)

            # Use configurable delay between processing channels
            # Check attribute existence before accessing
            if hasattr(config, 'channel_process_delay') and config.channel_process_delay > 0:
                 logger.debug(f"Waiting {config.channel_process_delay}s before next channel...")
                 await asyncio.sleep(config.channel_process_delay)

        except ValueError as e:
             # Handle potential errors from get_entity if identifier is invalid
             logger.error(f"Could not resolve channel identifier '{channel_identifier}': {e}. Ensure it's a valid username, link, or ID.")
        except Exception as e:
            # Catch errors during entity fetching or message processing for a single channel
            logger.error(f"Failed to process channel '{channel_identifier}': {e}", exc_info=True)
            # Continue with the next channel

        # Removed explicit gc.collect() call

    logger.info("Finished channel monitoring run.")
    # Save message store state once after processing all channels
    await message_store.save(force=True)
    # Final queue processing after checking all channels in this run
    await queue_manager.process_queues(force=True)


async def run_app(config: AppConfig):
    """Main application function: sets up managers and runs the monitoring loop."""
    # Instantiate managers
    # Note: TelegramClientManager uses SessionManager internally
    client_manager = TelegramClientManager(config)
    sheet_manager = SheetManager(config)
    queue_manager = QueueManager(config, sheet_manager)
    # MessageStore import moved to top level

    try:
        # Initialize components sequentially, handling potential startup errors
        logger.info("Initializing application components...")
        async with sheet_manager, queue_manager, MessageStore(config) as message_store:
            # Initialize Telegram client last, as it might require interaction
            async with client_manager:
                logger.info("Application components initialized successfully.")

                # Main monitoring loop
                while True:
                    logger.info("Starting new monitoring cycle...")
                    await monitor_channels(client_manager, message_store, queue_manager, config)

                    logger.info(f"Monitoring cycle complete. Waiting {config.check_interval_hours} hour(s) for next cycle.")
                    # Wait for the configured interval
                    await asyncio.sleep(config.check_interval_hours * 3600)

    except ConnectionError as e:
        # Specific handling for Telegram connection errors during startup/runtime
        logger.critical(f"Telegram connection error: {e}. Application cannot continue.", exc_info=True)
        # Perform minimal cleanup if possible (logging already handled by finally)
    except asyncio.CancelledError:
        async with client_manager, sheet_manager, queue_manager, MessageStore(config) as message_store:
            logger.info("Application components initialized successfully.")

            while True: # Main monitoring loop
                logger.info("Starting new monitoring cycle...")
                await monitor_channels(client_manager, message_store, queue_manager, config)

                logger.info(f"Monitoring cycle complete. Waiting {config.check_interval_hours} hour(s) for next cycle.")
                # Wait for the configured interval
                await asyncio.sleep(config.check_interval_hours * 3600)

    except asyncio.CancelledError:
        logger.info("Main application loop cancelled.")
    except Exception as e:
        logger.critical(f"Fatal error in main application loop: {e}", exc_info=True)
    finally:
        logger.info("Application shutdown sequence initiated.")
        # Context managers (__aexit__) will handle cleanup for each component


def main():
    """Synchronous entry point."""
    config = None
    try:
        # Load configuration first
        config = load_config()

        # Setup logging using loaded config
        setup_logging(config)

        logger.info("=== Telegram Job Monitor Starting ===")

        # Run the main async application
        asyncio.run(run_app(config))

    except (SystemExit, KeyboardInterrupt) as e:
         logger.info(f"Application interrupted ({type(e).__name__}). Exiting.")
         # Cleanup should be handled by atexit/signal handlers in managers if asyncio.run is interrupted
    except Exception as e:
        # Catch errors during config loading or initial setup
        logger.critical(f"Application failed to start: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=== Telegram Job Monitor Stopped ===")
        logging.shutdown() # Ensure all logs are flushed


if __name__ == "__main__":
    main()

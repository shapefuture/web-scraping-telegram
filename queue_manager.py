import asyncio
import gc
import logging
import os # Added import
import time
from collections import deque
from typing import List, Dict, Any, Optional

# Assuming AppConfig holds relevant settings like thresholds, limits, etc.
from config_loader import AppConfig
# Import SheetManager to interact with it for saving batches
from sheets_manager import SheetManager

logger = logging.getLogger(__name__)

# Optional: Import psutil for memory checks, handle ImportError
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None # Define psutil as None if not available
    PSUTIL_AVAILABLE = False
    logger.warning("psutil library not found. Memory usage checks will be disabled.")


class CircuitBreaker:
    """Implements the Circuit Breaker pattern to handle external service failures."""
    STATE_CLOSED = "CLOSED"
    STATE_OPEN = "OPEN"
    STATE_HALF_OPEN = "HALF_OPEN"

    def __init__(self, config: AppConfig):
        self.failure_threshold = config.circuit_breaker_threshold
        self.reset_timeout = config.circuit_breaker_timeout
        self._failures = 0
        self._last_failure_time = 0
        self._state = self.STATE_CLOSED
        self._lock = asyncio.Lock()
        # Optional: Add stats tracking if needed
        logger.info(f"CircuitBreaker initialized: Threshold={self.failure_threshold}, Timeout={self.reset_timeout}s")

    @property
    def state(self) -> str:
        """Get the current state of the circuit breaker."""
        return self._state

    async def record_success(self):
        """Record a successful operation, closing the circuit if it was half-open."""
        async with self._lock:
            if self._state == self.STATE_HALF_OPEN:
                logger.info("Circuit breaker closed after successful operation in HALF-OPEN state.")
                self._state = self.STATE_CLOSED
                self._failures = 0
            elif self._state == self.STATE_CLOSED:
                 # Reset failures on success even in closed state if needed
                 if self._failures > 0:
                      logger.debug("Resetting failure count after success in CLOSED state.")
                      self._failures = 0
            # No change needed if OPEN

    async def record_failure(self):
        """Record a failed operation, potentially opening the circuit."""
        async with self._lock:
            self._failures += 1
            self._last_failure_time = time.monotonic()
            logger.debug(f"Circuit breaker failure recorded. Count: {self._failures}/{self.failure_threshold}")

            if self._state == self.STATE_HALF_OPEN:
                logger.warning("Circuit breaker opened again after failure in HALF-OPEN state.")
                self._state = self.STATE_OPEN
            elif self._failures >= self.failure_threshold and self._state == self.STATE_CLOSED:
                logger.warning(f"Circuit breaker opened due to reaching failure threshold ({self.failure_threshold}).")
                self._state = self.STATE_OPEN

    async def is_open(self) -> bool:
        """Check if the circuit is open, potentially moving to half-open."""
        async with self._lock:
            if self._state == self.STATE_OPEN:
                time_since_last_failure = time.monotonic() - self._last_failure_time
                if time_since_last_failure >= self.reset_timeout:
                    logger.info(f"Circuit breaker reset timeout ({self.reset_timeout}s) reached. Moving to HALF-OPEN state.")
                    self._state = self.STATE_HALF_OPEN
                    self._failures = 0 # Reset failures for half-open test
                    return False # Allow one attempt in half-open
                else:
                    # Still within timeout, circuit remains open
                    return True
            # Circuit is CLOSED or HALF-OPEN, allow operation
            return False

    async def reset(self):
        """Manually reset the circuit breaker to the closed state."""
        async with self._lock:
            logger.info("Circuit breaker manually reset to CLOSED state.")
            self._state = self.STATE_CLOSED
            self._failures = 0
            self._last_failure_time = 0


class QueueManager:
    """Manages job queues, batch processing, retries, and interacts with SheetManager."""

    def __init__(self, config: AppConfig, sheet_manager: SheetManager):
        self.config = config
        self.sheet_manager = sheet_manager
        # Use deque for efficient appends/pops from left
        self._queues: Dict[str, deque] = {
            "high_salary": deque(),
            "low_salary": deque()
        }
        self._failed_items: Dict[str, List[Dict[str, Any]]] = {
             "high_salary": [],
             "high_salary": [],
             "low_salary": []
        }
        self._lock = asyncio.Lock() # General lock for queue modifications
        self._processing_lock = asyncio.Lock() # Lock to prevent concurrent processing runs
        # Separate circuit breakers per queue type
        self._circuit_breakers: Dict[str, CircuitBreaker] = {
            "high_salary": CircuitBreaker(config),
            "low_salary": CircuitBreaker(config)
        }
        self._last_memory_check_time = 0
        self._memory_check_interval = 60 # Seconds
        self._last_batch_process_time = 0
        self._shutdown_signal = asyncio.Event()

        # Optional: Stats tracking
        self._stats = {
            'processed_items': 0,
            'failed_items_initial': 0, # Failures before adding to failed_items queue
            'failed_items_persistent': 0, # Items moved to failed_items queue
            'retry_attempts': 0,
            'batches_processed': 0,
            'memory_usage_mb': None
        }
        logger.info("QueueManager initialized.")

    async def add_to_queue(self, job_data: Dict[str, Any]):
        """Adds a job dictionary to the appropriate queue."""
        if not isinstance(job_data, dict):
             logger.warning(f"Invalid job_data type received: {type(job_data)}. Skipping.")
             return

        queue_type = "high_salary" if job_data.get('high_salary', False) else "low_salary"
        async with self._lock:
            queue = self._queues[queue_type]
            if len(queue) >= self.config.max_queue_size:
                logger.warning(f"{queue_type.replace('_', ' ').title()} queue is full (size {len(queue)} >= {self.config.max_queue_size}). Forcing processing.")
                # Schedule processing instead of blocking the add operation
                asyncio.create_task(self.process_queues())
                # Optional: Implement strategy for full queue (e.g., drop oldest, wait)
                # For now, we rely on processing to clear space.

            queue.append(job_data)
            logger.debug(f"Added job '{job_data.get('position', 'N/A')}' to {queue_type} queue. Size: {len(queue)}")

            # Trigger processing if batch size is reached
            if len(queue) >= self.config.max_batch_size:
                 logger.debug(f"{queue_type} queue reached batch size ({self.config.max_batch_size}). Triggering processing.")
                 asyncio.create_task(self.process_queues()) # Non-blocking task

    async def _check_memory_usage(self):
        """Check memory usage and log if above threshold."""
        if not PSUTIL_AVAILABLE:
            return # Skip if psutil not installed

        current_time = time.monotonic()
        if current_time - self._last_memory_check_time < self._memory_check_interval:
            return

        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            self._stats['memory_usage_mb'] = round(memory_mb, 2)
            self._last_memory_check_time = current_time

            logger.debug(f"Memory usage: {memory_mb:.2f} MB")

            if memory_mb > self.config.memory_limit_mb:
                logger.warning(f"High memory usage detected: {memory_mb:.2f} MB (Limit: {self.config.memory_limit_mb} MB). Forcing GC.")
                gc.collect() # Force garbage collection
                # Re-check after GC
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                logger.info(f"Memory usage after GC: {memory_mb:.2f} MB")
                # Optional: Trigger queue processing if memory is still high
                # if memory_mb > self.config.memory_limit_mb:
                #     logger.warning("Memory still high after GC, forcing queue processing.")
                #     asyncio.create_task(self.process_queues(force=True))

        except Exception as e:
            logger.error(f"Error checking memory usage: {e}")

    async def process_queues(self, force: bool = False):
        """Processes both queues in batches if conditions are met."""
        # Prevent concurrent processing runs
        if self._processing_lock.locked():
            logger.debug("Processing already in progress, skipping.")
            return

        async with self._processing_lock:
            logger.debug("Starting queue processing cycle.")
            await self._check_memory_usage() # Check memory before processing

            # Check if enough time has passed since last batch processing
            time_since_last_batch = time.monotonic() - self._last_batch_process_time
            if not force and time_since_last_batch < self.config.queue_processing_interval:
                 logger.debug(f"Skipping scheduled processing: Only {time_since_last_batch:.1f}s passed (interval: {self.config.queue_processing_interval}s).")
                 logger.debug(f"Skipping scheduled processing: Only {time_since_last_batch:.1f}s passed (interval: {self.config.queue_processing_interval}s).")
                 return

            # Check circuit breakers individually before processing each queue
            # Note: This check is slightly redundant as it's also checked in _process_single_batch,
            # but it prevents unnecessary batch extraction if the breaker is already open.
            # if await self._circuit_breakers['high_salary'].is_open() and await self._circuit_breakers['low_salary'].is_open():
            #     logger.warning("Both circuit breakers are OPEN. Skipping processing cycle.")
            #     return

            processed_batch = False
            for queue_type in ["high_salary", "low_salary"]:
                async with self._lock: # Lock during batch extraction
                    queue = self._queues[queue_type]
                    if not queue:
                        continue # Skip empty queue

                    # Extract batch without blocking adds for too long
                    batch_to_process = []
                    count = 0
                    while queue and count < self.config.max_batch_size:
                         batch_to_process.append(queue.popleft())
                         count += 1

                if batch_to_process:
                    logger.info(f"Processing batch of {len(batch_to_process)} items from {queue_type} queue.")

                    # Check specific circuit breaker before processing batch
                    circuit_breaker = self._circuit_breakers[queue_type]
                    if await circuit_breaker.is_open():
                        logger.warning(f"Circuit breaker for {queue_type} is OPEN. Re-queuing batch.")
                        # Re-add batch to the front of the queue if breaker is open
                        async with self._lock:
                            for item in reversed(batch_to_process): # Add back in original order
                                self._queues[queue_type].appendleft(item)
                        continue # Skip processing this batch

                    # Process the batch
                    success = await self._process_single_batch(queue_type, batch_to_process)

                    if success:
                        await circuit_breaker.record_success()
                        self._stats['processed_items'] += len(batch_to_process)
                        self._stats['batches_processed'] += 1
                        processed_batch = True
                    else:
                        # Failure handled within _process_single_batch (moves to failed_items)
                        await circuit_breaker.record_failure()
                        # No need to break loop, try processing other queue if applicable

                    # Small delay between processing different queue types if needed
                    await asyncio.sleep(0.1)

            if processed_batch:
                 self._last_batch_process_time = time.monotonic()
            logger.debug("Finished queue processing cycle.")


    async def _process_single_batch(self, queue_type: str, batch: List[Dict[str, Any]]) -> bool:
        """
        Processes a single batch, attempting to save it via SheetManager.
        Handles moving items to failed queue on failure.
        """
        # Note: Circuit breaker check is done *before* calling this method in process_queues
        try:
            # SheetManager's save_batch_to_sheet already includes retries and API error handling
            success = await self.sheet_manager.save_batch_to_sheet(queue_type, batch)

            if success:
                logger.debug(f"Successfully processed batch for {queue_type}.")
                return True
            else:
                logger.error(f"Failed to save batch for {queue_type} after retries by SheetManager.")
                # Move failed batch items to the persistent failed queue
                async with self._lock:
                    self._failed_items[queue_type].extend(batch)
                    self._stats['failed_items_persistent'] += len(batch)
                logger.warning(f"Moved {len(batch)} failed items to persistent failure queue for {queue_type}.")
                return False

        except Exception as e:
            # This catch block is for unexpected errors *within* this method,
            # not errors from sheet_manager.save_batch_to_sheet (which handles its own).
            logger.error(f"Unexpected error in _process_single_batch for {queue_type}: {e}", exc_info=True)
            # Assume failure and move to failed queue
            async with self._lock:
                 self._failed_items[queue_type].extend(batch)
                 self._stats['failed_items_persistent'] += len(batch)
            logger.warning(f"Moved {len(batch)} items to persistent failure queue due to unexpected error for {queue_type}.")
            return False


    async def run_periodic_processing(self):
        """Runs process_queues periodically."""
        logger.info("Starting periodic queue processing task...")
        while not self._shutdown_signal.is_set():
            try:
                await self.process_queues()
                # Wait for the processing interval before the next check
                await asyncio.sleep(self.config.queue_processing_interval)
            except asyncio.CancelledError:
                 logger.info("Periodic processing task cancelled.")
                 break
            except Exception as e:
                 logger.error(f"Error in periodic processing loop: {e}", exc_info=True)
                 # Avoid busy-looping on errors, wait before retrying
                 await asyncio.sleep(self.config.queue_processing_interval)

    async def shutdown(self):
        """Initiates shutdown, processes remaining items, and logs stats."""
        logger.info("QueueManager shutdown initiated.")
        self._shutdown_signal.set() # Signal periodic task to stop

        # Wait briefly for the periodic task to potentially finish its current cycle
        await asyncio.sleep(0.1)

        # Process any remaining items in the queues forcefully
        logger.info("Processing remaining items before shutdown...")
        # Ensure processing lock is released before final processing, in case it was held
        if self._processing_lock.locked():
             try:
                 self._processing_lock.release()
                 logger.warning("Released processing lock during shutdown.")
             except RuntimeError: # If lock not owned by current task
                 pass
        await self.process_queues(force=True) # Force one last processing run

        # Log final stats
        async with self._lock: # Ensure consistent access to stats and queues
            logger.info("--- QueueManager Final Stats ---")
            logger.info(f"  Items remaining in high_salary queue: {len(self._queues['high_salary'])}")
            logger.info(f"  Items remaining in low_salary queue: {len(self._queues['low_salary'])}")
            logger.info(f"  Items in persistent high_salary failure queue: {len(self._failed_items['high_salary'])}")
            logger.info(f"  Items in persistent low_salary failure queue: {len(self._failed_items['low_salary'])}")
            logger.info(f"  Total batches processed: {self._stats['batches_processed']}")
            logger.info(f"  Total items processed successfully: {self._stats['processed_items']}")
            logger.info(f"  Total items moved to persistent failure queue: {self._stats['failed_items_persistent']}")
            if PSUTIL_AVAILABLE and self._stats['memory_usage_mb'] is not None:
                 logger.info(f"  Last recorded memory usage: {self._stats['memory_usage_mb']:.2f} MB")
            logger.info("-------------------------------")

            # Save failed items to a file for later inspection/retry
            self._save_failed_items() # Now implemented

        logger.info("QueueManager shutdown complete.")

    def _save_failed_items(self):
        """Saves items from the persistent failure queues to JSON files."""
        # This runs synchronously during shutdown (called from async shutdown)
        for queue_type, items in self._failed_items.items():
            if not items:
                continue

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = Path(self.config.base_path) / f"failed_{queue_type}_items_{timestamp}.json"
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(items, f, indent=4, ensure_ascii=False)
                logger.info(f"Saved {len(items)} failed items for {queue_type} to {filename}")
            except Exception as e:
                logger.error(f"Error saving failed {queue_type} items to {filename}: {e}")

    async def __aenter__(self):
        # Start periodic processing in the background
        self._periodic_task = asyncio.create_task(self.run_periodic_processing())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cancel the background task and perform shutdown
        if hasattr(self, '_periodic_task') and self._periodic_task:
            self._periodic_task.cancel()
            try:
                await self._periodic_task
            except asyncio.CancelledError:
                logger.debug("Periodic processing task successfully cancelled.")
        await self.shutdown()

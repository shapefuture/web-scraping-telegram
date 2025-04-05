#!/usr/bin/env python3
import sys
import logging

# Note: For this script to run correctly, either:
# 1. Run it from the project's root directory.
# 2. Ensure the project root directory is included in the PYTHONPATH environment variable.
# 3. Install the project as a package (e.g., using setup.py or pyproject.toml).
# The commented-out sys.path modification below is generally discouraged.
# import os
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the main function from the new main module
try:
    from main import main as run_main_app
except ImportError as e:
     logging.basicConfig(level=logging.CRITICAL)
     logging.critical(f"Failed to import application components. Ensure all modules (main.py, config_loader.py, etc.) are present and correct: {e}")
     sys.exit(1)
except Exception as e:
     logging.basicConfig(level=logging.CRITICAL)
     logging.critical(f"An unexpected error occurred during initial import: {e}")
     sys.exit(1)


if __name__ == "__main__":
    # Execute the main application logic from main.py
    run_main_app()

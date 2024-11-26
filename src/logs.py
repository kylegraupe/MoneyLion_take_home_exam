"""
Task 4b: Logging
- Key events are logged using the logging library.
- Errors are logged in try-except wrappings.

New log files are created for each run describing the application status for each run.
"""

import logging
import os
from datetime import datetime

# Generate a unique log file name based on the current timestamp
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = f"app_{current_time}.log"
os.makedirs("logs", exist_ok=True)  # Ensure the logs directory exists

logging.basicConfig(
    filename=os.path.join("logs", LOG_FILE),
    level=logging.INFO,  # Log INFO level and above
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Create a logger instance
logger = logging.getLogger(__name__)

def log_event(event_message):
    """Log an informational event."""
    logger.info(event_message)

def log_error(error_message):
    """Log an error event."""
    logger.error(error_message)

def log_critical(critical_message):
    """Log a critical event."""
    logger.critical(critical_message)

def log_warning(warning_message):
    """Log a warning."""
    logger.warning(warning_message)
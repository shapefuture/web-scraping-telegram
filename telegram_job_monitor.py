import asyncio
import logging
from datetime import datetime, timedelta
import pandas as pd
from telethon import TelegramClient, events
from telethon.tl.types import Channel, User
import gspread
from google.oauth2.service_account import Credentials
import schedule
import time
import os
import re
import json
from config import (
    WORKSHEET_NAME,
    CHANNELS,
    CHECK_INTERVAL_HOURS
)
import telethon.errors
import pickle
import gc
import shutil

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_monitor.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Load credentials from environment variables with defaults and validation
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
RENDER = os.getenv('RENDER', '').lower() == 'true'
TELEGRAM_CODE = os.getenv('TELEGRAM_CODE')
SESSION_FILE = os.getenv('SESSION_FILE', 'session')  # Allow custom session file name
CHECK_INTERVAL_HOURS = int(os.getenv('CHECK_INTERVAL_HOURS', '1'))  # Default to checking every hour
MAX_MESSAGES_PER_CHANNEL = int(os.getenv('MAX_MESSAGES_PER_CHANNEL', '30'))  # Default to 30 messages per channel
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))  # Default batch size for Google Sheets operations
DEBUG_MODE = os.getenv('DEBUG_MODE', '').lower() == 'true'  # Enable detailed debugging

# Set custom backoff parameters
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
INITIAL_RETRY_DELAY = int(os.getenv('INITIAL_RETRY_DELAY', '2'))

# File paths for data persistence
PROCESSED_MESSAGES_FILE = os.getenv('PROCESSED_MESSAGES_FILE', 'processed_messages.pkl')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'backups')

# Create backup directory if it doesn't exist
os.makedirs(BACKUP_DIR, exist_ok=True)

# Enhanced logging based on debug mode
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled - showing detailed logs")

def load_processed_messages():
    """Load the set of processed message IDs from file with backup handling."""
    try:
        if os.path.exists(PROCESSED_MESSAGES_FILE):
            with open(PROCESSED_MESSAGES_FILE, 'rb') as f:
                messages = pickle.load(f)
                logger.info(f"Loaded {len(messages)} processed messages from {PROCESSED_MESSAGES_FILE}")
                
                # Create a backup of the loaded file
                backup_file = os.path.join(BACKUP_DIR, f"processed_messages_{int(time.time())}.pkl")
                try:
                    shutil.copy2(PROCESSED_MESSAGES_FILE, backup_file)
                    logger.debug(f"Created backup of processed messages at {backup_file}")
                    
                    # Clean up old backups (keep last 5)
                    backup_files = sorted([f for f in os.listdir(BACKUP_DIR) 
                                         if f.startswith('processed_messages_') and f.endswith('.pkl')])
                    if len(backup_files) > 5:
                        for old_file in backup_files[:-5]:
                            os.remove(os.path.join(BACKUP_DIR, old_file))
                            logger.debug(f"Removed old backup: {old_file}")
                except Exception as backup_err:
                    logger.warning(f"Failed to create backup: {backup_err}")
                
                return messages
        else:
            logger.info(f"No processed messages file found at {PROCESSED_MESSAGES_FILE}, starting with empty set")
    except Exception as e:
        logger.error(f"Error loading processed messages: {e}")
        logger.exception("Full traceback:")
        
        # Try to load from the most recent backup
        try:
            backup_files = sorted([f for f in os.listdir(BACKUP_DIR) 
                                 if f.startswith('processed_messages_') and f.endswith('.pkl')])
            if backup_files:
                latest_backup = os.path.join(BACKUP_DIR, backup_files[-1])
                logger.info(f"Attempting to load from backup: {latest_backup}")
                with open(latest_backup, 'rb') as f:
                    messages = pickle.load(f)
                    logger.info(f"Loaded {len(messages)} processed messages from backup")
                    return messages
        except Exception as backup_err:
            logger.error(f"Failed to load from backup: {backup_err}")
    
    return set()

def save_processed_messages(processed_messages):
    """Save the set of processed message IDs to file with error handling."""
    if not processed_messages:
        logger.warning("Attempted to save empty processed messages set, ignoring")
        return
        
    try:
        # First save to a temporary file
        temp_file = f"{PROCESSED_MESSAGES_FILE}.tmp"
        with open(temp_file, 'wb') as f:
            pickle.dump(processed_messages, f)
            
        # Then rename to the actual file (safer atomic operation)
        if os.path.exists(PROCESSED_MESSAGES_FILE):
            os.replace(temp_file, PROCESSED_MESSAGES_FILE)
        else:
            os.rename(temp_file, PROCESSED_MESSAGES_FILE)
            
        logger.debug(f"Saved {len(processed_messages)} processed messages to {PROCESSED_MESSAGES_FILE}")
    except Exception as e:
        logger.error(f"Error saving processed messages: {e}")
        logger.exception("Full traceback:")

# Validate required environment variables with detailed feedback
def validate_environment():
    """Validate all required environment variables and return result."""
    required_vars = {
        'API_ID': API_ID,
        'API_HASH': API_HASH,
        'PHONE': PHONE,
        'GOOGLE_SHEET_ID': GOOGLE_SHEET_ID
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your environment or .env file")
        return False
        
    # Validate phone number format
    if PHONE and not PHONE.startswith('+') and not PHONE.isdigit():
        logger.error(f"Invalid phone number format: {PHONE}. Should be all digits or start with '+'")
        return False
        
    # Log configuration
    logger.info("=== Configuration ===")
    logger.info(f"API_ID is set: {bool(API_ID)}")
    logger.info(f"API_HASH is set: {bool(API_HASH)}")
    logger.info(f"PHONE is set: {bool(PHONE)}")
    logger.info(f"GOOGLE_SHEET_ID is set: {bool(GOOGLE_SHEET_ID)}")
    logger.info(f"Running on Render: {RENDER}")
    logger.info(f"Monitoring channels: {CHANNELS}")
    logger.info(f"Check interval: {CHECK_INTERVAL_HOURS} hours")
    logger.info(f"Max messages per channel: {MAX_MESSAGES_PER_CHANNEL}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Debug mode: {DEBUG_MODE}")
    logger.info("==================")
    
    return True

# Validate required environment variables
required_vars = {
    'API_ID': API_ID,
    'API_HASH': API_HASH,
import asyncio
import logging
from datetime import datetime, timedelta
import pandas as pd
from telethon import TelegramClient, events
from telethon.tl.types import Channel, User
import gspread
from google.oauth2.service_account import Credentials
import schedule
import time
import os
import re
import json
from config import (
    WORKSHEET_NAME,
    CHANNELS,
    CHECK_INTERVAL_HOURS
)
import telethon.errors
import pickle
import gc
import shutil

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_monitor.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Load credentials from environment variables with defaults and validation
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
RENDER = os.getenv('RENDER', '').lower() == 'true'
TELEGRAM_CODE = os.getenv('TELEGRAM_CODE')
SESSION_FILE = os.getenv('SESSION_FILE', 'session')  # Allow custom session file name
CHECK_INTERVAL_HOURS = int(os.getenv('CHECK_INTERVAL_HOURS', '1'))  # Default to checking every hour
MAX_MESSAGES_PER_CHANNEL = int(os.getenv('MAX_MESSAGES_PER_CHANNEL', '30'))  # Default to 30 messages per channel
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))  # Default batch size for Google Sheets operations
DEBUG_MODE = os.getenv('DEBUG_MODE', '').lower() == 'true'  # Enable detailed debugging

# Set custom backoff parameters
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
INITIAL_RETRY_DELAY = int(os.getenv('INITIAL_RETRY_DELAY', '2'))

# File paths for data persistence
PROCESSED_MESSAGES_FILE = os.getenv('PROCESSED_MESSAGES_FILE', 'processed_messages.pkl')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'backups')

# Create backup directory if it doesn't exist
os.makedirs(BACKUP_DIR, exist_ok=True)

# Enhanced logging based on debug mode
if DEBUG_MODE:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled - showing detailed logs")

def load_processed_messages():
    """Load the set of processed message IDs from file with backup handling."""
    try:
        if os.path.exists(PROCESSED_MESSAGES_FILE):
            with open(PROCESSED_MESSAGES_FILE, 'rb') as f:
                messages = pickle.load(f)
                logger.info(f"Loaded {len(messages)} processed messages from {PROCESSED_MESSAGES_FILE}")
                
                # Create a backup of the loaded file
                backup_file = os.path.join(BACKUP_DIR, f"processed_messages_{int(time.time())}.pkl")
                try:
                    shutil.copy2(PROCESSED_MESSAGES_FILE, backup_file)
                    logger.debug(f"Created backup of processed messages at {backup_file}")
                    
                    # Clean up old backups (keep last 5)
                    backup_files = sorted([f for f in os.listdir(BACKUP_DIR) 
                                         if f.startswith('processed_messages_') and f.endswith('.pkl')])
                    if len(backup_files) > 5:
                        for old_file in backup_files[:-5]:
                            os.remove(os.path.join(BACKUP_DIR, old_file))
                            logger.debug(f"Removed old backup: {old_file}")
                except Exception as backup_err:
                    logger.warning(f"Failed to create backup: {backup_err}")
                
                return messages
        else:
            logger.info(f"No processed messages file found at {PROCESSED_MESSAGES_FILE}, starting with empty set")
    except Exception as e:
        logger.error(f"Error loading processed messages: {e}")
        logger.exception("Full traceback:")
        
        # Try to load from the most recent backup
        try:
            backup_files = sorted([f for f in os.listdir(BACKUP_DIR) 
                                 if f.startswith('processed_messages_') and f.endswith('.pkl')])
            if backup_files:
                latest_backup = os.path.join(BACKUP_DIR, backup_files[-1])
                logger.info(f"Attempting to load from backup: {latest_backup}")
                with open(latest_backup, 'rb') as f:
                    messages = pickle.load(f)
                    logger.info(f"Loaded {len(messages)} processed messages from backup")
                    return messages
        except Exception as backup_err:
            logger.error(f"Failed to load from backup: {backup_err}")
    
    return set()

def save_processed_messages(processed_messages):
    """Save the set of processed message IDs to file."""
    try:
        with open(PROCESSED_MESSAGES_FILE, 'wb') as f:
            pickle.dump(processed_messages, f)
    except Exception as e:
        logger.error(f"Error saving processed messages: {e}")

# Validate required environment variables
required_vars = {
    'API_ID': API_ID,
    'API_HASH': API_HASH,
    'PHONE': PHONE,
    'GOOGLE_CREDENTIALS_JSON': GOOGLE_CREDENTIALS_JSON,
    'GOOGLE_SHEET_ID': GOOGLE_SHEET_ID
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.error("Please ensure all required environment variables are set in your .env file")
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# If running on Render, validate TELEGRAM_CODE
if RENDER and not TELEGRAM_CODE:
    logger.error("TELEGRAM_CODE environment variable is required when running on Render")
    raise ValueError("TELEGRAM_CODE environment variable is required when running on Render")

# Print configuration (without sensitive data)
logger.info("=== Configuration ===")
logger.info(f"API_ID is set: {bool(API_ID)}")
logger.info(f"API_HASH is set: {bool(API_HASH)}")
logger.info(f"PHONE is set: {bool(PHONE)}")
logger.info(f"GOOGLE_SHEET_ID is set: {bool(GOOGLE_SHEET_ID)}")
logger.info(f"Running on Render: {RENDER}")
logger.info(f"Monitoring channels: {CHANNELS}")
logger.info(f"Check interval: {CHECK_INTERVAL_HOURS} hours")
logger.info("==================")

# Keywords for parsing
JOB_KEYWORDS = {
    'position': [
        r'ваканси[яи]', r'ищем', r'требуется', r'нужен', r'нужна', r'нужны',
        r'position:', r'вакансия:', r'вакансия', r'job:', r'job', r'role:',
        r'role', r'position', r'vacancy:', r'vacancy', r'ищем', r'требуется',
        r'нужен', r'нужна', r'нужны', r'ищем', r'требуется', r'нужен',
        r'нужна', r'нужны', r'ищем', r'требуется', r'нужен', r'нужна',
        r'нужны', r'ищем', r'требуется', r'нужен', r'нужна', r'нужны'
    ],
    'requirements': [
        r'требования:', r'требования', r'requirements:', r'requirements',
        r'квалификация:', r'квалификация', r'qualification:', r'qualification',
        r'обязанности:', r'обязанности', r'duties:', r'duties', r'от вас:',
        r'от вас', r'от кандидата:', r'от кандидата', r'что нужно:',
        r'что нужно', r'что требуется:', r'что требуется', r'необходимые навыки:',
        r'необходимые навыки', r'необходимые знания:', r'необходимые знания',
        r'необходимый опыт:', r'необходимый опыт', r'необходимые требования:',
        r'необходимые требования', r'необходимые требования:', r'необходимые требования'
    ],
    'what_they_offer': [
        r'предлагаем:', r'предлагаем', r'предлагается:', r'предлагается',
        r'we offer:', r'we offer', r'offering:', r'offering', r'условия:',
        r'условия', r'conditions:', r'conditions', r'benefits:', r'benefits',
        r'что мы предлагаем:', r'что мы предлагаем', r'что предлагаем:',
        r'что предлагаем', r'что вы получите:', r'что вы получите',
        r'что мы даем:', r'что мы даем', r'что мы даём:', r'что мы даём',
        r'что мы даем:', r'что мы даем', r'что мы даём:', r'что мы даём',
        r'что мы даем:', r'что мы даем', r'что мы даём:', r'что мы даём'
    ]
}

# Keywords for fit percentage calculation
FIT_KEYWORDS = {
    'psychology': [
        'психолог', 'психология', 'психотерапевт', 'психотерапия',
        'психолог-консультант', 'психологическое консультирование',
        'психолог-коуч', 'психологический коуч', 'психолог-тренер',
        'психолог-преподаватель', 'психолог-исследователь',
        'psychologist', 'psychology', 'psychotherapist', 'psychotherapy',
        'psychological counseling', 'psychological coach', 'psychological trainer',
        'psychological researcher'
    ],
    'coaching': [
        'коуч', 'коучинг', 'коуч-консультант', 'коуч-тренер',
        'коуч-преподаватель', 'коуч-ментор', 'коуч-наставник',
        'коуч-психолог', 'коуч-терапевт', 'коуч-консультант',
        'coach', 'coaching', 'coach-consultant', 'coach-trainer',
        'coach-mentor', 'coach-psychologist', 'coach-therapist'
    ],
    'crypto': [
        'крипто', 'криптовалюта', 'блокчейн', 'web3', 'defi',
        'crypto', 'cryptocurrency', 'blockchain', 'web3', 'defi',
        'nft', 'nfts', 'smart contract', 'смарт-контракт',
        'crypto trading', 'криптотрейдинг', 'crypto exchange',
        'криптобиржа', 'crypto wallet', 'криптокошелек'
    ],
    'premium': [
        'premium', 'премиум', 'luxury', 'люкс', 'high-end'
    ]
}

# Keywords for fit percentage calculation
FIT_KEYWORDS = {
    'psychology': [
        'психолог', 'психология', 'психотерапевт', 'психотерапия',
        'психолог-консультант', 'психологическое консультирование',
        'психолог-коуч', 'психологический коуч', 'психолог-тренер',
        'психолог-преподаватель', 'психолог-исследователь',
        'psychologist', 'psychology', 'psychotherapist', 'psychotherapy',
        'psychological counseling', 'psychological coach', 'psychological trainer',
        'psychological researcher'
    ],
    'coaching': [
        'коуч', 'коучинг', 'коуч-консультант', 'коуч-тренер',
        'коуч-преподаватель', 'коуч-ментор', 'коуч-наставник',
        'коуч-психолог', 'коуч-терапевт', 'коуч-консультант',
        'coach', 'coaching', 'coach-consultant', 'coach-trainer',
        'coach-mentor', 'coach-psychologist', 'coach-therapist'
    ],
    'crypto': [
        'крипто', 'криптовалюта', 'блокчейн', 'web3', 'defi',
        'crypto', 'cryptocurrency', 'blockchain', 'web3', 'defi',
        'nft', 'nfts', 'smart contract', 'смарт-контракт',
        'crypto trading', 'криптотрейдинг', 'crypto exchange',
        'криптобиржа', 'crypto wallet', 'криптокошелек'
    ],
    'premium': [
        'premium', 'премиум', 'luxury', 'люкс', 'high-end'
    ],
    'flexible_hours': [
        'гибкий график', 'гибкое время', 'гибкий режим',
        'flexible hours', 'flexible schedule', 'flexible working hours',
        'flexible time', 'flexible regime', 'flexible work schedule',
        'flexible working time', 'flexible work regime'
    ],
    'short_work_time': [
        'неполный день', 'частичная занятость', 'part-time',
        'part time', 'parttime', 'неполная занятость',
        'short hours', 'short working hours', 'short work day',
        'short work time', 'short working time'
    ]
}

# Add remote work keywords to the top of the file with other keywords
REMOTE_KEYWORDS = [
    'удаленно', 'удаленная', 'удаленный', 'удаленка', 'remote',
    'дистанционно', 'дистанционная', 'дистанционный', 'дистанционка',
    'из дома', 'from home', 'work from home', 'wfh', 'work remotely',
    'remote work', 'remote position', 'remote job', 'удаленная работа',
    'удаленная вакансия', 'удаленная должность', 'удаленный сотрудник',
    'удаленный специалист', 'удаленный работник', 'удаленный персонал',
    'удаленный штат', 'удаленный коллектив', 'удаленная команда'
]

def calculate_fit_percentage(text):
    """Calculate fit percentage based on Anna's preferences."""
    text_lower = text.lower()
    total_score = 0
    
    # Main categories (25% each)
    main_categories = ['psychology', 'coaching', 'crypto', 'premium']
    for category in main_categories:
        if any(keyword in text_lower for keyword in FIT_KEYWORDS[category]):
            total_score += 25
    
    # Bonus categories (20% each)
    bonus_categories = ['flexible_hours', 'short_work_time']
    for category in bonus_categories:
        if any(keyword in text_lower for keyword in FIT_KEYWORDS[category]):
            total_score += 20
    
    # Cap the total score at 100%
    return min(round(total_score, 2), 100)

def determine_priority(fit_percentage):
    """Determine priority based on fit percentage."""
    if fit_percentage >= 80:
        return 'High'
    elif fit_percentage >= 50:
        return 'Medium'
    else:
        return 'Low'

# Additional patterns for location and salary
LOCATION_PATTERNS = [
    r'📍\s*([^\n]+)',
    r'🏢\s*([^\n]+)',
    r'город:\s*([^\n]+)',
    r'город\s*([^\n]+)',
    r'location:\s*([^\n]+)',
    r'location\s*([^\n]+)'
]

SALARY_PATTERNS = [
    r'💰\s*([^\n]+)',
    r'💵\s*([^\n]+)',
    r'зарплата:\s*([^\n]+)',
    r'зарплата\s*([^\n]+)',
    r'salary:\s*([^\n]+)',
    r'salary\s*([^\n]+)'
]

def test_salary_parsing():
    """Test the salary parsing function with various formats."""
    test_cases = [
        "300-2000$",
        "1.2-2.5к$",
        "75 000 - 150 000 рублей",
        "2000-3000$",
        "от 80 000 до 120 000 ₽",
        "100k-200k$",
        "1.5k-2.5k USD",
        "150 000 - 250 000 руб",
        "€2000-3000",
        "2000€-3000€"
    ]
    
    print("\nTesting salary parsing:")
    for test in test_cases:
        is_high, salary = parse_salary(test)
        print(f"\nInput: {test}")
        print(f"Is high salary: {is_high}")
        print(f"Parsed salary: {salary}")

def parse_salary(salary_text):
    """Parse salary text to determine if it's a high salary position and extract the salary value."""
    if not salary_text:
        return False, None
        
    salary_text = salary_text.lower()
    
    # Debug logging to diagnose parsing issues
    logger.debug(f"Parsing salary text: {salary_text}")
    
    # Handle various number formats:
    # 1. Replace common separators and remove symbols
    salary_text = salary_text.replace(',', '.').replace('+', '').replace('±', '').replace('+/-', '').replace('≈', '')
    
    # Handle dual currency formats like "до 150 000 рублей / 1500 USDT"
    dual_currency_match = re.search(r'(\d+(?:\s\d+)*)\s*(?:руб|рублей|₽|р\.)?\s*(?:/|и)\s*(\d+(?:\s\d+)*)\s*(usdt|\$|usd)', salary_text, re.IGNORECASE)
    if dual_currency_match:
        rub_salary = int(dual_currency_match.group(1).replace(' ', ''))
        usd_salary = int(dual_currency_match.group(2).replace(' ', ''))
        
        # Check either currency against thresholds
        if rub_salary >= 100000 or usd_salary >= 1000:
            logger.debug(f"Detected high dual currency salary: {rub_salary} RUB / {usd_salary} USD")
            return True, f"{rub_salary:,} RUB / {usd_salary:,} USD".replace(',', ' ')
    
    # Handle Russian salary with "тыс" (thousands) or "тыс." abbreviation
    # Pattern: "X тыс" or "X тыс." or "X тыс руб" (e.g., "100 тыс. руб" or "80–100 тыс")
    match = re.search(r'(\d+(?:[.,]\d+)?)\s*[–-]?\s*(\d+(?:[.,]\d+)?)?\s*тыс(?:\.)?', salary_text)
    if match:
        # If it's a range (e.g., "80-100 тыс.")
        if match.group(2):
            min_salary = float(match.group(1).replace(' ', '')) * 1000
            max_salary = float(match.group(2).replace(' ', '')) * 1000
            if max_salary >= 100000 or min_salary >= 100000:
                logger.debug(f"Detected high Russian salary range (тыс format): {min_salary}-{max_salary}")
                return True, f"{min_salary:,.0f}-{max_salary:,.0f} RUB".replace(',', ' ')
        # If it's a single value (e.g., "100 тыс.")
        else:
            salary = float(match.group(1).replace(' ', '')) * 1000
            if salary >= 100000:
                logger.debug(f"Detected high Russian salary (тыс format): {salary}")
                return True, f"{salary:,.0f} RUB".replace(',', ' ')
    
    # Handle "до X" (up to X) format for Russian salaries - improved to handle ₽ symbol
    match = re.search(r'до\s*(\d+(?:\s\d+)*)\s*(?:руб|рублей|₽|р\.)?', salary_text)
    if match:
        max_salary = int(match.group(1).replace(' ', ''))
        if max_salary >= 100000:
            logger.debug(f"Detected high Russian salary (до format): {max_salary}")
            return True, f"up to {max_salary:,} RUB".replace(',', ' ')
    
    # Handle "до X" (up to X) format for USD salaries
    match = re.search(r'до\s*(\d+(?:\s\d+)*)\s*\$', salary_text)
    if match:
        max_salary = int(match.group(1).replace(' ', ''))
        if max_salary >= 1000:
            logger.debug(f"Detected high USD salary (до format): {max_salary}")
            return True, f"up to {max_salary:,} USD".replace(',', ' ')
    
    # Handle "от X" (from X) format for USD salaries
    match = re.search(r'от\s*(\d+(?:\s\d+)*)\s*\$', salary_text)
    if match:
        min_salary = int(match.group(1).replace(' ', ''))
        if min_salary >= 1000:
            logger.debug(f"Detected high USD salary (от format): {min_salary}")
            return True, f"from {min_salary:,} USD".replace(',', ' ')
    
    # Handle "от X" (from X) format for Russian salaries - improved to handle ₽ symbol
    match = re.search(r'от\s*(\d+(?:\s\d+)*)\s*(?:руб|рублей|₽|р\.)?', salary_text)
    if match:
        min_salary = int(match.group(1).replace(' ', ''))
        if min_salary >= 100000:
            logger.debug(f"Detected high Russian salary (от format): {min_salary}")
            return True, f"from {min_salary:,} RUB".replace(',', ' ')
    
    # Handle "гросс" (gross) format for Russian salaries
    match = re.search(r'(\d+(?:\s\d+)*)\s*гросс', salary_text)
    if match:
        salary = int(match.group(1).replace(' ', ''))
        if salary >= 100000:
            logger.debug(f"Detected high Russian salary (gross format): {salary}")
            return True, f"{salary:,} RUB (gross)".replace(',', ' ')
    
    # Handle Russian number format with spaces and "от" (e.g., "от 80 000 до 120 000 ₽")
    if 'руб' in salary_text or '₽' in salary_text or 'р.' in salary_text or 'рублей' in salary_text:
        # First try to find a range with "от" and "до"
        range_match = re.search(r'от\s*(\d+(?:\s\d+)*)\s*до\s*(\d+(?:\s\d+)*)', salary_text)
        if range_match:
            # Remove spaces and convert to numbers
            min_salary = int(range_match.group(1).replace(' ', ''))
            max_salary = int(range_match.group(2).replace(' ', ''))
            # Consider high salary if max salary is above threshold
            if max_salary >= 100000:
                logger.debug(f"Detected high Russian salary range (от-до format): {min_salary}-{max_salary}")
                return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
        
        # Then try to find a range with spaces and dash
        range_match = re.search(r'(\d+(?:\s\d+)*)\s*[–-]\s*(\d+(?:\s\d+)*)', salary_text)
        if range_match:
            # Remove spaces and convert to numbers
            min_salary = int(range_match.group(1).replace(' ', ''))
            max_salary = int(range_match.group(2).replace(' ', ''))
            # Consider high salary if max salary is above threshold
            if max_salary >= 100000:
                logger.debug(f"Detected high Russian salary range (dash format): {min_salary}-{max_salary}")
                return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
        
        # If no range found, try to find a single number with spaces
        single_match = re.search(r'(\d+(?:\s\d+)*)', salary_text)
        if single_match:
            # Remove spaces and convert to number
            salary = int(single_match.group(1).replace(' ', ''))
            if salary >= 100000:
                logger.debug(f"Detected high Russian salary (single number): {salary}")
                return True, f"{salary:,} RUB".replace(',', ' ')
    
    # Handle euro in Russian text: "X евро" or "X EUR"
    match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:евро|eur)', salary_text)
    if match:
        salary = float(match.group(1).replace(' ', ''))
        if salary >= 1000:
            logger.debug(f"Detected high EUR salary: {salary}")
            return True, f"{salary:,.0f} EUR".replace(',', ' ')
    
    # Improved handling for salary with dot as thousand separator 
    # Examples: "100.000 - 150.000", "1.000-1.500$", "1.2-2.5к$"
    dot_sep_match = re.search(r'(\d+)\.(\d{3})(?:\s*[–-]\s*(\d+)\.(\d{3}))?', salary_text)
    if dot_sep_match:
        # If it's a range (e.g., "100.000 - 150.000")
        if dot_sep_match.group(3) and dot_sep_match.group(4):
            min_salary = int(dot_sep_match.group(1) + dot_sep_match.group(2))
            max_salary = int(dot_sep_match.group(3) + dot_sep_match.group(4))
            
            # Check currency
            if '$' in salary_text or 'usd' in salary_text or 'usdt' in salary_text:
                if max_salary >= 1000:
                    logger.debug(f"Detected high USD salary range (dot sep): {min_salary}-{max_salary}")
                    return True, f"{min_salary:,}-{max_salary:,} USD".replace(',', ' ')
            else:  # Default to RUB if no currency
                if max_salary >= 100000:
                    logger.debug(f"Detected high RUB salary range (dot sep): {min_salary}-{max_salary}")
                    return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
        # If it's a single value (e.g., "100.000")
        else:
            salary = int(dot_sep_match.group(1) + dot_sep_match.group(2))
            
            # Check currency
            if '$' in salary_text or 'usd' in salary_text or 'usdt' in salary_text:
                if salary >= 1000:
                    logger.debug(f"Detected high USD salary (dot sep): {salary}")
                    return True, f"{salary:,} USD".replace(',', ' ')
            else:  # Default to RUB if no currency
                if salary >= 100000:
                    logger.debug(f"Detected high RUB salary (dot sep): {salary}")
                    return True, f"{salary:,} RUB".replace(',', ' ')
    
    # Handle partial dot-separated format (1.2-2.5k$ or 1.2k-2.5k$)
    partial_dot_sep_match = re.search(r'(\d+)\.(\d+)\s*[–-]\s*(\d+)\.(\d+)[kк]\$', salary_text)
    if partial_dot_sep_match:
        min_salary = float(partial_dot_sep_match.group(1) + '.' + partial_dot_sep_match.group(2)) * 1000
        max_salary = float(partial_dot_sep_match.group(3) + '.' + partial_dot_sep_match.group(4)) * 1000
        if max_salary >= 1000:
            logger.debug(f"Detected high USD salary range (partial dot sep): {min_salary}-{max_salary}")
            return True, f"{min_salary:,.0f}-{max_salary:,.0f} USD".replace(',', ' ')
    
    # General pattern to match salary ranges in Russian text without currency markers
    # This will catch phrases like "от 80 000 до 120 000 в зависимости от опыта"
    range_match = re.search(r'от\s*(\d+(?:\s\d+)*)\s*до\s*(\d+(?:\s\d+)*)', salary_text)
    if range_match:
        # Remove spaces and convert to numbers
        min_salary = int(range_match.group(1).replace(' ', ''))
        max_salary = int(range_match.group(2).replace(' ', ''))
        # Consider high salary if max salary is above threshold
        if max_salary >= 100000:
            logger.debug(f"Detected high Russian salary range (general от-до): {min_salary}-{max_salary}")
            return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
    
    # Handle USD salary ranges (e.g., "300-2000$")
    if '$' in salary_text or 'usd' in salary_text or 'usdt' in salary_text:
        # Find range with dash and $ symbol (e.g., "2000-3000$")
        range_match = re.search(r'(\d+(?:[.,]\d+)?)\s*[–-]\s*(\d+(?:[.,]\d+)?)\s*\$', salary_text)
        if range_match:
            min_salary = float(range_match.group(1).replace(' ', ''))
            max_salary = float(range_match.group(2).replace(' ', ''))
            if max_salary >= 1000:  # Check max salary
                logger.debug(f"Detected high USD salary range: {min_salary}-{max_salary}")
                return True, f"{min_salary:,.0f}-{max_salary:,.0f} USD".replace(',', ' ')
        
        # Try to find a single value with dollar sign
        single_match = re.search(r'(\d+(?:[.,]\d+)?)\s*\$', salary_text)
        if single_match:
            salary = float(single_match.group(1).replace(' ', ''))
            if salary >= 1000:
                logger.debug(f"Detected high USD salary: {salary}")
                return True, f"{salary:,.0f} USD".replace(',', ' ')
    
    # Handle k/K/к format with $ (e.g., 1.2-2.5к$, 1.2k-2.5k$, от 2000к $)
    if ('k' in salary_text or 'к' in salary_text) and '$' in salary_text:
        # Extract range with k/K/к
        range_match = re.search(r'(\d+(?:[.,]\d+)?)\s*[–-]\s*(\d+(?:[.,]\d+)?)[kк]\$', salary_text)
        if range_match:
            min_salary = float(range_match.group(1).replace(' ', '')) * 1000
            max_salary = float(range_match.group(2).replace(' ', '')) * 1000
            if max_salary >= 1000:
                logger.debug(f"Detected high USD salary k-format range: {min_salary}-{max_salary}")
                return True, f"{min_salary:,.0f}-{max_salary:,.0f} USD".replace(',', ' ')
    
        # Extract from X k/K/к format (e.g., "от 2000к $")
        from_k_match = re.search(r'от\s*(\d+(?:[.,]\d+)?)[kк]\s*\$', salary_text)
        if from_k_match:
            salary = float(from_k_match.group(1).replace(' ', '')) * 1000
            if salary >= 1000:
                logger.debug(f"Detected high USD salary from-k format: {salary}")
                return True, f"from {salary:,.0f} USD".replace(',', ' ')
        
        # Extract single value with k/K/к
        single_match = re.search(r'(\d+(?:[.,]\d+)?)[kк]\s*\$', salary_text)
        if single_match:
            salary = float(single_match.group(1).replace(' ', '')) * 1000
            if salary >= 1000:
                logger.debug(f"Detected high USD salary k-format: {salary}")
                return True, f"{salary:,.0f} USD".replace(',', ' ')
    
    # Handle k/K/к format for other currencies (e.g., $26k, 26K)
    if 'k' in salary_text or 'к' in salary_text:
        # Extract the number before k/K/к
        match = re.search(r'(\d+(?:[.,]\d+)?)[kк]', salary_text)
        if match:
            number = float(match.group(1).replace(' ', ''))
            # Convert k to actual number
            number = number * 1000
            if 'usd' in salary_text or '$' in salary_text or 'usdt' in salary_text:
                if number >= 1000:
                    logger.debug(f"Detected high USD salary k-format: {number}")
                    return True, f"{number:,.0f} USD".replace(',', ' ')
            elif 'eur' in salary_text or '€' in salary_text or 'евро' in salary_text:
                if number >= 1000:
                    logger.debug(f"Detected high EUR salary k-format: {number}")
                    return True, f"{number:,.0f} EUR".replace(',', ' ')
            elif 'руб' in salary_text or '₽' in salary_text or 'р.' in salary_text or 'рублей' in salary_text:
                if number >= 100000:
                    logger.debug(f"Detected high RUB salary k-format: {number}")
                    return True, f"{number:,.0f} RUB".replace(',', ' ')
    
    # Extract all numbers from the text
    numbers = re.findall(r'\d+(?:[.,]\d+)?', salary_text)
    if not numbers:
        return False, None
    
    # Convert numbers to float
    try:
        numbers = [float(num.replace(' ', '').replace(',', '.')) for num in numbers]
    except ValueError:
        return False, None
    
    # Check for USD
    if 'usd' in salary_text or '$' in salary_text or 'usdt' in salary_text:
        for num in numbers:
            if num >= 1000:
                logger.debug(f"Detected high USD salary from numbers: {num}")
                return True, f"{num:,.0f} USD".replace(',', ' ')
                
    # Check for RUB
    if 'руб' in salary_text or '₽' in salary_text or 'р.' in salary_text or 'рублей' in salary_text:
        for num in numbers:
            if num >= 100000:
                logger.debug(f"Detected high RUB salary from numbers: {num}")
                return True, f"{num:,.0f} RUB".replace(',', ' ')
                
    # Check for EUR
    if 'eur' in salary_text or '€' in salary_text or 'евро' in salary_text:
        for num in numbers:
            if num >= 1000:
                logger.debug(f"Detected high EUR salary from numbers: {num}")
                return True, f"{num:,.0f} EUR".replace(',', ' ')
    
    # For Russian text, look for numbers ≥ 100,000 even without currency markers
    # If we're parsing Russian text like "от 80 000 до 120 000"
    if any(word in salary_text for word in ['от', 'до', 'рублей', 'руб', 'тыс']):
        for num in numbers:
            if num >= 100000:
                logger.debug(f"Detected high RUB salary from context: {num}")
                return True, f"{num:,.0f} RUB".replace(',', ' ')
                
    # If no currency specified, assume RUB
    for num in numbers:
        if num >= 100000:
            logger.debug(f"Detected high RUB salary (default): {num}")
            return True, f"{num:,.0f} RUB".replace(',', ' ')
            
    logger.debug(f"No high salary detected in: {salary_text}")
    return False, None

def extract_section(text, patterns):
    """Extract text matching any of the given patterns."""
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

def test_parse_job_vacancy():
    """Test the job vacancy parser with a sample message."""
    sample_message = """Официант Лолита
📍Москва

Мы очень уютный ресторан расположенный в старинном особняке 18 века Демидовых, Рахмановых на Таганской, с открытой кухней и сногсшибательной атмосферой.

[Узнать подробности и оставить отклик](https://vitrina.jobs/card/?filters638355975=id__eq__1745&utm_source=tg&utm_medium=vacancy_17.02-23.02&utm_campaign=horeca_oficiant_lolita)

[Разместить свою вакансию](https://vitrina.jobs/main#rec637981865)

#вакансии_vitrinahoreca"""
    
    result = parse_job_vacancy(sample_message)
    print("\nTest Results:")
    print(f"Position: {result['position']}")
    print(f"Application Link: {result['application_link']}")
    print(f"What They Offer: {result['what_they_offer']}")
    print(f"Source: {result['source']}")
    print(f"Fit Percentage: {result['fit_percentage']}")
    print(f"Priority: {result['priority']}")
    print(f"High Salary: {result['high_salary']}")
    print(f"Salary: {result['salary']}")
    return result

def extract_job_link(text):
    """Extract job application link from the text."""
    # Look for common link patterns in job postings
    link_patterns = [
        r'\[(?:Узнать подробности|Подробнее|Откликнуться|Отклик|Apply|Apply now|Learn more|More details).*?\]\((.*?)\)',
        r'https?://(?:vitrina\.jobs|hh\.ru|career\.habr\.com|linkedin\.com).*?(?:\s|$)',
        r'https?://(?:jobs\.|careers\.).*?(?:\s|$)'
    ]
    
    for pattern in link_patterns:
        matches = re.findall(pattern, text)
        if matches:
            return matches[0]
    return None

def extract_telegram_link(text):
    """Extract Telegram link from the text and format it to point to specific posts."""
    # Look for Telegram-specific link patterns
    telegram_patterns = [
        r'https?://t\.me/[^\s]+',
        r'https?://telegram\.me/[^\s]+',
        r'https?://telegram\.dog/[^\s]+'
    ]
    
    for pattern in telegram_patterns:
        match = re.search(pattern, text)
        if match:
            # Clean up the link by removing any trailing characters and formatting
            link = match.group(0).strip('.,()[]{}')
            
            # If it's a bot link (contains /bot), return None
            if '/bot' in link:
                return None
            
            # If it's a channel/group link without a post ID, try to find the message ID
            if not re.search(r'/\d+$', link):
                # Look for message ID in the text
                message_id_match = re.search(r'/(\d+)(?:\s|$)', text)
                if message_id_match:
                    message_id = message_id_match.group(1)
                    # Add the message ID to the link
                    link = f"{link}/{message_id}"
            
            # Clean up any remaining formatting
            link = re.sub(r'[^\w\s\-:/.]', '', link)
            return link
    return None

def clean_position_text(text):
    """Clean position text by removing unnecessary symbols and formatting."""
    if not text:
        return ''
        
    # Remove markdown formatting
    text = re.sub(r'\*\*|\*|__|\[|\]|\(|\)', '', text)
    
    # Remove emojis and special characters
    text = re.sub(r'[^\w\s\-:.,]', '', text)
    
    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Remove common prefixes
    prefixes = [
        'требуется', 'требуются', 'нужен', 'нужна', 'нужны',
        'ищем', 'ищем:', 'вакансия:', 'вакансия', 'position:',
        'position', 'job:', 'job', 'role:', 'role'
    ]
    
    for prefix in prefixes:
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix):].strip(':').strip()
    
    return text.strip()

def parse_job_vacancy(text, channel_name=None):
    """Parse job vacancy text to extract specific fields."""
    # Initialize result dictionary
    result = {
        'position': '',
        'application_link': '',
        'telegram_link': '',
        'what_they_offer': '',
        'source': channel_name if channel_name else '',
        'fit_percentage': 'TBD',
        'priority': 'TBD',
        'high_salary': False,
        'salary': None
    }
    
    # Convert text to lowercase for case-insensitive matching
    text_lower = text.lower()
    
    # Split text into lines
    lines = text.split('\n')
    
    # First, try to find the position in the first few lines
    for line in lines[:3]:  # Check first 3 lines
        line = line.strip()
        if line and not any(keyword in line.lower() for keyword in ['📍', '💰', '💵', 'город:', 'зарплата:', 'http', 'https', '[']):
            result['position'] = clean_position_text(line)
            break
    
    # Extract location and salary if present
    location = extract_section(text, LOCATION_PATTERNS)
    salary = extract_section(text, SALARY_PATTERNS)
    
    # Extract job application link and Telegram link
    result['application_link'] = extract_job_link(text)
    result['telegram_link'] = extract_telegram_link(text)
    
    # Check if it's a high salary position and extract salary value
    if salary:
        result['high_salary'], result['salary'] = parse_salary(salary)
    
    # Split text into sections based on keywords
    sections = {}
    current_section = None
    current_text = []
    
    for line in lines:
        line_lower = line.lower()
        
        # Skip empty lines, links, and location/salary lines
        if (not line.strip() or 
            any(pattern in line_lower for pattern in ['📍', '💰', '💵', 'http', 'https', '[']) or
            line.startswith('#')):
            continue
        
        # Check if line contains any section keywords
        found_section = False
        for section, keywords in JOB_KEYWORDS.items():
            if any(keyword in line_lower for keyword in keywords):
                # Save previous section if exists
                if current_section and current_text:
                    sections[current_section] = '\n'.join(current_text).strip()
                current_section = section
                current_text = []
                found_section = True
                break
        
        if not found_section:
            if current_section:
                current_text.append(line)
            elif not result['position']:  # If no position found yet, this might be it
                result['position'] = clean_position_text(line.strip())
    
    # Save last section
    if current_section and current_text:
        sections[current_section] = '\n'.join(current_text).strip()
    
    # Extract what they offer
    if 'what_they_offer' in sections:
        result['what_they_offer'] = sections['what_they_offer'].strip()
    else:
        # Try to find offer in text without section headers
        offer_text = []
        for line in lines:
            if any(keyword in line.lower() for keyword in ['предлагаем', 'условия', 'график', 'schedule', 'conditions']):
                offer_text.append(line.strip())
        if offer_text:
            result['what_they_offer'] = '\n'.join(offer_text)
        else:
            # If no offer section found, use the description as what they offer
            description_lines = []
            for line in lines:
                if (line.strip() and 
                    not any(pattern in line.lower() for pattern in ['📍', '💰', '💵', 'http', 'https', '[']) and
                    not line.startswith('#')):
                    description_lines.append(line.strip())
            if description_lines:
                result['what_they_offer'] = '\n'.join(description_lines)
    
    # Add location and salary to what they offer if found
    if location or salary:
        offer_parts = []
        if result['what_they_offer']:
            offer_parts.append(result['what_they_offer'])
        if location:
            offer_parts.append(f"📍 {location}")
        if salary:
            offer_parts.append(f"💰 {salary}")
        result['what_they_offer'] = '\n'.join(offer_parts)
    
    # Calculate fit percentage and determine priority
    if result['position']:  # Only calculate if we found a position
        result['fit_percentage'] = calculate_fit_percentage(text)
        result['priority'] = determine_priority(result['fit_percentage'])
    
    return result

# Initialize Google Sheets
def setup_google_sheet():
    """Set up Google Sheets with proper formatting for both high and low salary tabs."""
    try:
        # Initialize Google Sheets client
        logger.info("Setting up Google Sheets connection...")
        
        # Parse Google credentials from environment variable
        try:
            # First, try to parse the JSON string directly
        try:
            credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            except json.JSONDecodeError:
                # If that fails, try to clean the string first
                cleaned_json = GOOGLE_CREDENTIALS_JSON.replace('\n', '\\n')
                credentials_dict = json.loads(cleaned_json)
                
            # Ensure the private key is properly formatted
            if 'private_key' in credentials_dict:
                credentials_dict['private_key'] = credentials_dict['private_key'].replace('\\n', '\n')
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in GOOGLE_CREDENTIALS_JSON: {str(e)}")
            raise ValueError(f"Invalid JSON in GOOGLE_CREDENTIALS_JSON: {str(e)}")
            
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=scopes
        )
        gc = gspread.authorize(creds)
        
        # Get the spreadsheet
        try:
            spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        except gspread.exceptions.APIError as e:
            logger.error(f"Failed to access Google Sheet with ID {GOOGLE_SHEET_ID}: {str(e)}")
            raise
        
        # Create or get the worksheets - one for high salary and one for low salary
        try:
            high_salary_sheet = spreadsheet.worksheet("High Salary Jobs")
        except gspread.WorksheetNotFound:
            high_salary_sheet = spreadsheet.add_worksheet("High Salary Jobs", 1000, 12)
            
        try:
            low_salary_sheet = spreadsheet.worksheet("Low Salary Jobs")
        except gspread.WorksheetNotFound:
            low_salary_sheet = spreadsheet.add_worksheet("Low Salary Jobs", 1000, 12)
        
        # Clear existing content
        high_salary_sheet.clear()
        low_salary_sheet.clear()
        
        # Add headers
        headers = [
            'Date', 'Channel', 'Position', 'Email', 'What They Offer',
            'Application Link', 'Telegram Link', 'Salary', 'High Salary',
            'Schedule Type', 'Job Type', 'Fit %'
        ]
        high_salary_sheet.append_row(headers)
        low_salary_sheet.append_row(headers)
        
        # Format headers
        header_format = {
            'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8},
            'textFormat': {'bold': True},
            'horizontalAlignment': 'CENTER'
        }
        
        # Apply header formatting to both sheets
        high_salary_sheet.format('A1:L1', header_format)
        low_salary_sheet.format('A1:L1', header_format)
        
        logger.info("Google Sheet setup completed successfully")
        return {"high_salary": high_salary_sheet, "low_salary": low_salary_sheet}
    except Exception as e:
        logger.error(f"Error setting up Google Sheet: {str(e)}")
        raise

# Initialize Telegram client
logger.info("Initializing Telegram client...")
if not all([API_ID, API_HASH, PHONE]):
    raise ValueError("Missing Telegram credentials. Please check your .env file.")

# Use a persistent session file name
SESSION_FILE = 'session'

# Initialize the client with the session file
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Store processed messages to avoid duplicates
processed_messages = load_processed_messages()

def determine_schedule_type(text):
    """Determine if the job has flexible schedule or low hours demand."""
    text_lower = text.lower()
    
    # Keywords for flexible schedule
    flexible_keywords = [
        'гибкий график', 'гибкое время', 'гибкий режим',
        'flexible hours', 'flexible schedule', 'flexible working hours',
        'flexible time', 'flexible regime', 'flexible work schedule',
        'flexible working time', 'flexible work regime',
        'свободный график', 'свободное время', 'свободный режим',
        'free schedule', 'free time', 'free regime',
        'удобный график', 'удобное время', 'удобный режим',
        'convenient schedule', 'convenient time', 'convenient regime'
    ]
    
    # Keywords for low hours demand
    low_hours_keywords = [
        'неполный день', 'частичная занятость', 'part-time',
        'part time', 'parttime', 'неполная занятость',
        'short hours', 'short working hours', 'short work day',
        'short work time', 'short working time',
        'занятость', 'employment', 'работа', 'work'
    ]
    
    # Patterns for specific time formats
    time_patterns = [
        r'(\d+)[-–](\d+)\s*(?:часа|часов|часов в день|часа в день|часов в неделю|часа в неделю)',
        r'(\d+)[-–](\d+)\s*(?:hours?|hours? per day|hours? per week)',
        r'(\d+)\s*(?:часа|часов|часов в день|часа в день|часов в неделю|часа в неделю)',
        r'(\d+)\s*(?:hours?|hours? per day|hours? per week)',
        r'занятость\s*(\d+)[-–](\d+)\s*(?:часа|часов|часов в день|часа в день|часов в неделю|часа в неделю)',
        r'занятость\s*(\d+)\s*(?:часа|часов|часов в день|часа в день|часов в неделю|часа в неделю)',
        r'employment\s*(\d+)[-–](\d+)\s*(?:hours?|hours? per day|hours? per week)',
        r'employment\s*(\d+)\s*(?:hours?|hours? per day|hours? per week)'
    ]
    
    # Check for flexible keywords first
    if any(keyword in text_lower for keyword in flexible_keywords):
        return 'Flexible Hours'
    
    # Check for specific time patterns
    for pattern in time_patterns:
        matches = re.findall(pattern, text_lower)
        if matches:
            # If it's a range (e.g., "1-2 hours")
            if len(matches[0]) == 2:
                min_hours, max_hours = map(int, matches[0])
                if max_hours <= 4:  # If maximum hours is 4 or less, consider it part-time
                    return 'Part-Time'
            # If it's a single number
            else:
                hours = int(matches[0][0])
                if hours <= 4:  # If hours is 4 or less, consider it part-time
                    return 'Part-Time'
    
    # Check for low hours keywords
    if any(keyword in text_lower for keyword in low_hours_keywords):
        return 'Part-Time'
    
    return 'Full-Time'

def is_remote_job(text):
    """Check if the job is remote and not a coding/programming job."""
    if not text:
        return False
        
    text_lower = text.lower()
    
    # First check for coding/programming jobs
    coding_keywords = [
        'программист', 'developer', 'разработчик', 'coder', 'programmer',
        'frontend', 'backend', 'fullstack', 'full-stack', 'full stack',
        'python', 'java', 'javascript', 'js', 'php', 'ruby', 'golang',
        'swift', 'kotlin', 'scala', 'rust', 'c++', 'c#', 'dotnet',
        'react', 'angular', 'vue', 'node.js', 'django', 'flask',
        'laravel', 'spring', 'spring boot', 'springboot', 'spring-boot',
        'aws', 'azure', 'cloud', 'devops', 'sre', 'qa', 'qа', 'тестировщик',
        'tester', 'testing', 'automation', 'автоматизация', 'ci/cd',
        'git', 'docker', 'kubernetes', 'k8s', 'jenkins', 'jira',
        'agile', 'scrum', 'kanban', 'sprint', 'sprint planning',
        'code review', 'code review', 'pull request', 'merge request',
        'branch', 'commit', 'repository', 'repo', 'gitlab', 'github',
        'bitbucket', 'stack overflow', 'stackoverflow', 'leetcode',
        'hackerrank', 'codewars', 'coding', 'programming', 'software',
        'it', 'айти', 'информационные технологии', 'технологии',
        'технический', 'technical', 'engineer', 'инженер', 'architect',
        'архитектор', 'system', 'системный', 'database', 'база данных',
        'sql', 'nosql', 'mongodb', 'postgresql', 'mysql', 'redis',
        'elasticsearch', 'kafka', 'rabbitmq', 'microservices',
        'микросервисы', 'api', 'rest', 'graphql', 'websocket',
        'websockets', 'socket.io', 'grpc', 'thrift', 'protobuf'
    ]
    
    if any(keyword in text_lower for keyword in coding_keywords):
        return False
    
    # Then check for non-remote indicators
    non_remote_indicators = [
        '📍', 'location:', 'место:', 'адрес:', 'address:',
        'москва', 'moscow', 'санкт-петербург', 'saint petersburg',
        'офис', 'office', 'офисная работа', 'office work',
        'в офисе', 'in office', 'в офисе компании', 'company office',
        'метро', 'metro', 'subway', 'станция', 'station',
        'ресторан', 'restaurant', 'кафе', 'cafe', 'бар', 'bar',
        'официант', 'waiter', 'waitress', 'бариста', 'barista',
        'повар', 'cook', 'chef', 'кухня', 'kitchen'
    ]
    
    if any(indicator in text_lower for indicator in non_remote_indicators):
        return False
    
    # Finally check for explicit remote keywords
    return any(keyword in text_lower for keyword in REMOTE_KEYWORDS)

async def process_message(message, channel):
    """Process a single message and save job data if it's a remote job vacancy."""
    try:
        # Get channel information for better logging
        channel_title = getattr(channel, 'title', str(channel)) if hasattr(channel, 'title') else str(channel)
        channel_id = getattr(channel, 'id', '') if hasattr(channel, 'id') else ''
        
        # Create a unique message identifier that includes the channel
        message_key = f"{channel_id}-{message.id}" if channel_id else str(message.id)
        
        # Skip if message has already been processed
        if message_key in processed_messages:
            logger.debug(f"Skipping already processed message {message.id} from {channel_title}")
            return
            
        # Extract message text, handling media messages
        text = ""
        if message.text:
            text = message.text
        elif message.caption:
            text = message.caption
        elif hasattr(message, 'message') and message.message:  # Some message types have message attribute
            text = message.message
            
        # Skip if text is too short to be a job posting
        if len(text) < 20:
            logger.debug(f"Skipping message with short text ({len(text)} chars) from {channel_title}")
            return
        
        # Skip if not a remote job
        if not is_remote_job(text):
            logger.debug(f"Skipping non-remote job from {channel_title}")
            return
        
        # Parse the job vacancy
        job_data = parse_job_vacancy(text)
        if not job_data:
            logger.debug(f"Couldn't parse job vacancy from message {message.id} in {channel_title}")
            return
            
        # Add channel name to job data
        job_data['channel'] = channel_title
        
        # Add message date
        if hasattr(message, 'date'):
            job_data['post_date'] = message.date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            job_data['post_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Add Telegram post link with proper formatting
        if hasattr(message, 'id'):
            try:
                if hasattr(channel, 'username') and channel.username:
                    # Use the channel's username for the link
                    job_data['telegram_link'] = f"https://t.me/{channel.username}/{message.id}"
                elif hasattr(channel, 'id') and channel.id:
                    # Use the channel ID if no username
                    job_data['telegram_link'] = f"https://t.me/c/{channel.id}/{message.id}"
                else:
                    # Fallback to channel title
                    channel_name = channel_title.replace(' ', '_').replace('@', '')
                    job_data['telegram_link'] = f"https://t.me/{channel_name}/{message.id}"
            except Exception as e:
                logger.error(f"Error creating Telegram link: {str(e)}")
                # Simple fallback
                job_data['telegram_link'] = f"Telegram post ID: {message.id}"
        
        # Calculate fit percentages for each category
        text_lower = text.lower()
        job_data['fit_percentage'] = calculate_fit_percentage(text)
        job_data['psych_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['psychology']) else 0
        job_data['coach_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['coaching']) else 0
        job_data['crypto_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['crypto']) else 0
        job_data['premium_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['premium']) else 0
        
        # Save to Google Sheet
        await save_to_sheet(job_data)
        
        # Mark message as processed using the unique key
        processed_messages.add(message_key)
        
        # Save processed messages occasionally to avoid losing progress
        # Use modulo operation on message ID as a simple way to decide when to save
        if message.id % 10 == 0:
            save_processed_messages(processed_messages)
        
        logger.info(f"Saved remote job vacancy: {job_data.get('position', 'Unknown position')} from {channel_title}")
        return True
    except Exception as e:
        logger.error(f"Error processing message {getattr(message, 'id', 'unknown')} from {getattr(channel, 'title', str(channel))}: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        return False

async def check_channels():
    """Check channels for new messages with improved error handling and recovery."""
    logger.info("Checking channels for new messages...")
    
    # Process channels in smaller chunks to manage memory and handle failures gracefully
    success_count = 0
    error_count = 0
    
    # Create chunks of channels to process
    chunks = [CHANNELS[i:i+5] for i in range(0, len(CHANNELS), 5)]
    
    for chunk_idx, channel_chunk in enumerate(chunks):
        logger.info(f"Processing channel chunk {chunk_idx+1}/{len(chunks)}...")
        
        for channel_name in channel_chunk:
            try:
                logger.info(f"Checking channel: {channel_name}")
                
                # Try to get the channel entity with retries
                channel = None
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                channel = await client.get_entity(channel_name)
                        break
                    except (ConnectionError, TimeoutError) as conn_err:
                        retry_count += 1
                        logger.warning(f"Connection error on attempt {retry_count}/{max_retries} for {channel_name}: {conn_err}")
                        if retry_count >= max_retries:
                            raise
                        await asyncio.sleep(2 * retry_count)  # Exponential backoff
                    except Exception as e:
                        logger.error(f"Unexpected error getting entity for {channel_name}: {e}")
                        raise
                
                if not channel:
                    logger.error(f"Failed to get entity for {channel_name} after {max_retries} attempts")
                    error_count += 1
                    continue
                
                # Process messages for this channel with a reasonable limit
                processed_count = await fetch_historical_messages(channel, limit=30)
                
                if processed_count > 0:
                    success_count += 1
                    logger.info(f"Successfully processed {processed_count} messages from {getattr(channel, 'title', channel_name)}")
                else:
                    logger.info(f"No new messages found in {getattr(channel, 'title', channel_name)}")
                
                # Process any queued items after each channel
                if (hasattr(save_to_sheet, 'high_salary_queue') and save_to_sheet.high_salary_queue) or \
                   (hasattr(save_to_sheet, 'low_salary_queue') and save_to_sheet.low_salary_queue):
                    await process_queues()
                
            except telethon.errors.ChannelPrivateError:
                logger.warning(f"Cannot access private channel: {channel_name}. Skipping.")
                error_count += 1
            except telethon.errors.FloodWaitError as e:
                wait_time = e.seconds
                logger.warning(f"Rate limit hit for {channel_name}. Need to wait {wait_time} seconds.")
                # If wait time is reasonable, wait and retry
                if wait_time <= 300:  # 5 minutes max
                    logger.info(f"Waiting {wait_time} seconds before continuing...")
                    await asyncio.sleep(wait_time)
                    # Try again after waiting
                    try:
                        channel = await client.get_entity(channel_name)
                        processed_count = await fetch_historical_messages(channel, limit=15)  # Reduced limit after flood wait
                        if processed_count > 0:
                            success_count += 1
                    except Exception as retry_err:
                        logger.error(f"Failed to retry after flood wait for {channel_name}: {retry_err}")
                        error_count += 1
                else:
                    logger.error(f"Flood wait time too long ({wait_time}s) for {channel_name}. Skipping for now.")
                    error_count += 1
            except Exception as e:
                logger.error(f"Error checking channel {channel_name}: {e}")
                logger.exception("Full traceback:")
                error_count += 1
            
            # Brief pause between channels to avoid rate limits
            await asyncio.sleep(2)
        
        # Process any queued items after each chunk
        if (hasattr(save_to_sheet, 'high_salary_queue') and save_to_sheet.high_salary_queue) or \
           (hasattr(save_to_sheet, 'low_salary_queue') and save_to_sheet.low_salary_queue):
            await process_queues()
        
        # Give the system a break between chunks
        await asyncio.sleep(5)
        
        # Force garbage collection between chunks
        gc.collect()
    
    logger.info(f"Channel check completed: {success_count} successful, {error_count} errors")
    return success_count, error_count

async def monitor_channels(client, sheets):
    """Monitor channels for new messages with improved error handling and recovery."""
    global client_global  # Use global client for other functions
    client_global = client
    
    # Initialize save_to_sheet static variables
    save_to_sheet.sheets = sheets
    save_to_sheet.high_salary_queue = []
    save_to_sheet.low_salary_queue = []
    save_to_sheet.last_batch_time = time.time()
    
    # Try to get the next row index for each sheet
    try:
        high_salary_sheet = sheets['high_salary']
        low_salary_sheet = sheets['low_salary']
        
        # Get all values in column A to find the next empty row
        high_salary_values = high_salary_sheet.col_values(1)  # Column A
        low_salary_values = low_salary_sheet.col_values(1)  # Column A
        
        save_to_sheet.high_salary_next_row = len(high_salary_values) + 1
        save_to_sheet.low_salary_next_row = len(low_salary_values) + 1
        
        # If sheets are empty, start from row 2 (after headers)
        if save_to_sheet.high_salary_next_row == 1:
            save_to_sheet.high_salary_next_row = 2
        if save_to_sheet.low_salary_next_row == 1:
            save_to_sheet.low_salary_next_row = 2
            
        logger.info(f"Starting row indices - High salary: {save_to_sheet.high_salary_next_row}, Low salary: {save_to_sheet.low_salary_next_row}")
                
    except Exception as e:
        logger.error(f"Error getting next row indices: {e}")
        logger.info("Using default row indices (2)")
        save_to_sheet.high_salary_next_row = 2
        save_to_sheet.low_salary_next_row = 2
    
    try:
        # Set up the event handler for new messages
        @client.on(events.NewMessage(chats=CHANNELS))
        async def new_message_handler(event):
            """Handle new messages from monitored channels."""
            try:
                # Get the channel entity
                chat = await event.get_chat()
                channel_title = getattr(chat, 'title', str(chat.id))
                
                message = event.message
                message_key = f"{getattr(chat, 'id', '')}-{message.id}"
                
                # Skip if already processed
                if message_key in processed_messages:
                    return
                
                # Process the message
                await process_message(message, chat)
                
                # Mark as processed
                processed_messages.add(message_key)
                save_processed_messages(processed_messages)
                
                # Process queues if they're getting large
                if (hasattr(save_to_sheet, 'high_salary_queue') and len(save_to_sheet.high_salary_queue) >= 5) or \
                   (hasattr(save_to_sheet, 'low_salary_queue') and len(save_to_sheet.low_salary_queue) >= 5):
                    await process_queues()
                    
            except Exception as e:
                logger.error(f"Error processing new message from {getattr(event, 'chat_id', 'unknown')}: {e}")
                logger.exception("Full traceback:")
        
        # First, check existing messages in the channels
        success_count, error_count = await check_channels()
        
        # Process any remaining queued items
        if (hasattr(save_to_sheet, 'high_salary_queue') and save_to_sheet.high_salary_queue) or \
           (hasattr(save_to_sheet, 'low_salary_queue') and save_to_sheet.low_salary_queue):
            await process_queues()
        
        if error_count > 0:
            logger.warning(f"Completed initial scan with {error_count} errors")
        
        # Now start listening for new messages
        logger.info("Now listening for new messages...")
        
        # Set up a periodic check task
        async def periodic_check():
            """Periodically check channels and process queues."""
            while True:
                try:
                    logger.info("Running periodic channel check...")
                    await check_channels()
                    
                    # Process any remaining queued items
                    if (hasattr(save_to_sheet, 'high_salary_queue') and save_to_sheet.high_salary_queue) or \
                       (hasattr(save_to_sheet, 'low_salary_queue') and save_to_sheet.low_salary_queue):
                        await process_queues()
                        
                    # Clean up memory
                    gc.collect()
                    
                    # Check memory usage if psutil is available
                    try:
                        import psutil
                        process = psutil.Process(os.getpid())
                        mem_info = process.memory_info()
                        logger.info(f"Current memory usage: {mem_info.rss / 1024 / 1024:.2f} MB")
                    except ImportError:
                        pass
                    
                    # Wait for the next check
                    check_interval = CHECK_INTERVAL_HOURS * 60 * 60  # Convert to seconds
                    logger.info(f"Next check in {CHECK_INTERVAL_HOURS} hours")
                    await asyncio.sleep(check_interval)
                    
                except Exception as e:
                    logger.error(f"Error in periodic check: {e}")
                    logger.exception("Full traceback:")
                    # Wait before retrying
                    await asyncio.sleep(300)  # 5 minutes
        
        # Start the periodic check task
        asyncio.create_task(periodic_check())
        
        # Keep the main task running to receive new messages
        while True:
            await asyncio.sleep(3600)  # Check every hour if still running
            
    except Exception as e:
        logger.error(f"Error in monitor_channels: {e}")
        logger.exception("Full traceback:")
        raise

async def fetch_historical_messages(channel, limit=100):
    """Fetch historical messages from a channel with memory optimization."""
    logger.info(f"Fetching historical messages from {getattr(channel, 'title', channel)}")
    
    try:
        # Process messages in smaller chunks to manage memory
        batch_size = 20
        total_processed = 0
        last_message_id = 0
        
        while total_processed < limit:
            current_batch_size = min(batch_size, limit - total_processed)
            
            # Get messages in chunks using offset_id to paginate
            messages = []
            try:
        messages = await client.get_messages(
            channel,
                    limit=current_batch_size,
                    offset_id=last_message_id if last_message_id else 0,
                    reverse=last_message_id != 0  # Only use reverse for pagination
        )
            except Exception as e:
                logger.error(f"Error fetching messages from {getattr(channel, 'title', channel)}: {e}")
                break
        
            # If no messages returned, we've reached the end
            if not messages:
                logger.info(f"No more messages to fetch from {getattr(channel, 'title', channel)}")
                break
        
            # Process each message in the batch
            valid_messages = 0
        for message in messages:
                if not message or not message.id:
                    continue
                    
                # Update last_message_id for pagination
                if not last_message_id or message.id < last_message_id:
                    last_message_id = message.id
                
                # Skip if already processed
                message_key = f"{getattr(channel, 'id', '')}-{message.id}"
                if message_key in processed_messages:
                    continue
                    
                # Process the message
                await process_message(message, channel)
                
                # Mark as processed
                processed_messages.add(message_key)
                valid_messages += 1
                
                # Process Google Sheets queue to prevent memory buildup
                if (hasattr(save_to_sheet, 'high_salary_queue') and len(save_to_sheet.high_salary_queue) >= 10) or \
                   (hasattr(save_to_sheet, 'low_salary_queue') and len(save_to_sheet.low_salary_queue) >= 10):
                    await process_queues()
            
            # Update total processed count
            total_processed += valid_messages
            
            # Save processed messages periodically
            if total_processed % 50 == 0:
                save_processed_messages(processed_messages)
                
            # Give the system a break to prevent memory build-up
            await asyncio.sleep(1)
            
            # Force garbage collection periodically
            if total_processed % 50 == 0:
                gc.collect()
                
        # Final save of processed messages
        save_processed_messages(processed_messages)
        
        logger.info(f"Finished fetching historical messages from {getattr(channel, 'title', channel)}: {total_processed} processed")
        return total_processed
                
    except Exception as e:
        logger.error(f"Error fetching historical messages from {getattr(channel, 'title', channel)}: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        return 0

async def authenticate_client(client):
    """Authenticate the Telegram client with proper error handling and session management."""
    logger.info("Starting authentication process...")
    
    try:
        # First, try to connect to Telegram
        await client.connect()
        
        # Check if we're already authorized
        is_authorized = await client.is_user_authorized()
        
        if is_authorized:
            logger.info("Already authenticated with existing session")
            
            # Verify the session is valid by getting user information
            try:
                me = await client.get_me()
                if me and hasattr(me, 'id'):
                    logger.info(f"Successfully verified as {getattr(me, 'first_name', 'Unknown')} (id: {me.id})")
                    return True
                else:
                    logger.warning("Session appears invalid: get_me() returned incomplete user data")
                    # Session might be corrupted, force re-authentication
                    logger.info("Re-authenticating...")
                    is_authorized = False
            except Exception as e:
                logger.warning(f"Failed to verify existing session: {e}")
                # Session error, need to re-authenticate
                logger.info("Re-authenticating...")
                is_authorized = False
        
        if not is_authorized:
            if RENDER and TELEGRAM_CODE:
            logger.info("Running on Render, using TELEGRAM_CODE from environment")
            try:
                    # First, request the code if needed
                    await client.send_code_request(PHONE)
                    # Then sign in with the code
                    await client.sign_in(phone=PHONE, code=TELEGRAM_CODE)
            except telethon.errors.SessionPasswordNeededError:
                logger.error("Two-factor authentication is required but not supported in Render environment")
                    return False
        else:
                logger.info("Interactive authentication required")
                try:
                    await client.send_code_request(PHONE)
                    code = input('Please enter the verification code you received: ')
                    await client.sign_in(phone=PHONE, code=code)
                    
                    # Handle two-factor authentication if needed
                    if await client.is_user_authorized():
                        logger.info("Successfully authenticated")
                    else:
                        # This branch would execute if 2FA is enabled
                        password = input('Please enter your 2FA password: ')
                        await client.sign_in(password=password)
                except telethon.errors.SessionPasswordNeededError:
                    # Handle 2FA explicitly
                    password = input('Two-factor authentication is enabled. Please enter your password: ')
                    await client.sign_in(password=password)
                except Exception as e:
                    logger.error(f"Interactive authentication failed: {e}")
                    return False
            
            # Verify the new authentication
            try:
                me = await client.get_me()
                if me and hasattr(me, 'id'):
                    logger.info(f"Successfully authenticated as {getattr(me, 'first_name', 'Unknown')} (id: {me.id})")
                    return True
                else:
                    logger.error("Authentication verification failed: get_me() returned incomplete user data")
                    return False
            except Exception as e:
                logger.error(f"Failed to verify new authentication: {e}")
                return False
        
        return True
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        logger.exception("Full authentication error traceback:")
        return False

async def main():
    """Main function to run the Telegram job monitor."""
    client = None
    
    try:
        # Initialize Telegram client
        logger.info("Initializing Telegram client...")
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
        
        # Authenticate the client
        auth_success = await authenticate_client(client)
        if not auth_success:
            logger.error("Authentication failed, exiting...")
            return
        
        # Test the parser
        logger.info("Testing job vacancy parser...")
        test_message = "Официант Лолита\nhttps://vitrina.jobs/card/?filters638355975=id__eq__1745&utm_source=tg&utm_medium=vacancy_17.02-23.02&utm_campaign=horeca_oficiant_lolita\n\nОфициант Лолита\nМы очень уютный ресторан расположенный в старинном особняке 18 века Демидовых, Рахмановых на Таганской, с открытой кухней и сногсшибательной атмосферой.\n📍 Москва"
        job_data = parse_job_vacancy(test_message)
        logger.info("\nTest Results:")
        for key, value in job_data.items():
            logger.info(f"{key}: {value}")
        logger.info("\nParser test successful! Starting the main script...")
        
        # Set up Google Sheets
        logger.info("Testing Google Sheets connection...")
        sheets = setup_google_sheet()
        logger.info("Google Sheets connection successful!")
        
        # Start monitoring
        logger.info("Script started")
        await monitor_channels(client, sheets)
        
    except Exception as e:
        logger.error(f"Script stopped due to error: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        raise
    finally:
        if client and client.is_connected():
            await client.disconnect()
            logger.info("Telegram client disconnected")

def run_schedule():
    """Run the monitoring process using schedule with memory optimizations."""
    logger.info("Starting scheduled task...")
    
    # Log memory usage if psutil is available
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        logger.info(f"Current memory usage: {mem_info.rss / 1024 / 1024:.2f} MB")
    except ImportError:
        logger.info("psutil not available, skipping memory usage logging")
    
    async def run_monitoring():
        try:
            # Clean up old message IDs (older than 30 days)
            current_time = time.time()
            cutoff_time = current_time - (7 * 24 * 60 * 60 if RENDER else 30 * 24 * 60 * 60)
            old_messages = {msg_id for msg_id in processed_messages if msg_id < cutoff_time}
            if old_messages:
                processed_messages.difference_update(old_messages)
                save_processed_messages(processed_messages)
                logger.info(f"Cleaned up {len(old_messages)} old message IDs")
            
            # Process channels in smaller batches to manage memory
            print("\nFetching historical messages from channels...")
            chunks = [CHANNELS[i:i+5] for i in range(0, len(CHANNELS), 5)]
            
            for chunk in chunks:
                for channel_id in chunk:
                try:
                    channel = await client.get_entity(channel_id)
                    await fetch_historical_messages(channel)
                    print(f"Successfully fetched historical messages from {channel.title}")
                        # Process queue after each channel
                        if hasattr(save_to_sheet, 'queue') and save_to_sheet.queue:
                            await process_queue()
                        # Add a delay between channels
                        await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"Error processing historical messages for {channel_id}: {e}")
                    logger.exception("Full traceback:")
                
                # Process any remaining queued items after each chunk
                if hasattr(save_to_sheet, 'queue') and save_to_sheet.queue:
                    await process_queue()
                
                # Log memory usage after processing each chunk
                try:
                    import psutil
                    process = psutil.Process(os.getpid())
                    mem_info = process.memory_info()
                    logger.info(f"Memory usage after chunk: {mem_info.rss / 1024 / 1024:.2f} MB")
                except ImportError:
                    pass
                
                # Force garbage collection
                import gc
                gc.collect()
                
                # Add a longer delay between chunks to reduce memory pressure
                await asyncio.sleep(15)
            
            # Then check for new messages
            await check_channels()
            
            # Final processing of any remaining items
            if hasattr(save_to_sheet, 'queue') and save_to_sheet.queue:
                await process_queue()
            
        except Exception as e:
            logger.error(f"Error in run_monitoring: {e}")
            logger.exception("Full traceback:")
    
    # Use the existing event loop instead of creating a new one
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_monitoring())

async def save_to_sheet(job_data):
    """Save job data to Google Sheet with memory optimization."""
    try:
        # Initialize Google Sheets client and batch queue if not already done
        if not hasattr(save_to_sheet, 'sheets'):
            sheets = setup_google_sheet()
            save_to_sheet.sheets = sheets
            save_to_sheet.high_salary_queue = []
            save_to_sheet.low_salary_queue = []
            save_to_sheet.last_batch_time = 0
            save_to_sheet.high_salary_next_row = 2  # Start from row 2 (after headers)
            save_to_sheet.low_salary_next_row = 2   # Start from row 2 (after headers)
        
        # Determine schedule type and job type
        schedule_type = determine_schedule_type(job_data.get('what_they_offer', ''))
        job_type = determine_job_type(job_data.get('what_they_offer', ''))
        
        # Extract email
        email = extract_email(job_data.get('what_they_offer', ''))
        
        # Get salary from job_data
        salary = job_data.get('salary', '')
        high_salary = job_data.get('high_salary', False)
        
        # Prepare row data
        row_data = [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            job_data.get('channel', ''),
            job_data.get('position', ''),
            email,
            job_data.get('what_they_offer', ''),
            job_data.get('application_link', ''),
            job_data.get('telegram_link', ''),
            salary,
            'Yes' if high_salary else 'No',
            schedule_type,
            job_type,
            job_data.get('fit_percentage', '')
        ]
        
        # Add the row to the appropriate queue
        if high_salary:
            save_to_sheet.high_salary_queue.append(row_data)
            logger.info(f"Queued high salary job: {job_data.get('position', 'Unknown position')} from {job_data.get('channel', '')}")
        else:
            save_to_sheet.low_salary_queue.append(row_data)
            logger.info(f"Queued low salary job: {job_data.get('position', 'Unknown position')} from {job_data.get('channel', '')}")
        
        # Process the queues if they reach a certain size or if enough time has passed
        current_time = time.time()
        high_queue_size = len(save_to_sheet.high_salary_queue)
        low_queue_size = len(save_to_sheet.low_salary_queue)
        time_since_last_batch = current_time - save_to_sheet.last_batch_time
        
        # Use smaller batch size on Render to reduce memory usage
        max_batch_size = 5 if RENDER else 10
        batch_interval = 20 if RENDER else 30  # seconds
        
        # Process the queues if:
        # 1. Either queue has collected enough rows, or
        # 2. It's been enough time since the last batch and we have items to process
        if (high_queue_size >= max_batch_size or low_queue_size >= max_batch_size or 
            (time_since_last_batch > batch_interval and (high_queue_size > 0 or low_queue_size > 0))):
            await process_queues()
        
    except Exception as e:
        logger.error(f"Error saving to Google Sheet: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def process_queues():
    """Process the queued job data for both high and low salary jobs and write to Google Sheets in batches."""
    if (not hasattr(save_to_sheet, 'high_salary_queue') or not save_to_sheet.high_salary_queue) and \
       (not hasattr(save_to_sheet, 'low_salary_queue') or not save_to_sheet.low_salary_queue):
        return
    
    try:
        high_queue_size = len(save_to_sheet.high_salary_queue) if hasattr(save_to_sheet, 'high_salary_queue') else 0
        low_queue_size = len(save_to_sheet.low_salary_queue) if hasattr(save_to_sheet, 'low_salary_queue') else 0
        
        logger.info(f"Processing queues: {high_queue_size} high salary jobs, {low_queue_size} low salary jobs")
        
        # Get the current queues and reset for new items
        high_queue_to_process = save_to_sheet.high_salary_queue.copy() if hasattr(save_to_sheet, 'high_salary_queue') else []
        low_queue_to_process = save_to_sheet.low_salary_queue.copy() if hasattr(save_to_sheet, 'low_salary_queue') else []
        
        save_to_sheet.high_salary_queue = []
        save_to_sheet.low_salary_queue = []
        save_to_sheet.last_batch_time = time.time()
        
        # Process high salary queue
        if high_queue_to_process:
            await process_single_queue("high_salary", high_queue_to_process)
            
        # Process low salary queue
        if low_queue_to_process:
            await process_single_queue("low_salary", low_queue_to_process)
            
    except Exception as e:
        # Add items back to the queues if processing failed
        if hasattr(save_to_sheet, 'high_salary_queue') and 'high_queue_to_process' in locals():
            save_to_sheet.high_salary_queue.extend(high_queue_to_process)
        if hasattr(save_to_sheet, 'low_salary_queue') and 'low_queue_to_process' in locals():
            save_to_sheet.low_salary_queue.extend(low_queue_to_process)
            
        logger.error(f"Error processing queues: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def process_single_queue(queue_type, queue_to_process):
    """Process a single queue (either high or low salary) and write to the appropriate sheet."""
    if not queue_to_process:
        return
        
    try:
        sheet = save_to_sheet.sheets[queue_type]
        next_row_attr = f"{queue_type}_next_row"
        
        # Determine next row only once per batch
        try:
            # Get all values in column A to find the next empty row
            all_values = sheet.col_values(1)  # Column A
            next_row = len(all_values) + 1
            if next_row == 1:  # Empty sheet, add headers
                next_row = 2  # Skip header row
            setattr(save_to_sheet, next_row_attr, next_row)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit exceeded when finding next row for {queue_type}. Using estimated row.")
                next_row = getattr(save_to_sheet, next_row_attr)
                # Add delay before proceeding
                await asyncio.sleep(5)
            else:
                raise
        
        # Try to batch append the rows with exponential backoff
        max_retries = 5
        retry_delay = 2
        
        for retry in range(max_retries):
            try:
                # Use batch append instead of individual inserts
                cell_range = f"A{next_row}:L{next_row + len(queue_to_process) - 1}"
                sheet.update(cell_range, queue_to_process)
                logger.info(f"Successfully wrote {len(queue_to_process)} rows to {queue_type} sheet")
                setattr(save_to_sheet, next_row_attr, next_row + len(queue_to_process))
                break
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429 and retry < max_retries - 1:
                    logger.warning(f"Rate limit exceeded when batch writing to {queue_type}. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    # If we still can't write after max retries, add items back to the queue
                    if queue_type == "high_salary":
                        save_to_sheet.high_salary_queue.extend(queue_to_process)
                    else:
                        save_to_sheet.low_salary_queue.extend(queue_to_process)
                    logger.error(f"Failed to write batch to {queue_type} after {max_retries} attempts.")
                    raise
    except Exception as e:
        # Add items back to the appropriate queue
        if queue_type == "high_salary":
            save_to_sheet.high_salary_queue.extend(queue_to_process)
        else:
            save_to_sheet.low_salary_queue.extend(queue_to_process)
            
        logger.error(f"Error processing {queue_type} queue: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

def extract_email(text):
    """Extract email address from text."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, text)
    return match.group(0) if match else ''

def determine_job_type(text):
    """Determine job type based on keywords."""
    text_lower = text.lower()
    job_types = []
    
    if any(keyword in text_lower for keyword in FIT_KEYWORDS['psychology']):
        job_types.append('Psychology')
    if any(keyword in text_lower for keyword in FIT_KEYWORDS['coaching']):
        job_types.append('Coaching')
    if any(keyword in text_lower for keyword in FIT_KEYWORDS['crypto']):
        job_types.append('Crypto')
    if any(keyword in text_lower for keyword in FIT_KEYWORDS['premium']):
        job_types.append('Premium')
        
    return ', '.join(job_types) if job_types else 'Other'

def run():
    """Run the application using a new improved authentication flow."""
    # Load processed messages
    global processed_messages
    processed_messages = load_processed_messages()
    
    # Set up the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    client = None
    
    try:
        # Initialize Telegram client
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
        
        # Handle authentication in the main coroutine
        loop.run_until_complete(main())
            
            # Now start the scheduled monitoring
            schedule.every(CHECK_INTERVAL_HOURS).hours.do(run_schedule)
            
            while True:
                schedule.run_pending()
                time.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("Script stopped by user")
            print("\nScript stopped by user. Press Ctrl+C again to exit.")
        except Exception as e:
            logger.error(f"Script stopped due to error: {e}")
            logger.exception("Full traceback:")
            print(f"\nError: {e}")
        finally:
            # Ensure proper cleanup
            try:
            if client and client.is_connected():
                    loop.run_until_complete(client.disconnect())
                logger.info("Telegram client disconnected")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
            finally:
            if loop and not loop.is_closed():
                loop.close() 
                logger.info("Event loop closed")
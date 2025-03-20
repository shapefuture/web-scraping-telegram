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

# Load credentials from environment variables
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
RENDER = os.getenv('RENDER', '').lower() == 'true'
TELEGRAM_CODE = os.getenv('TELEGRAM_CODE')
SESSION_FILE = 'session'  # Changed from 'anon' to 'session' to match existing session file

# Add after other global variables
PROCESSED_MESSAGES_FILE = 'processed_messages.pkl'

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
    
    # Handle various number formats:
    # 1. Replace common separators with dots and remove plus symbol
    salary_text = salary_text.replace(',', '.').replace('+', '')
    
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
                return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
        
        # Then try to find a range with spaces and dash
        range_match = re.search(r'(\d+(?:\s\d+)*)\s*[–-]\s*(\d+(?:\s\d+)*)', salary_text)
        if range_match:
            # Remove spaces and convert to numbers
            min_salary = int(range_match.group(1).replace(' ', ''))
            max_salary = int(range_match.group(2).replace(' ', ''))
            # Consider high salary if max salary is above threshold
            if max_salary >= 100000:
                return True, f"{min_salary:,}-{max_salary:,} RUB".replace(',', ' ')
        
        # If no range found, try to find a single number with spaces
        single_match = re.search(r'(\d+(?:\s\d+)*)', salary_text)
        if single_match:
            # Remove spaces and convert to number
            salary = int(single_match.group(1).replace(' ', ''))
            if salary >= 100000:
                return True, f"{salary:,} RUB".replace(',', ' ')
    
    # Handle USD salary ranges (e.g., "300-2000$")
    if '$' in salary_text:
        # First try to find a range with dash
        range_match = re.search(r'(\d+(?:\.\d+)?)\s*[–-]\s*(\d+(?:\.\d+)?)\s*\$', salary_text)
        if range_match:
            min_salary, max_salary = map(float, range_match.groups())
            if max_salary >= 1000:  # Check max salary
                return True, f"{min_salary:,.0f}-{max_salary:,.0f} USD".replace(',', ' ')
        
        # Then try to find a range with dots
        range_match = re.search(r'(\d+(?:\.\d+)?)\s*[–-]\s*(\d+(?:\.\d+)?)', salary_text)
        if range_match:
            min_salary, max_salary = map(float, range_match.groups())
            if max_salary >= 1000:  # Check max salary
                return True, f"{min_salary:,.0f}-{max_salary:,.0f} USD".replace(',', ' ')
    
    # Handle k/K format with $ (e.g., 1.2-2.5к$, 1.2k-2.5k$)
    if 'k' in salary_text and '$' in salary_text:
        # Extract numbers before k/K
        matches = re.findall(r'(\d+(?:\.\d+)?)k', salary_text)
        if matches:
            # Convert k to actual number
            numbers = [float(num) * 1000 for num in matches]
            if len(numbers) == 2:  # Range format
                if numbers[1] >= 1000:  # Check max salary
                    return True, f"{numbers[0]:,.0f}-{numbers[1]:,.0f} USD".replace(',', ' ')
            else:  # Single number
                if numbers[0] >= 1000:
                    return True, f"{numbers[0]:,.0f} USD".replace(',', ' ')
    
    # Handle k/K format (e.g., $26k, 26K)
    if 'k' in salary_text:
        # Extract the number before k/K
        match = re.search(r'(\d+(?:\.\d+)?)k', salary_text)
        if match:
            number = float(match.group(1))
            # Convert k to actual number
            number = number * 1000
            if 'usd' in salary_text or '$' in salary_text:
                if number >= 1000:
                    return True, f"{number:,.0f} USD".replace(',', ' ')
            elif 'eur' in salary_text or '€' in salary_text:
                if number >= 1000:
                    return True, f"{number:,.0f} EUR".replace(',', ' ')
            elif 'руб' in salary_text or '₽' in salary_text or 'р.' in salary_text or 'рублей' in salary_text:
                if number >= 100000:
                    return True, f"{number:,.0f} RUB".replace(',', ' ')
    
    # Extract all numbers from the text
    numbers = re.findall(r'\d+(?:\.\d+)?', salary_text)
    if not numbers:
        return False, None
    
    # Convert numbers to float
    try:
        numbers = [float(num) for num in numbers]
    except ValueError:
        return False, None
    
    # Check for USD
    if 'usd' in salary_text or '$' in salary_text:
        for num in numbers:
            if num >= 1000:
                return True, f"{num:,.0f} USD".replace(',', ' ')
                
    # Check for RUB
    if 'руб' in salary_text or '₽' in salary_text or 'р.' in salary_text or 'рублей' in salary_text:
        for num in numbers:
            if num >= 100000:
                return True, f"{num:,.0f} RUB".replace(',', ' ')
                
    # Check for EUR
    if 'eur' in salary_text or '€' in salary_text:
        for num in numbers:
            if num >= 1000:
                return True, f"{num:,.0f} EUR".replace(',', ' ')
                
    # If no currency specified, assume RUB
    for num in numbers:
        if num >= 100000:
            return True, f"{num:,.0f} RUB".replace(',', ' ')
            
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
    """Set up Google Sheets with proper formatting."""
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
        
        # Create or get the main worksheet
        try:
            sheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(WORKSHEET_NAME, 1000, 12)
        
        # Clear existing content
        sheet.clear()
        
        # Add headers
        headers = [
            'Date', 'Channel', 'Position', 'Email', 'What They Offer',
            'Application Link', 'Telegram Link', 'Salary', 'High Salary',
            'Schedule Type', 'Job Type', 'Fit %'
        ]
        sheet.append_row(headers)
        
        # Format headers
        header_format = {
            'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8},
            'textFormat': {'bold': True},
            'horizontalAlignment': 'CENTER'
        }
        
        # Apply header formatting
        sheet.format('A1:L1', header_format)
        
        logger.info("Google Sheet setup completed successfully")
        return sheet
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

async def process_message(message, channel_name):
    """Process a single message and save job data if it's a remote job vacancy."""
    try:
        # Skip if message has already been processed
        if message.id in processed_messages:
            logger.debug(f"Skipping already processed message {message.id} from {channel_name}")
            return
            
        # Extract message text
        text = message.text if message.text else ""
        
        # Skip if not a remote job
        if not is_remote_job(text):
            logger.debug(f"Skipping non-remote job from {channel_name}")
            return
        
        # Parse the job vacancy
        job_data = parse_job_vacancy(text)
        if not job_data:
            return
            
        # Add channel name to job data
        job_data['channel'] = channel_name
        
        # Add Telegram post link with proper formatting
        if hasattr(message, 'id'):
            try:
                # Get the channel entity to get its username
                channel = await client.get_entity(channel_name)
                if hasattr(channel, 'username') and channel.username:
                    # Use the channel's username for the link
                    job_data['telegram_link'] = f"https://t.me/{channel.username}/{message.id}"
                else:
                    # If no username, use the channel name without @ symbol
                    channel_username = channel_name.replace('@', '')
                    job_data['telegram_link'] = f"https://t.me/{channel_username}/{message.id}"
            except Exception as e:
                logger.error(f"Error getting channel username: {str(e)}")
                # Fallback to using channel name without @ symbol
                channel_username = channel_name.replace('@', '')
                job_data['telegram_link'] = f"https://t.me/{channel_username}/{message.id}"
        
        # Calculate fit percentages for each category
        text_lower = text.lower()
        job_data['fit_percentage'] = calculate_fit_percentage(text)
        job_data['psych_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['psychology']) else 0
        job_data['coach_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['coaching']) else 0
        job_data['crypto_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['crypto']) else 0
        job_data['premium_percentage'] = 25 if any(keyword in text_lower for keyword in FIT_KEYWORDS['premium']) else 0
        
        # Save to Google Sheet
        await save_to_sheet(job_data)
        
        # Mark message as processed
        processed_messages.add(message.id)
        save_processed_messages(processed_messages)
        
        logger.info(f"Saved remote job vacancy: {job_data.get('position', 'Unknown position')} from {channel_name}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def check_channels():
    """Check all channels for new messages."""
    try:
        for channel_name in CHANNELS:
            try:
                logger.info(f"Checking channel: {channel_name}")
                channel = await client.get_entity(channel_name)
                logger.info(f"Successfully got entity for {channel_name}")
                
                # Get messages from the last 30 days
                messages = await client.get_messages(
                    channel,
                    limit=100,
                    offset_date=datetime.now() - timedelta(days=30)
                )
                
                logger.info(f"Retrieved {len(messages)} messages from {channel_name}")
                
                # Process each message
                for message in messages:
                    logger.debug(f"Processing message {message.id} from {channel_name}")
                    await process_message(message, channel.title)
                    
            except Exception as e:
                logger.error(f"Error checking channel {channel_name}: {str(e)}")
                logger.error("Full traceback:", exc_info=True)
                continue
                
    except Exception as e:
        logger.error(f"Error in check_channels: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def fetch_historical_messages(channel):
    """Fetch historical messages from a channel with memory optimization."""
    try:
        logger.info(f"Fetching historical messages from {channel.title}")
        
        # Reduce the limit for Render to save memory
        message_limit = 30 if RENDER else 100
        day_limit = 7 if RENDER else 30
        
        # Get messages from the last N days with reduced limit
        messages = await client.get_messages(
            channel,
            limit=message_limit,
            offset_date=datetime.now() - timedelta(days=day_limit)
        )
        
        logger.info(f"Retrieved {len(messages)} messages from {channel.title}")
        
        # Process each message with periodic garbage collection
        for i, message in enumerate(messages):
            if message.id not in processed_messages:
                await process_message(message, channel.title)
                processed_messages.add(message.id)
                
                # Save processed messages periodically to avoid keeping too many in memory
                if i % 10 == 0:
                    save_processed_messages(processed_messages)
                    
                    # Process the queue to avoid memory buildup
                    if hasattr(save_to_sheet, 'queue') and save_to_sheet.queue:
                        await process_queue()
                    
                    # Short sleep to avoid rate limits and reduce CPU usage
                    await asyncio.sleep(0.1)
                
    except Exception as e:
        logger.error(f"Error fetching historical messages from {channel.title}: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def main():
    """Main function to run the Telegram job monitor."""
    try:
        # Initialize Telegram client
        logger.info("Initializing Telegram client...")
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
        
        # Start the client
        await client.start(phone=PHONE)
        
        # If running on Render, use the TELEGRAM_CODE environment variable
        if RENDER and not client.is_user_authorized():
            logger.info("Running on Render, using TELEGRAM_CODE from environment")
            try:
                await client.sign_in(phone=PHONE, code=TELEGRAM_CODE)
            except telethon.errors.SessionPasswordNeededError:
                logger.error("Two-factor authentication is required but not supported in Render environment")
                raise
        else:
            logger.info("Running locally or already authenticated")
        
        logger.info("Successfully authenticated with Telegram")
        
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
        sheet = setup_google_sheet()
        logger.info("Google Sheets connection successful!")
        
        # Start monitoring
        logger.info("Script started")
        await monitor_channels(client, sheet)
        
    except Exception as e:
        logger.error(f"Script stopped due to error: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        raise
    finally:
        if 'client' in locals():
            await client.disconnect()

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
        if not hasattr(save_to_sheet, 'sheet'):
            save_to_sheet.sheet = setup_google_sheet()
            save_to_sheet.queue = []
            save_to_sheet.last_batch_time = 0
            save_to_sheet.next_row = 2  # Start from row 2 (after headers)
        
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
        
        # Add the row to the queue
        save_to_sheet.queue.append(row_data)
        
        # Process the queue if it reaches a certain size or if enough time has passed
        current_time = time.time()
        queue_size = len(save_to_sheet.queue)
        time_since_last_batch = current_time - save_to_sheet.last_batch_time
        
        # Use smaller batch size on Render to reduce memory usage
        max_batch_size = 5 if RENDER else 10
        batch_interval = 20 if RENDER else 30  # seconds
        
        # Process the queue if:
        # 1. We have collected enough rows, or
        # 2. It's been enough time since the last batch
        if queue_size >= max_batch_size or (queue_size > 0 and time_since_last_batch > batch_interval):
            await process_queue()
        
        logger.info(f"Queued job data: {job_data.get('position', 'Unknown position')} from {job_data.get('channel', '')}")
        
    except Exception as e:
        logger.error(f"Error saving to Google Sheet: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def process_queue():
    """Process the queued job data and write to Google Sheets in batch."""
    if not hasattr(save_to_sheet, 'queue') or not save_to_sheet.queue:
        return
    
    try:
        logger.info(f"Processing queue with {len(save_to_sheet.queue)} items")
        
        # Get the current queue and reset for new items
        queue_to_process = save_to_sheet.queue.copy()
        save_to_sheet.queue = []
        save_to_sheet.last_batch_time = time.time()
        
        # Determine next row only once per batch
        try:
            # Get all values in column A to find the next empty row
            all_values = save_to_sheet.sheet.col_values(1)  # Column A
            next_row = len(all_values) + 1
            if next_row == 1:  # Empty sheet, add headers
                next_row = 2  # Skip header row
            save_to_sheet.next_row = next_row
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded when finding next row. Using estimated row.")
                next_row = save_to_sheet.next_row
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
                save_to_sheet.sheet.update(cell_range, queue_to_process)
                logger.info(f"Successfully wrote {len(queue_to_process)} rows to Google Sheet")
                save_to_sheet.next_row += len(queue_to_process)
                break
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429 and retry < max_retries - 1:
                    logger.warning(f"Rate limit exceeded when batch writing. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    # If we still can't write after max retries, add items back to the queue
                    save_to_sheet.queue.extend(queue_to_process)
                    logger.error(f"Failed to write batch after {max_retries} attempts. Queue size: {len(save_to_sheet.queue)}")
                    raise
    except Exception as e:
        # Add items back to the queue if processing failed
        save_to_sheet.queue.extend(queue_to_process)
        logger.error(f"Error processing queue: {str(e)}")
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

def load_processed_messages():
    """Load the set of processed message IDs from file."""
    try:
        if os.path.exists(PROCESSED_MESSAGES_FILE):
            with open(PROCESSED_MESSAGES_FILE, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        logger.error(f"Error loading processed messages: {e}")
    return set()

def save_processed_messages(processed_messages):
    """Save the set of processed message IDs to file."""
    try:
        with open(PROCESSED_MESSAGES_FILE, 'wb') as f:
            pickle.dump(processed_messages, f)
    except Exception as e:
        logger.error(f"Error saving processed messages: {e}")

if __name__ == "__main__":
    # Create a client instance
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    
    # Check if we're running on Render to skip tests
    if RENDER:
        print("Running on Render, skipping tests to conserve memory")
    else:
        # First, test the parser (only when not on Render)
        print("Testing job vacancy parser...")
        test_message = "Официант Лолита\nhttps://vitrina.jobs/card/?filters638355975=id__eq__1745&utm_source=tg&utm_medium=vacancy_17.02-23.02&utm_campaign=horeca_oficiant_lolita\n\nОфициант Лолита\nМы очень уютный ресторан расположенный в старинном особняке 18 века Демидовых, Рахмановых на Таганской, с открытой кухней и сногсшибательной атмосферой.\n📍 Москва"
        job_data = parse_job_vacancy(test_message)
        print("\nTest Results:")
        for key, value in job_data.items():
            print(f"{key}: {value}")
            
        print("\nParser test successful! Starting the main script...")
    
    loop = asyncio.get_event_loop()
    try:
        logger.info("Script started")
        
        # Test Google Sheets connection
        logger.info("Testing Google Sheets connection...")
        setup_google_sheet()
        
        # Run authentication first
        print("\n=== Telegram Channel Monitor ===")
        print("Starting authentication process...")
        print(f"Using phone number: {PHONE}")
        
        # Connect to Telegram
        loop.run_until_complete(client.connect())
        
        # Try to use existing session
        try:
            logger.info("Attempting to use existing session...")
            # Check if session file exists
            if os.path.exists(f"{SESSION_FILE}.session"):
                # Make sure to properly await the coroutine
                is_authorized = loop.run_until_complete(client.is_user_authorized())
                if not is_authorized:
                    # If not authorized, try to sign in
                    if RENDER and TELEGRAM_CODE:
                        logger.info("Using TELEGRAM_CODE from environment for authentication")
                        # First connect
                        loop.run_until_complete(client.connect())
                        # Then sign in with code
                        loop.run_until_complete(client.sign_in(phone=PHONE, code=TELEGRAM_CODE))
                    else:
                        logger.info("Interactive authentication required")
                        # Use the built-in interactive sign-in process
                        loop.run_until_complete(client.connect())
                        if not loop.run_until_complete(client.is_user_authorized()):
                            loop.run_until_complete(client.send_code_request(PHONE))
                            code = input('Please enter the code you received: ')
                            loop.run_until_complete(client.sign_in(phone=PHONE, code=code))
                else:
                    logger.info("Already authenticated using existing session")
            else:
                logger.info("No session file found. Creating new session...")
                if RENDER and TELEGRAM_CODE:
                    logger.info("Using TELEGRAM_CODE from environment for initial authentication")
                    # First connect
                    loop.run_until_complete(client.connect())
                    # Send code request
                    loop.run_until_complete(client.send_code_request(PHONE))
                    # Then sign in with code
                    loop.run_until_complete(client.sign_in(phone=PHONE, code=TELEGRAM_CODE))
                else:
                    logger.info("Interactive authentication required")
                    # Use the built-in interactive sign-in process
                    loop.run_until_complete(client.connect())
                    if not loop.run_until_complete(client.is_user_authorized()):
                        loop.run_until_complete(client.send_code_request(PHONE))
                        code = input('Please enter the code you received: ')
                        loop.run_until_complete(client.sign_in(phone=PHONE, code=code))
                    
            # Verify authentication by trying to get your own user info
            try:
                me = loop.run_until_complete(client.get_me())
                if me:
                    logger.info(f"Successfully authenticated as {me.first_name if hasattr(me, 'first_name') else 'Unknown'} (id: {me.id if hasattr(me, 'id') else 'Unknown'})")
                else:
                    logger.error("Authentication verification failed: get_me() returned None")
                    raise ValueError("Authentication verification failed")
            except Exception as e:
                logger.error(f"Failed to verify authentication: {e}")
                raise
                
        except Exception as e:
            logger.error(f"Failed to authenticate: {e}")
            if RENDER:
                logger.error("Please ensure TELEGRAM_CODE environment variable is set correctly")
            else:
                logger.error("Please run the script locally first to create a session file")
            raise
        
        print("\nAuthentication successful!")
        
        # Now start the scheduled monitoring
        schedule.every(CHECK_INTERVAL_HOURS).hours.do(run_schedule)
        run_schedule()  # Run immediately instead of waiting for the first interval
        
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
            if client.is_connected():
                loop.run_until_complete(client.disconnect())
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            loop.close() 
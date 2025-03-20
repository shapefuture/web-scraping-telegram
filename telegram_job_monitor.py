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
                else:
                    # If no message ID found, try to find it in the message object
                    if hasattr(message, 'id'):
                        link = f"{link}/{message.id}"
                    else:
                        return None
            
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

def parse_job_vacancy(text):
    """Parse job vacancy text to extract specific fields."""
    # Initialize result dictionary
    result = {
        'position': '',
        'application_link': '',
        'telegram_link': '',
        'what_they_offer': '',
        'source': '',
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
            credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
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
SESSION_FILE = 'telegram_session'

# Initialize the client with the session file
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Store processed messages to avoid duplicates
processed_messages = set()

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
    """Fetch historical messages from a channel."""
    try:
        logger.info(f"Fetching historical messages from {channel.title}")
        
        # Get messages from the last 30 days
        messages = await client.get_messages(
            channel,
            limit=100,
            offset_date=datetime.now() - timedelta(days=30)
        )
        
        logger.info(f"Retrieved {len(messages)} messages from {channel.title}")
        
        # Process each message
        for message in messages:
            if message.id not in processed_messages:
                await process_message(message, channel.title)
                processed_messages.add(message.id)
                
    except Exception as e:
        logger.error(f"Error fetching historical messages from {channel.title}: {str(e)}")
        logger.error("Full traceback:", exc_info=True)

async def main():
    """Main function to run the Telegram job monitor."""
    try:
        # Initialize Telegram client
        logger.info("Initializing Telegram client...")
        client = TelegramClient('anon', API_ID, API_HASH)
        
        # Start the client
        await client.start(phone=PHONE)
        
        # If running on Render, use the TELEGRAM_CODE environment variable
        if RENDER:
            logger.info("Running on Render, using TELEGRAM_CODE from environment")
            try:
                await client.sign_in(code=TELEGRAM_CODE)
            except telethon.errors.SessionPasswordNeededError:
                logger.error("Two-factor authentication is required but not supported in Render environment")
                raise
        else:
            logger.info("Running locally, using interactive authentication")
            await client.start()
        
        logger.info("Successfully authenticated with Telegram")
        
        # Test the parser
        logger.info("Testing job vacancy parser...")
        test_message = "Официант Лолита\nhttps://vitrina.jobs/card/?filters638355975=id__eq__1745&utm_source=tg&utm_medium=vacancy_17.02-23.02&utm_campaign=horeca_oficiant_lolita\n\nОфициант Лолита\nМы очень уютный ресторан расположенный в старинном особняке 18 века Демидовых, Рахмановых на Таганской, с открытой кухней и сногсшибательной атмосферой.\n📍 Москва"
        job_data = parse_job_vacancy(test_message, "test_channel")
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
    """Run the monitoring process using schedule."""
    logger.info("Starting scheduled task...")
    
    async def run_monitoring():
        try:
            # First, fetch historical messages from all channels
            print("\nFetching historical messages from all channels...")
            for channel_id in CHANNELS:
                try:
                    channel = await client.get_entity(channel_id)
                    await fetch_historical_messages(channel)
                    print(f"Successfully fetched historical messages from {channel.title}")
                except Exception as e:
                    logger.error(f"Error processing historical messages for {channel_id}: {e}")
                    logger.exception("Full traceback:")
            
            # Then check for new messages
            await check_channels()
            
        except Exception as e:
            logger.error(f"Error in run_monitoring: {e}")
            logger.exception("Full traceback:")
    
    # Use the existing event loop instead of creating a new one
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_monitoring())

async def save_to_sheet(job_data):
    """Save job data to Google Sheet."""
    try:
        # Initialize Google Sheets client if not already done
        if not hasattr(save_to_sheet, 'sheet'):
            save_to_sheet.sheet = setup_google_sheet()
        
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
            salary,  # Use the salary value directly
            'Yes' if high_salary else 'No',
            schedule_type,
            job_type,
            job_data.get('fit_percentage', '')
        ]
        
        # Find the next empty row
        next_row = 2  # Start from row 2 (after headers)
        while next_row < 1000:  # Limit to prevent infinite loop
            if not any(save_to_sheet.sheet.row_values(next_row)):
                break
            next_row += 1
        
        # Append row to sheet
        save_to_sheet.sheet.insert_row(row_data, next_row)
        
        logger.info(f"Saved job data to Google Sheet: {job_data.get('position', 'Unknown position')} with salary: {salary}")
        
    except Exception as e:
        logger.error(f"Error saving to Google Sheet: {str(e)}")
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

if __name__ == "__main__":
    # Test the parser first
    print("Testing job vacancy parser...")
    test_result = test_parse_job_vacancy()
    
    # Continue with the main script if test is successful
    if test_result['position']:
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
            
            # Check if we have a valid session file
            if os.path.exists(f"{SESSION_FILE}.session"):
                logger.info("Found existing session file, attempting to use it...")
                try:
                    loop.run_until_complete(client.start())
                    logger.info("Successfully restored session")
                except Exception as e:
                    logger.warning(f"Failed to restore session: {e}")
                    logger.info("Proceeding with new authentication...")
                    if not loop.run_until_complete(client.is_user_authorized()):
                        # Send code request
                        loop.run_until_complete(client.send_code_request(PHONE))
                        logger.info("Verification code sent to Telegram")
                        
                        # Wait a moment for the code to be sent
                        time.sleep(5)
                        
                        # Check if running on Render
                        if RENDER:
                            if not TELEGRAM_CODE:
                                logger.error("TELEGRAM_CODE environment variable is required when running on Render")
                                raise ValueError("TELEGRAM_CODE environment variable is required when running on Render")
                            code = TELEGRAM_CODE
                            logger.info("Using verification code from environment variable")
                        else:
                            print("\nPlease enter the verification code you received: ")
                            code = input()
                        
                        try:
                            # Add a small delay before attempting to sign in
                            time.sleep(2)
                            loop.run_until_complete(client.sign_in(PHONE, code))
                        except telethon.errors.PhoneCodeInvalidError:
                            logger.error("Invalid verification code. Please check the code and try again.")
                            raise
                        except telethon.errors.SessionPasswordNeededError:
                            if RENDER:
                                logger.error("Two-factor authentication is required but not supported in Render environment")
                                raise ValueError("Two-factor authentication is required but not supported in Render environment")
                            print("\nTwo-factor authentication is enabled.")
                            print("Please enter your 2FA password: ")
                            password = input()
                            loop.run_until_complete(client.sign_in(password=password))
                        print("\nAuthentication successful!")
                except Exception as e:
                    logger.error(f"Failed to restore session: {e}")
                    logger.info("Proceeding with new authentication...")
                    if not loop.run_until_complete(client.is_user_authorized()):
                        # Send code request
                        loop.run_until_complete(client.send_code_request(PHONE))
                        logger.info("Verification code sent to Telegram")
                        
                        # Wait a moment for the code to be sent
                        time.sleep(5)
                        
                        # Check if running on Render
                        if RENDER:
                            if not TELEGRAM_CODE:
                                logger.error("TELEGRAM_CODE environment variable is required when running on Render")
                                raise ValueError("TELEGRAM_CODE environment variable is required when running on Render")
                            code = TELEGRAM_CODE
                            logger.info("Using verification code from environment variable")
                        else:
                            print("\nPlease enter the verification code you received: ")
                            code = input()
                        
                        try:
                            # Add a small delay before attempting to sign in
                            time.sleep(2)
                            loop.run_until_complete(client.sign_in(PHONE, code))
                        except telethon.errors.PhoneCodeInvalidError:
                            logger.error("Invalid verification code. Please check the code and try again.")
                            raise
                        except telethon.errors.SessionPasswordNeededError:
                            if RENDER:
                                logger.error("Two-factor authentication is required but not supported in Render environment")
                                raise ValueError("Two-factor authentication is required but not supported in Render environment")
                            print("\nTwo-factor authentication is enabled.")
                            print("Please enter your 2FA password: ")
                            password = input()
                            loop.run_until_complete(client.sign_in(password=password))
                        print("\nAuthentication successful!")
            else:
                logger.info("No existing session found, starting new authentication...")
                if not loop.run_until_complete(client.is_user_authorized()):
                    # Send code request
                    loop.run_until_complete(client.send_code_request(PHONE))
                    logger.info("Verification code sent to Telegram")
                    
                    # Wait a moment for the code to be sent
                    time.sleep(5)
                    
                    # Check if running on Render
                    if RENDER:
                        if not TELEGRAM_CODE:
                            logger.error("TELEGRAM_CODE environment variable is required when running on Render")
                            raise ValueError("TELEGRAM_CODE environment variable is required when running on Render")
                        code = TELEGRAM_CODE
                        logger.info("Using verification code from environment variable")
                    else:
                        print("\nPlease enter the verification code you received: ")
                        code = input()
                    
                    try:
                        # Add a small delay before attempting to sign in
                        time.sleep(2)
                        loop.run_until_complete(client.sign_in(PHONE, code))
                    except telethon.errors.PhoneCodeInvalidError:
                        logger.error("Invalid verification code. Please check the code and try again.")
                        raise
                    except telethon.errors.SessionPasswordNeededError:
                        if RENDER:
                            logger.error("Two-factor authentication is required but not supported in Render environment")
                            raise ValueError("Two-factor authentication is required but not supported in Render environment")
                        print("\nTwo-factor authentication is enabled.")
                        print("Please enter your 2FA password: ")
                        password = input()
                        loop.run_until_complete(client.sign_in(password=password))
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
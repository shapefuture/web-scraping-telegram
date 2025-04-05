import re
import logging
from datetime import datetime

from typing import List, Optional

logger = logging.getLogger(__name__)

# Default keywords, consider moving to config or making configurable
DEFAULT_FIT_KEYWORDS = ['python', 'javascript', 'react', 'node', 'web', 'full-stack', 'backend', 'frontend', 'remote', 'developer', 'engineer', 'software']
DEFAULT_SALARY_THRESHOLD = 100000

def parse_job_vacancy(
    text: str,
    channel_title: str,
    message_timestamp: Optional[datetime] = None,
    salary_threshold: int = DEFAULT_SALARY_THRESHOLD,
    fit_keywords: List[str] = DEFAULT_FIT_KEYWORDS
) -> Optional[dict]:
    """
    Parse job vacancy message text into structured data.

    Args:
        text: The message text.
        channel_title: The title of the channel the message came from.
        message_timestamp: The original timestamp of the message (optional).
        salary_threshold: The threshold to determine 'high_salary' (optional).
        fit_keywords: List of keywords to calculate fit percentage (optional).

    Returns:
        A dictionary containing parsed job data, or None if parsing fails
        or no position is identified.
    """
    if not text:
        return None
    try:
        # Basic job data structure
        job_data = {
            'channel': channel_title,
            'position': '',
            'what_they_offer': text,
            'application_link': '',
            'telegram_link': '',
            'salary': '',
            'high_salary': False,
            'fit_percentage': 0,
            'schedule_type': 'Unknown',
            'job_type': 'Unknown',
            'email': '',
            'timestamp': (message_timestamp or datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        }

        lines = text.split('\n')

        # Extract position (more robustly) - Keywords could be made configurable
        position_keywords = ['position:', 'role:', 'job:', 'vacancy:', 'looking for:', 'hiring:']
        for i, line in enumerate(lines):
            line_lower = line.lower()
            for keyword in position_keywords:
                if keyword in line_lower:
                    # Take text after the keyword
                    potential_position = line.split(keyword, 1)[1].strip()
                    if potential_position:
                        job_data['position'] = potential_position
                        break
            if job_data['position']:
                break
        # Fallback: use the first non-empty line if no keyword found
        if not job_data['position']:
            for line in lines:
                stripped_line = line.strip()
                if stripped_line:
                    job_data['position'] = stripped_line
                    break

        # Extract salary information
        # Regex attempts to handle various formats like $50k, 100K USD, $100,000 - $120,000
        # Note: Regex can be brittle and might misinterpret numbers.
        salary_pattern = r'\$?(\d{1,3}(?:[,.]?\d{3})*)\s?([kK])?(?:\s*-\s*\$?(\d{1,3}(?:[,.]?\d{3})*)\s?([kK])?)?(?:\s*(?:USD|EUR|GBP))?'
        salary_matches = re.findall(salary_pattern, text)
        extracted_salaries = []
        if salary_matches:
            for match in salary_matches:
                low_val_str, low_k, high_val_str, high_k = match
                try:
                    low_val = int(low_val_str.replace(',', '').replace('.', ''))
                    if low_k: low_val *= 1000

                    salary_str = f"${low_val:,}"
                    if low_k: salary_str += "k"

                    if high_val_str:
                        high_val = int(high_val_str.replace(',', '').replace('.', ''))
                        if high_k: high_val *= 1000
                        salary_str += f" - ${high_val:,}"
                        if high_k: salary_str += "k"
                        # Use the lower end for high_salary check
                        job_data['high_salary'] = low_val >= salary_threshold
                    else:
                        job_data['high_salary'] = low_val >= salary_threshold

                    extracted_salaries.append(salary_str)

                except (ValueError, IndexError):
                    continue # Ignore malformed salary strings
            if extracted_salaries:
                 job_data['salary'] = " | ".join(extracted_salaries) # Join if multiple found


        # Extract application link and Telegram link
        # Prioritize links containing keywords. Link regex might capture non-links.
        # Note: Currently only captures the first identified link of each type.
        link_pattern = r'https?://[^\s<>"\')]+|t\.me/[^\s<>"\')]+|@\w+' # Basic link/mention pattern
        links = re.findall(link_pattern, text)
        app_link_keywords = ['apply', 'career', 'job', 'form', 'link'] # Could be configurable
        potential_app_links = []
        potential_tg_links = []

        for link in links:
            if link.startswith('@') or 't.me/' in link:
                potential_tg_links.append(link)
            elif any(keyword in link.lower() for keyword in app_link_keywords):
                 potential_app_links.append(link)
            elif not job_data['application_link']: # Fallback if no keyword match
                 potential_app_links.append(link)

        job_data['application_link'] = potential_app_links[0] if potential_app_links else ''
        job_data['telegram_link'] = potential_tg_links[0] if potential_tg_links else ''


        # Extract Email using helper function
        job_data['email'] = extract_email(text)

        # Determine Schedule Type and Job Type
        job_data['schedule_type'] = determine_schedule_type(text)
        job_data['job_type'] = determine_job_type(text)


        # Calculate fit percentage based on provided keywords
        text_lower = text.lower()
        matches = sum(1 for keyword in fit_keywords if keyword in text_lower)
        job_data['fit_percentage'] = int((matches / len(fit_keywords)) * 100) if fit_keywords else 0

        # Only return if a position was identified
        if not job_data['position']:
             logger.debug(f"No position found in message from {channel_title}. Skipping.")
             return None

        return job_data

    except Exception as e:
        logger.error(f"Error parsing job vacancy from channel '{channel_title}': {e}", exc_info=True)
        # Log the problematic text for debugging (optional, consider privacy)
        # logger.debug(f"Problematic text: {text[:500]}...")
        return None


def determine_schedule_type(text: str) -> str:
    """
    Determine the schedule type (e.g., Full-time, Part-time, Remote) from text.
    Note: Simple keyword matching can be inaccurate.
    """
    text_lower = text.lower()
    # Order matters slightly (e.g., check part-time before time)
    if 'remote' in text_lower or 'work from home' in text_lower or 'wfh' in text_lower:
        return 'Remote'
    if 'hybrid' in text_lower:
        return 'Hybrid'
    if 'full-time' in text_lower or 'full time' in text_lower:
        return 'Full-time'
    if 'part-time' in text_lower or 'part time' in text_lower:
        return 'Part-time'
    if 'contract' in text_lower: # Contract check before part-time/full-time
        return 'Contract'
    if 'freelance' in text_lower:
        return 'Freelance'
    if 'internship' in text_lower:
        return 'Internship'
    if 'on-site' in text_lower or 'office' in text_lower: # Check before time-based
        return 'On-site'
    return 'Unknown' # Default

def determine_job_type(text: str) -> str:
    """
    Determine the job type (e.g., Permanent, Contract) from text.
    Note: Simple keyword matching can be inaccurate.
    """
    text_lower = text.lower()
    # Order matters
    if 'contract' in text_lower or 'fixed-term' in text_lower:
        return 'Contract'
    if 'freelance' in text_lower:
        return 'Freelance'
    if 'internship' in text_lower:
        return 'Internship'
    if 'permanent' in text_lower or 'full-time' in text_lower: # Often implies permanent
        return 'Permanent'
    if 'freelance' in text_lower:
        return 'Freelance'
    if 'internship' in text_lower:
        return 'Internship'
    return 'Unknown'

def extract_email(text: str) -> str:
    """Extract email address from text using regex."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    return emails[0] if emails else ''

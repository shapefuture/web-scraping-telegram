import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Telegram API credentials
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Google Sheets settings
SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_ID')
WORKSHEET_NAME = 'Channel Messages'

# List of Telegram channels/groups to monitor
CHANNELS = [
    'vitrinajobs',  # Remote jobs channel
    'razoom_jobs',          # Job search channel
    'dddwork',             # Work opportunities channel
    
    # Психология
    'onlinevakansii',
    'dexto',
    'ugskpm',              # Работа в Правительстве Москвы
    'careerspace',         # General channel with potential psychology roles
    'zigmund_co',          # Зигмунд и Ко | Вакансии в психологии

    # Коучинг
    'happymonday',
    'zigmund_co',          # Зигмунд и Ко | Вакансии в психологии (includes coaching roles)

    # Крипто
    'cryptoheadhunter',    # Крипто HeadHunter | WEB 3 Jobs
    'cryptojobslist',
    'ethereum_jobs_market',
    'yourweb3jobs',
    'ethereum_hh',
    'eth_jobs',
    'job_web3',
    'bloconomica',
    'Blockchain_geeklink',
    'soliditydevjobs',
    'CryptoHunterWeb3',    # Крипто HeadHunter | WEB 3 Jobs
    'holder_job_devs',     # WEB3 Builders
    'near_jobs',           # Near Jobs
    'solana_jobs',         # Solana Jobs
    'tonjobs',             # TON Jobs
    'work2earn_crypto',    # Networking and crypto/DeFi roles
    'workingincrypto',     # General crypto roles
    'rabotaweb3',          # Web 3 related jobs
    'workers_tg',          # General with crypto executive roles
    'cryptojobs',          # General crypto with community roles

    # Управление сообществом
    'dna325',
    'work2earn_crypto',    # Potential community management in crypto/web 3

    # Образование
    'edujobs',
    'edmarketclubjob',
    'dl_marketplace',
    '+_4OZTSkAl3xhYTk6',   # Приватная ссылка
    'onwards_upwards',     # Onwards & Upwards
    'online_schools_chat', # Онлайн школы. Чат
    'zapusk_online_2',     # Запуск онлайн-школы 2.0
    'scholars_faculty',
    'vacancies_edu',       # Вакансии в образовании

    # Люкс
    'onlinevakansii',

    # Креативные индустрии
    'designizer',
    'mediajobs_ru',
    'jobpower',
    'rueventjob',
    'talentedpeoples',
    'cgfreelance',
    'cresume',
    'vdhl_good',           # VDHL.RU
    'textodromo',          # Текстодром
    'work_copywriters',    # Работа для копирайтеров и редакторов
    'Work4writers',        # Work for writers
    'kopirayter_kopirayting', # Копирайтер Редактор – Вакансии
    'self_ma',             # Копирайтер, редактор — удаленная работа
    'work_editor',         # Работа с текстами — вакансии
    'edit_zp',             # Редактируем зп | Вакансии для копирайтеров
    'worklis',             # Design and visual arts
    'design_hunters',      # Design Hunters
    'post_wintour_jobs',   # POST WINTOUR JOBS

    # Поддержка
    'dna325',
    'RabotaUdalenka',      # Features support-related roles

    # Маркетинг
    'normrabota',
    'workasap',
    'dnative_job',
    'vacanciesrus',
    'vacancysmm',
    'freelance_vacancy',
    'digitalcv',
    'prwork',
    'forallmarketing',
    'marketing_jobs',
    'seohr',
    'perezvonyu',
    'adgoodashell',
    'freelancegigs',       # Приватная ссылка
    'smm_leads',
    'reclamodrom',         # Рекламодром
    'digitalhr',           # DigitalHR
    'holder_job_marketing',# Web 3 marketing

    # Общие каналы для удаленной работы
    'hh_vacancy_udalenka', # Удаленная работа от hh.ru
    'alenavladimirskaya',  # Вакансии от Алены Владимирской
    'finder_vc',           # Finder.vc
    'tgwork',              # Удалённая работа / TGwork
    'remocate',            # Remocate
    'home_office',         # Хоум офис | Удаленно
    'kon_na_udalenke',     # Конь на удаленке
    'interesting_work',    # Интересная работа в Москве и на удаленке
    'premium_udalenka',    # Премиум удаленка
    'skillbridge',         # SkillBridge
    'paradise_work',       # Paradise Work - Удаленная работа | Фриланс
    'freelance_tavern',    # Фриланс Таверна Official

    # Междисциплинарные каналы
    'Well_paid_Job',
    'forallmedia'          # Roles for editors and copywriters across fields
]

# Time settings
CHECK_INTERVAL_HOURS = 1 
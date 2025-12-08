import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_BACKUP_PATH = os.getenv(r'DEFAULT_BACKUP_PATH', r'localhost')

SETTINGS_FILE = os.getenv('SETTINGS_FILE', 'connection_history.json')

ODBC_DRIVER = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')

DEFAULT_USER = os.getenv('DEFAULT_USER', 'sa')

# Интервал проверки планировщика (в миллисекундах)
SCHEDULER_CHECK_INTERVAL = int(os.getenv('SCHEDULER_CHECK_INTERVAL', '30000'))


import logging 
from pathlib import Path

# ToDo: move this to a version.py?
__version__ = '0.1'

logger = logging.getLogger(__name__)

CONFIG_FILE = Path(__file__).parent / "config.json"
if not CONFIG_FILE.exists():
    logger.error('config.json not found. Should be created at: '
                 '{}'.format(CONFIG_FILE))
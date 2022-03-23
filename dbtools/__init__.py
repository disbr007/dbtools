import logging 
from pathlib import Path

# ToDo: move this to a version.py?
__version__ = '1.0.20'

logger = logging.getLogger(__name__)

# TODO: Improve how this is located (users home, here, environmental variable, etc.)
CONFIG_FILE = Path(__file__).parent / "dbtools-config.yml"
# if not CONFIG_FILE.exists():
    # logger.error('config.json not found. Should be created at: '
                #  '{}'.format(CONFIG_FILE))
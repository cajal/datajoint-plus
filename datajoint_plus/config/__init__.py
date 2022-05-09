import os
from enum import Enum

from datajoint.settings import config

class DEFAULTS(Enum):
    DJ_LOGLEVEL = 'INFO'
    DJ_LOG_BASE_DIR = 'logs' 

for default in DEFAULTS:
    if os.getenv(default.name) is None:
        os.environ[default.name] = default.value

config_mapping = {
    'loglevel': DEFAULTS.DJ_LOGLEVEL,
    'log_base_dir': DEFAULTS.DJ_LOG_BASE_DIR,
}

for k, v in config_mapping.items():
    config.update({k: os.getenv(v.name)})


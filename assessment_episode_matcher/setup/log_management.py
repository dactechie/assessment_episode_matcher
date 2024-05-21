import os
import shutil
from datetime import datetime

import logging
from logging.config import dictConfig

def configure_logging(log_dir, running_file:str, log_suffix:str=""):
    timestamp = datetime.now().strftime("%H")
    if log_suffix:
        log_suffix = f"-{log_suffix}"
    fname = f"{log_dir}/{timestamp}{log_suffix}.log"
    log_config = {
        'version': 1,
        'handlers': {
            'fileHandler': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'myFormatter',
                'filename': fname,
                'mode': 'a'
            }
        },
        'formatters': {
            'myFormatter': {
                'format': '%(asctime)s - %(levelname)s - %(message)s'
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['fileHandler']
        }
    }
    dictConfig(log_config)
    logger = logging.getLogger(running_file)
    return logger


def setup_logdir_by_currentdate(env_suffix:str="") -> str:
  
    base_log_dir = f"logs_{env_suffix}" if env_suffix  else "logs"

    # if env and env != 'prod':
    #     base_log_dir = f'{env}/{base_log_dir}'

    archive_dir = os.path.join(base_log_dir, 'archive')

    yyyymmdd_str = datetime.now().strftime('%Y-%m-%d')
    today_log_dir = os.path.join(base_log_dir, yyyymmdd_str)

    # Create base log directory if it doesn't exist
    if not os.path.exists(base_log_dir):
        os.makedirs(base_log_dir)

    # Create archive directory if it doesn't exist
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    # Move the old log directory to the archive if today's directory doesn't exist
    if not os.path.exists(today_log_dir):
        for dir_name in os.listdir(base_log_dir):
            dir_path = os.path.join(base_log_dir, dir_name)
            if os.path.isdir(dir_path) and dir_name != 'archive':
                shutil.move(dir_path, archive_dir)

        # Create today's log directory
        os.makedirs(today_log_dir)

    return today_log_dir


if __name__ == '__main__':
  log_dir = setup_logdir_by_currentdate ()
  purpose = ""
  logger = configure_logging(log_dir,  __name__, purpose)
  logger.debug("Help")
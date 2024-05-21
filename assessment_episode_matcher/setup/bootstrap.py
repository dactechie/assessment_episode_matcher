from logging import Logger
import os
# import sys
from typing import Optional
from pathlib import Path
# import keyring
# import getpass
# from tcli.utils.common.config import ConfigLoader
from assessment_episode_matcher.utils.environment import ConfigManager #, ConfigKeys
from assessment_episode_matcher.setup.log_management import setup_logdir_by_currentdate, configure_logging

# config:dict 
# logger = None


def setup_config(config_file) -> dict :
    ConfigManager.setup('dev')
    cfg = ConfigManager().config
    return cfg
    # config_loader = ConfigLoader(config_file=config_file)
    # config_loader.load_config()
    # return config_loader

def setup_directories(root: Path, env:str=""):
    # suffix = config.get("env_suffix")
    if env == 'dev' or env == 'test':
        suffix = f"{os.sep}{env}"

    data_dir = Path(f'{root}{os.sep}data{suffix}')
    data_dir.mkdir(exist_ok=True)
    in_dir = data_dir / 'in'
    in_dir.mkdir(exist_ok=True)
    out_dir = data_dir / 'out'
    out_dir.mkdir(exist_ok=True)
    ew_dir = out_dir / 'errors_warnings'
    ew_dir.mkdir(exist_ok=True)
    processed_dir = data_dir / 'processed'
    processed_dir.mkdir(exist_ok=True)

    return data_dir, in_dir, out_dir, ew_dir, processed_dir
    

def setup_logging(env:str=""):
    today_log_dir = setup_logdir_by_currentdate(env)
    logger = configure_logging(today_log_dir,__name__,"")
    return logger, today_log_dir


class Bootstrap:
  _instance = None
  config: dict
  logger :Logger
  today_log_dir:str
    

  def __new__(cls):
    if cls._instance is None:
        cls._instance = super(Bootstrap, cls).__new__(cls)
        # cls._instance.env = 'local'
    return cls._instance
  
  @classmethod
  def setup(cls, root: Path, env:str=""):
      cls.config = setup_config(env)
      cls.data_dir, cls.in_dir, cls.out_dir, \
        cls.ew_dir, cls.processed_dir = setup_directories(root, env)
      cls.logger, cls.today_log_dir = setup_logging(env)
      return cls

      
  # @property
  # def config(self):
  #     return self.config    

  # @property
  # def config(self):
  #     return self.data_dir
  

# def setup(env:str=""):
#     global logger, config
#     config = setup_config(env)
#     # if not env:
#     #     print("env = dev")
#     #     env = "dev"

#     # current_directory = os.path.dirname(__file__)
#     # package_root_path = os.path.abspath(os.path.join(current_directory, os.pardir, os.pardir))
#     # config_file_path = os.path.join(package_root_path, f'{env}.config.yaml')
#     # config_loader:ConfigLoader = setup_config(config_file_path)
#     # if not config_loader:
#     #     sys.exit("no Config file was present")

#     # else:
  
#     #     env = config_loader.get("stage", {}).get("stage_name", "dev")
#     #     print(f"Setting config from config file {env}")

#     # config_loader["env"] =  env
#     # config_loader["env_suffix"]  = "" if env == 'prod' else env

#     # config = config_loader.config
    
#     data_dir = setup_directories(env)

#     logger, _ = setup_logging(env)
#     return config, logger, data_dir #for testing purposes
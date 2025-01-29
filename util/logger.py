import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from strategy import STRATEGY_DIR_PATH

class StrategyLogger():
    def __init__(self, name: str) -> None:
        self.logger_name = name
        self.log_directory = f'{STRATEGY_DIR_PATH}/log/{self.logger_name}'
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self._check_dir_exist()
        handler = CustomTimedRotatingFileHandler(f'{self.log_directory}/trade')
        formatter = logging.Formatter(f"%(asctime)s [{name}] %(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info(f'Initialize {name} StrategyLogger')
    
    def _check_dir_exist(self) -> None:
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when="midnight", interval=1, backupCount=7, encoding="utf-8"):
        self.base_filename = filename
        self.current_time = datetime.now().strftime("%Y_%m_%d")
        filename = f"{self.base_filename}_{self.current_time}.log"
        super().__init__(filename, when, interval, backupCount, encoding)

    def doRollover(self):
        self.stream.close()
        self.current_time = datetime.now().strftime("%Y_%m_%d")
        new_filename = f"{self.base_filename}_{self.current_time}.log"
        self.base_filename = new_filename
        self.stream = self._open()

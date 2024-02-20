import logging, os
from logging.handlers import RotatingFileHandler

class Logger:
    def __init__(self, name, log_file, level=logging.INFO, log_to_console=True):
        """
        Initialize a Logger instance.
        """
        # self.ensure_dir(log_file)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.setup_file_handler(log_file, level)
        if log_to_console:
            self.setup_console_handler(level)


    def setup_file_handler(self, log_file, level):
        """
        Set up file handler for logging to a file.
        """
        file_handler = RotatingFileHandler(log_file, maxBytes=1024*1024*5, backupCount=5)  # 5 MB per file, 5 files backup
        file_handler.setLevel(level)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def setup_console_handler(self, level):
        """
        Set up console handler for logging to the console.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        """
        Get the configured logger.
        """
        return self.logger
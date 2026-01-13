import logging
from typing import Optional
import sys

class CalculatorLogger:
    """Centralized logging for NoSQL Database Calculator"""
    
    def __init__(self, verbose: bool = True, level: int = logging.INFO):
        """
        Initialize the logger.
        
        Args:
            verbose: If True, outputs messages to console
            level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.verbose = verbose
        self.logger = logging.getLogger("NoSQLCalculator")
        self.logger.setLevel(level)
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Add console handler if verbose
        if verbose:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            formatter = logging.Formatter('%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def info(self, message: str):
        """Log info message"""
        if self.verbose:
            print(message)
    
    def debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message"""
        self.logger.error(message)

import logging

class ColorFormatter(logging.Formatter):
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[0;37m',   # White
        'INFO': '\033[94m',      # Blue
        'WARNING': '\033[93m',   # Yellow
        'ERROR': '\033[38;5;208m',  # Orange (using 256-color code)
        'CRITICAL': '\033[91m',  # Red
        'RESET': '\033[0m'       # Reset color
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        message = super().format(record)
        return f"{color}{message}{self.COLORS['RESET']}"

def setup_logger(name:str, level: int, handler:logging.Handler, propagate:bool) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = propagate
    return logger
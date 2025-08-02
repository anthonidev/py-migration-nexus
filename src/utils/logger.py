import logging
import sys
from typing import Optional

class ColoredFormatter(logging.Formatter):
    
    # Códigos de color ANSI
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Verde
        'WARNING': '\033[33m',    # Amarillo
        'ERROR': '\033[31m',      # Rojo
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    # Emojis para cada nivel
    EMOJIS = {
        'DEBUG': '🐛',
        'INFO': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '💥'
    }

    def __init__(self, use_colors: bool = True, use_emojis: bool = True):
        self.use_colors = use_colors and self._supports_color()
        self.use_emojis = use_emojis
        
        # Formato base
        if self.use_emojis:
            fmt = '%(asctime)s - %(emoji)s %(colored_levelname)s - %(name)s - %(message)s'
        else:
            fmt = '%(asctime)s - %(colored_levelname)s - %(name)s - %(message)s'
            
        super().__init__(fmt, datefmt='%Y-%m-%d %H:%M:%S')

    def _supports_color(self) -> bool:
        try:
            if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
                if sys.platform == 'win32':
                    try:
                        import os
                        os.system('color')
                        return True
                    except:
                        return False
                return True
            return False
        except:
            return False

    def format(self, record):
        record = logging.makeLogRecord(record.__dict__)
        
        level_name = record.levelname
        
        if self.use_emojis:
            record.emoji = self.EMOJIS.get(level_name, '')
        
        if self.use_colors:
            color = self.COLORS.get(level_name, '')
            reset = self.COLORS['RESET']
            record.colored_levelname = f"{color}{level_name:<8}{reset}"
        else:
            record.colored_levelname = f"{level_name:<8}"
        
        return super().format(record)


class ColoredLogger:
    
    _loggers = {} 
    
    @classmethod
    def get_logger(cls, name: Optional[str] = None, 
                   level: int = logging.INFO,
                   use_colors: bool = True,
                   use_emojis: bool = True,
                   log_file: Optional[str] = None) -> logging.Logger:
       
        if name is None:
            import inspect
            frame = inspect.currentframe()
            if frame and frame.f_back:
                name = frame.f_back.f_globals.get('__name__', 'unknown')
            else:
                name = 'unknown'
        
        cache_key = f"{name}_{level}_{use_colors}_{use_emojis}_{log_file}"
        if cache_key in cls._loggers:
            return cls._loggers[cache_key]
        
        logger = logging.getLogger(name)
        
        if logger.handlers:
            return logger
        
        logger.setLevel(level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = ColoredFormatter(use_colors=use_colors, use_emojis=use_emojis)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(level)
        logger.addHandler(console_handler)
        
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_formatter = ColoredFormatter(use_colors=False, use_emojis=False)
                file_handler.setFormatter(file_formatter)
                file_handler.setLevel(level)
                logger.addHandler(file_handler)
            except Exception as e:
                logger.warning(f"No se pudo crear el archivo de log {log_file}: {e}")
        
        logger.propagate = False
        
        cls._loggers[cache_key] = logger
        
        return logger


def get_logger(name: Optional[str] = None, 
               level: int = logging.INFO,
               use_colors: bool = True,
               use_emojis: bool = True,
               log_file: Optional[str] = None) -> logging.Logger:
    
    return ColoredLogger.get_logger(
        name=name,
        level=level,
        use_colors=use_colors,
        use_emojis=use_emojis,
        log_file=log_file
    )

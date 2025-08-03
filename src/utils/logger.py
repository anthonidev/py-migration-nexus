
import logging
import sys
from typing import Optional
from rich.console import Console
from rich.logging import RichHandler

console = Console()

class SimpleLogger:
    
    _loggers = {}  
    
    @classmethod
    def get_logger(cls, name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
        
        if name is None:
            import inspect
            frame = inspect.currentframe()
            if frame and frame.f_back:
                name = frame.f_back.f_globals.get('__name__', 'migration')
            else:
                name = 'migration'
        
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        
        if logger.handlers:
            return logger
        
        logger.setLevel(level)
        
        rich_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,  
            markup=True,    
            rich_tracebacks=True 
        )
        
        rich_handler.setFormatter(
            logging.Formatter(
                fmt="%(message)s",
                datefmt="[%X]"
            )
        )
        
        logger.addHandler(rich_handler)
        logger.propagate = False  
        
        cls._loggers[name] = logger
        
        return logger

def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
  
    return SimpleLogger.get_logger(name, level)

def info(message: str):
    logger = get_logger()
    logger.info(message)

def failure(message: str):
    logger = get_logger()
    logger.error(message)

def warning(message: str):
    logger = get_logger()
    logger.warning(message)

def debug(message: str):
    logger = get_logger()
    logger.debug(message)

def success(message: str):
    logger = get_logger()
    logger.info(f"\n[bold green]✅ {message}[/bold green]")

def failure(message: str):
    logger = get_logger()
    logger.error(f"[bold red]❌ {message}[/bold red]")

def progress(message: str):
    logger = get_logger()
    logger.info(f"[bold blue]🚀 {message}[/bold blue]")
    
def title(message: str):
    logger = get_logger()
    logger.info(f"\n[bold magenta]{message}[/bold magenta]\n")
    
def subtitle(message: str):
    logger = get_logger()
    logger.info(f"\n[bold cyan]{message}[/bold cyan]")
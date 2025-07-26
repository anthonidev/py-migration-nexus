import logging
from typing import Optional

def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Configura y retorna un logger consistente para el proyecto
    
    Args:
        name: Nombre del logger (si es None, usa el nombre del módulo llamador)
        level: Nivel de logging
    
    Returns:
        Logger configurado
    """
    if name is None:
        name = __name__
    
    logger = logging.getLogger(name)
    
    # Evitar duplicar handlers si ya está configurado
    if logger.handlers:
        return logger
    
    # Configurar formato
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.setLevel(level)
    
    return logger
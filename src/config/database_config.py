import os
from typing import Optional

class DatabaseConfig:
    """Configuración centralizada para las conexiones de base de datos"""
    
    @staticmethod
    def get_postgres_url() -> str:
        """Obtiene la URL de conexión a PostgreSQL"""
        url = os.getenv('NEXUS_POSTGRES_URL')
        if not url:
            raise ValueError("Variable de entorno NEXUS_POSTGRES_URL no encontrada")
        return url
    
    @staticmethod
    def get_mongo_url() -> str:
        """Obtiene la URL de conexión a MongoDB"""
        url = os.getenv('MS_NEXUS_USER')
        if not url:
            raise ValueError("Variable de entorno MS_NEXUS_USER no encontrada")
        return url
    
    @staticmethod
    def get_mongo_database_name() -> Optional[str]:
        """Extrae el nombre de la base de datos de la URL de MongoDB"""
        mongo_url = DatabaseConfig.get_mongo_url()
        # Extraer nombre de DB de la URL si está presente
        if '/' in mongo_url and not mongo_url.endswith('/'):
            return mongo_url.split('/')[-1]
        return None
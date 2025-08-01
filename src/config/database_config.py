import os
from typing import Optional


class DatabaseConfig:
    @staticmethod
    def get_postgres_url() -> str:
        url = os.getenv('NEXUS_POSTGRES_URL')
        if not url:
            raise ValueError(
                "Variable de entorno NEXUS_POSTGRES_URL no encontrada")
        return url

    @staticmethod
    def get_mongo_url() -> str:
        url = os.getenv('MS_NEXUS_USER')
        if not url:
            raise ValueError("Variable de entorno MS_NEXUS_USER no encontrada")
        return url

    @staticmethod
    def get_mongo_database_name() -> Optional[str]:
        mongo_url = DatabaseConfig.get_mongo_url()
        if '/' in mongo_url and not mongo_url.endswith('/'):
            return mongo_url.split('/')[-1]
        return None

    @staticmethod
    def get_payments_postgres_url() -> str:
        url = os.getenv('MS_NEXUS_PAYMENTS')
        if not url:
            raise ValueError(
                "Variable de entorno MS_NEXUS_PAYMENTS no encontrada")
        return url

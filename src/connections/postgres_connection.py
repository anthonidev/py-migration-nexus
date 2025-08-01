import psycopg2
from contextlib import contextmanager
from typing import Generator
from src.config.database_config import DatabaseConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PostgresConnection:

    def __init__(self):
        self.connection = None

    def connect(self) -> psycopg2.extensions.connection:
        try:
            self.connection = psycopg2.connect(
                DatabaseConfig.get_postgres_url())
            logger.info("Conexión a PostgreSQL establecida exitosamente")
            return self.connection
        except Exception as e:
            logger.error(f"Error conectando a PostgreSQL: {str(e)}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Conexión a PostgreSQL cerrada")

    @contextmanager
    def get_cursor(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        try:
            yield cursor
        except Exception as e:
            logger.error(f"Error en operación de base de datos: {str(e)}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def execute_query(self, query: str, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall(), [desc[0] for desc in cursor.description]

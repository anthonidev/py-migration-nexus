from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class UserRanksExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_user_ranks(self) -> List[Dict[str, Any]]:
        """Extrae todos los user_ranks desde el monolito con relaciones necesarias"""
        logger.info("Extrayendo user_ranks desde PostgreSQL (monolito)")

        query = """
        SELECT
            ur.id,
            u.email AS user_email,
            cr.code AS current_rank_code,
            hr.code AS highest_rank_code,
            ur.metadata,
            ur."createdAt" AS created_at,
            ur."updatedAt" AS updated_at
        FROM
            user_ranks ur
            INNER JOIN users u ON ur.user_id = u.id
            INNER JOIN ranks cr ON ur.current_rank_id = cr.id
            LEFT JOIN ranks hr ON ur.highest_rank_id = hr.id
        ORDER BY
            ur."createdAt" DESC;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)
            data: List[Dict[str, Any]] = []
            for row in results:
                record = dict(zip(columns, row))
                # Normalizar email a lower
                if record.get('user_email'):
                    record['user_email'] = record['user_email'].lower().strip()
                data.append(record)

            logger.info(f"Extraídos {len(data)} user_ranks")
            return data

        except Exception as e:
            logger.error(f"Error extrayendo user_ranks: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida integridad básica en origen"""
        logger.info("Validando datos de origen para user_ranks")
        result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Orígenes huérfanos (por seguridad; la consulta principal ya hace INNER JOIN requisitando usuario y current_rank)
            orphan_hranks_query = """
            SELECT COUNT(*)
            FROM user_ranks ur
            LEFT JOIN users u ON ur.user_id = u.id
            WHERE u.id IS NULL
            """
            orphan_users, _ = self.postgres_conn.execute_query(orphan_hranks_query)
            if orphan_users[0][0] > 0:
                result['errors'].append(f"{orphan_users[0][0]} user_ranks sin usuario válido")

            orphan_current_rank_query = """
            SELECT COUNT(*)
            FROM user_ranks ur
            LEFT JOIN ranks r ON ur.current_rank_id = r.id
            WHERE r.id IS NULL
            """
            orphan_cr, _ = self.postgres_conn.execute_query(orphan_current_rank_query)
            if orphan_cr[0][0] > 0:
                result['errors'].append(f"{orphan_cr[0][0]} user_ranks sin current_rank válido")

            # highest_rank_id puede ser nulo, pero si existe debe referenciar a rank existente
            orphan_highest_rank_query = """
            SELECT COUNT(*)
            FROM user_ranks ur
            LEFT JOIN ranks r ON ur.highest_rank_id = r.id
            WHERE ur.highest_rank_id IS NOT NULL AND r.id IS NULL
            """
            orphan_hr, _ = self.postgres_conn.execute_query(orphan_highest_rank_query)
            if orphan_hr[0][0] > 0:
                result['warnings'].append(f"{orphan_hr[0][0]} user_ranks con highest_rank_id inválido (se migrarán con NULL)")

            if result['errors']:
                result['valid'] = False

            return result

        except Exception as e:
            logger.error(f"Error validando datos de origen: {str(e)}")
            result['valid'] = False
            result['errors'].append(str(e))
            return result

    def close_connection(self):
        self.postgres_conn.disconnect()

from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MonthlyVolumeRanksExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_monthly_volume_ranks(self) -> List[Dict[str, Any]]:
        """Extrae todos los volúmenes mensuales con los datos necesarios desde el monolito"""
        logger.info("Extrayendo monthly_volume_ranks desde PostgreSQL (monolito)")

        query = """
        SELECT
            mvr.id,
            u.email as user_email,
            mvr."totalVolume",
            mvr."leftVolume",
            mvr."rightVolume",
            mvr."leftDirects",
            mvr."rightDirects",
            mvr."monthStartDate",
            mvr."monthEndDate",
            mvr.status,
            ar.code as assigned_rank_code,
            mvr.metadata,
            mvr."createdAt",
            mvr."updatedAt"
        FROM
            monthly_volume_ranks mvr
                INNER JOIN users u ON mvr.user_id = u.id
                LEFT JOIN ranks ar ON mvr.assigned_rank_id = ar.id
        ORDER BY
            mvr."monthStartDate" DESC, mvr."createdAt" DESC;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)
            data: List[Dict[str, Any]] = []
            for row in results:
                record = dict(zip(columns, row))
                if record.get('user_email'):
                    record['user_email'] = record['user_email'].lower().strip()
                data.append(record)

            logger.info(f"Extraídos {len(data)} monthly_volume_ranks")
            return data

        except Exception as e:
            logger.error(f"Error extrayendo monthly_volume_ranks: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida integridad básica en origen para monthly_volume_ranks"""
        logger.info("Validando datos de origen para monthly_volume_ranks")
        result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Volúmenes negativos
            negative_q = """
            SELECT COUNT(*)
            FROM monthly_volume_ranks
            WHERE "leftVolume" < 0 OR "rightVolume" < 0 OR "totalVolume" < 0
            """
            neg, _ = self.postgres_conn.execute_query(negative_q)
            if neg[0][0] > 0:
                result['errors'].append(f"{neg[0][0]} registros con volúmenes negativos")

            # Fechas inválidas o inconsistentes
            invalid_dates_q = """
            SELECT COUNT(*)
            FROM monthly_volume_ranks
            WHERE "monthStartDate" IS NULL OR "monthEndDate" IS NULL OR "monthEndDate" <= "monthStartDate"
            """
            inv, _ = self.postgres_conn.execute_query(invalid_dates_q)
            if inv[0][0] > 0:
                result['errors'].append(f"{inv[0][0]} registros con fechas de mes inválidas")

            # Duplicados por usuario y mes
            dup_q = """
            SELECT COUNT(*) FROM (
              SELECT user_id, "monthStartDate", COUNT(*) c
              FROM monthly_volume_ranks
              GROUP BY user_id, "monthStartDate"
              HAVING COUNT(*) > 1
            ) t
            """
            dup, _ = self.postgres_conn.execute_query(dup_q)
            if dup[0][0] > 0:
                result['warnings'].append(f"{dup[0][0]} usuarios con duplicados por mes (se migrarán tal cual)")

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

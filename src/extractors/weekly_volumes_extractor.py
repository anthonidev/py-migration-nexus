from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class WeeklyVolumesExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_weekly_volumes_data(self) -> List[Dict[str, Any]]:
        """Extrae todos los volúmenes semanales con su historial desde PostgreSQL"""
        logger.info("Iniciando extracción de volúmenes semanales desde PostgreSQL")

        query = """
        SELECT
            wv.id,
            u.email AS "userEmail",
            wv."leftVolume",
            wv."rightVolume",
            wv."paidAmount" AS "commissionEarned",
            wv."weekStartDate",
            wv."weekEndDate",
            wv.status,
            wv."selectedSide",
            wv."createdAt" AS "processedAt",
            wv."createdAt",
            wv.metadata,
            COALESCE(
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'id', wvh.id,
                        'volumeSide', wvh."selectedSide",
                        'volume', wvh.volume,
                        'createdAt', wvh."createdAt",
                        'updatedAt', wvh."updatedAt",
                        'payment_id', wvh.payment_id
                    ) ORDER BY wvh."createdAt"
                ) FILTER (WHERE wvh.id IS NOT NULL),
                '[]'::json
            ) AS history
        FROM
            weekly_volumes wv
            INNER JOIN users u ON wv.user_id = u.id
            LEFT JOIN weekly_volumes_history wvh ON wv.id = wvh.weekly_volume_id
        GROUP BY
            wv.id,
            u.email,
            wv."leftVolume",
            wv."rightVolume",
            wv."paidAmount",
            wv."weekStartDate",
            wv."weekEndDate",
            wv.status,
            wv."selectedSide",
            wv.metadata,
            wv."createdAt"
        ORDER BY
            wv."createdAt" DESC;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            weekly_volumes_data = []
            for row in results:
                weekly_volume_dict = dict(zip(columns, row))
                weekly_volumes_data.append(weekly_volume_dict)

            logger.info(f"Extraídos {len(weekly_volumes_data)} volúmenes semanales desde PostgreSQL")
            return weekly_volumes_data

        except Exception as e:
            logger.error(f"Error extrayendo volúmenes semanales: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen para volúmenes semanales")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar volúmenes semanales sin usuario válido
            orphan_volumes_query = """
            SELECT COUNT(*) 
            FROM weekly_volumes wv 
            LEFT JOIN users u ON wv.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_results, _ = self.postgres_conn.execute_query(orphan_volumes_query)
            orphan_volumes = orphan_results[0][0]

            if orphan_volumes > 0:
                validation_results['errors'].append(f"{orphan_volumes} volúmenes semanales sin usuario válido")
                validation_results['valid'] = False

            # Validar volúmenes negativos
            negative_volumes_query = """
            SELECT COUNT(*) 
            FROM weekly_volumes 
            WHERE "leftVolume" < 0 OR "rightVolume" < 0
            """
            negative_results, _ = self.postgres_conn.execute_query(negative_volumes_query)
            negative_volumes = negative_results[0][0]

            if negative_volumes > 0:
                validation_results['errors'].append(f"{negative_volumes} volúmenes con valores negativos")
                validation_results['valid'] = False

            # Validar fechas inconsistentes
            invalid_dates_query = """
            SELECT COUNT(*) 
            FROM weekly_volumes 
            WHERE "weekStartDate" IS NULL 
               OR "weekEndDate" IS NULL
               OR "weekEndDate" <= "weekStartDate"
            """
            invalid_dates_results, _ = self.postgres_conn.execute_query(invalid_dates_query)
            invalid_dates = invalid_dates_results[0][0]

            if invalid_dates > 0:
                validation_results['errors'].append(f"{invalid_dates} volúmenes con fechas inválidas")
                validation_results['valid'] = False

            # Validar duplicados por usuario y semana
            duplicates_query = """
            SELECT COUNT(*) 
            FROM (
                SELECT user_id, "weekStartDate", COUNT(*) as count
                FROM weekly_volumes 
                GROUP BY user_id, "weekStartDate" 
                HAVING COUNT(*) > 1
            ) duplicates
            """
            duplicates_results, _ = self.postgres_conn.execute_query(duplicates_query)
            duplicates = duplicates_results[0][0]

            if duplicates > 0:
                validation_results['warnings'].append(f"{duplicates} usuarios con volúmenes duplicados por semana")

            # Validar history huérfano
            orphan_history_query = """
            SELECT COUNT(*) 
            FROM weekly_volumes_history wvh 
            LEFT JOIN weekly_volumes wv ON wvh.weekly_volume_id = wv.id 
            WHERE wv.id IS NULL
            """
            orphan_history_results, _ = self.postgres_conn.execute_query(orphan_history_query)
            orphan_history = orphan_history_results[0][0]

            if orphan_history > 0:
                validation_results['warnings'].append(f"{orphan_history} registros de history huérfanos (se omitirán)")

            # Validar volúmenes negativos en history
            negative_history_query = """
            SELECT COUNT(*) 
            FROM weekly_volumes_history 
            WHERE volume < 0
            """
            negative_history_results, _ = self.postgres_conn.execute_query(negative_history_query)
            negative_history = negative_history_results[0][0]

            if negative_history > 0:
                validation_results['errors'].append(f"{negative_history} registros de history con volumen negativo")
                validation_results['valid'] = False

            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()
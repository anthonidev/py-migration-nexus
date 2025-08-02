from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UserPointsExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_user_points_data(self) -> List[Dict[str, Any]]:
        """Extrae todos los puntos de usuarios con sus transacciones desde PostgreSQL"""
        logger.info("Iniciando extracción de puntos de usuarios desde PostgreSQL")

        query = """
        SELECT
            up.id,
            up."availablePoints",
            up."totalEarnedPoints",
            up."totalWithdrawnPoints",
            u.email AS "userEmail",
            COALESCE(
                JSON_AGG(
                    CASE
                        WHEN pt.id IS NOT NULL THEN
                            JSON_BUILD_OBJECT(
                                'id', pt.id,
                                'amount', pt.amount,
                                'status', pt.status,
                                'metadata', pt.metadata,
                                'pendingAmount', pt."pendingAmount",
                                'withdrawnAmount', pt."withdrawnAmount",
                                'isArchived', pt."isArchived",
                                'type', pt.type,
                                'createdAt', pt."createdAt",
                                'updatedAt', pt."createdAt",
                                'payments', COALESCE(payments_agg.payments, '[]'::json)
                            )
                        ELSE NULL
                        END
                ) FILTER (WHERE pt.id IS NOT NULL),
                '[]'::json
            ) AS transactions
        FROM
            public.user_points up
            INNER JOIN public.users u ON up.user_id = u.id
            LEFT JOIN public.points_transactions pt ON up.user_id = pt.user_id
            LEFT JOIN (
                SELECT
                    ptp.points_transaction_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'points_transaction_id', ptp.points_transaction_id,
                            'payment_id', ptp.payment_id,
                            'createdAt', ptp."createdAt",
                            'updatedAt', ptp."updatedAt"
                        )
                    ) AS payments
                FROM
                    public.points_transactions_payments ptp
                GROUP BY
                    ptp.points_transaction_id
            ) payments_agg ON pt.id = payments_agg.points_transaction_id
        GROUP BY
            up.id,
            up."availablePoints",
            up."totalEarnedPoints",
            up."totalWithdrawnPoints",
            u.email
        ORDER BY
            up.id;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            user_points_data = []
            for row in results:
                user_point_dict = dict(zip(columns, row))
                user_points_data.append(user_point_dict)

            logger.info(f"Extraídos {len(user_points_data)} registros de puntos de usuarios desde PostgreSQL")
            return user_points_data

        except Exception as e:
            logger.error(f"Error extrayendo puntos de usuarios: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen para puntos de usuarios")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar puntos de usuarios sin usuario válido
            orphan_user_points_query = """
            SELECT COUNT(*) 
            FROM user_points up 
            LEFT JOIN users u ON up.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_results, _ = self.postgres_conn.execute_query(orphan_user_points_query)
            orphan_user_points = orphan_results[0][0]

            if orphan_user_points > 0:
                validation_results['errors'].append(f"{orphan_user_points} registros de puntos sin usuario válido")
                validation_results['valid'] = False

            # Validar puntos negativos
            negative_points_query = """
            SELECT COUNT(*) 
            FROM user_points 
            WHERE "availablePoints" < 0 
               OR "totalEarnedPoints" < 0 
               OR "totalWithdrawnPoints" < 0
            """
            negative_results, _ = self.postgres_conn.execute_query(negative_points_query)
            negative_points = negative_results[0][0]

            if negative_points > 0:
                validation_results['errors'].append(f"{negative_points} registros con puntos negativos")
                validation_results['valid'] = False

            # Validar consistencia de puntos
            inconsistent_points_query = """
            SELECT COUNT(*) 
            FROM user_points 
            WHERE "totalWithdrawnPoints" > "totalEarnedPoints"
            """
            inconsistent_results, _ = self.postgres_conn.execute_query(inconsistent_points_query)
            inconsistent_points = inconsistent_results[0][0]

            if inconsistent_points > 0:
                validation_results['errors'].append(f"{inconsistent_points} registros con puntos retirados mayores a ganados")
                validation_results['valid'] = False

            # Validar transacciones huérfanas
            orphan_transactions_query = """
            SELECT COUNT(*) 
            FROM points_transactions pt 
            LEFT JOIN users u ON pt.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_trans_results, _ = self.postgres_conn.execute_query(orphan_transactions_query)
            orphan_transactions = orphan_trans_results[0][0]

            if orphan_transactions > 0:
                validation_results['warnings'].append(f"{orphan_transactions} transacciones huérfanas (se omitirán)")

            # Validar transacciones con montos negativos
            negative_transactions_query = """
            SELECT COUNT(*) 
            FROM points_transactions 
            WHERE amount < 0 
               OR "pendingAmount" < 0 
               OR "withdrawnAmount" < 0
            """
            negative_trans_results, _ = self.postgres_conn.execute_query(negative_transactions_query)
            negative_transactions = negative_trans_results[0][0]

            if negative_transactions > 0:
                validation_results['errors'].append(f"{negative_transactions} transacciones con montos negativos")
                validation_results['valid'] = False

            # Validar pagos huérfanos
            orphan_payments_query = """
            SELECT COUNT(*) 
            FROM points_transactions_payments ptp 
            LEFT JOIN points_transactions pt ON ptp.points_transaction_id = pt.id 
            WHERE pt.id IS NULL
            """
            orphan_payments_results, _ = self.postgres_conn.execute_query(orphan_payments_query)
            orphan_payments = orphan_payments_results[0][0]

            if orphan_payments > 0:
                validation_results['warnings'].append(f"{orphan_payments} pagos de transacciones huérfanos (se omitirán)")

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
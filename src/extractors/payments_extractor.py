from src.utils.logger import get_logger
from src.connections.postgres_connection import PostgresConnection
from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

logger = get_logger(__name__)

class PaymentsExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_payments_data(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando extracción de pagos desde PostgreSQL")

        query = """
        SELECT
            p.id,
            u.email as "userEmail",
            p.payment_config_id as "paymentConfigId",
            p.amount,
            p.status,
            p."methodPayment" as "paymentMethod",
            p."codeOperation" as "operationCode",
            p."numberTicket" as "ticketNumber",
            p."rejectionReason",
            COALESCE(payment_images.items, '[]'::json) as "items",
            p.reviewed_by_id as "reviewedById",
            reviewer.email as "reviewedByEmail",
            p."reviewedAt",
            p."isArchived",
            p."relatedEntityType",
            p."relatedEntityId",
            p.metadata,
            p."createdAt",
            p."updatedAt"
        FROM
            payments p
                INNER JOIN users u ON p.user_id = u.id
                LEFT JOIN users reviewer ON p.reviewed_by_id = reviewer.id
                LEFT JOIN (
                SELECT
                    payment_id,
                    JSON_AGG(
                            JSON_BUILD_OBJECT(
                                    'id', id,
                                    'url', url,
                                    'cloudinaryPublicId', "cloudinaryPublicId",
                                    'amount', amount,
                                    'bankName', "bankName",
                                    'transactionReference', "transactionReference",
                                    'transactionDate', "transactionDate",
                                    'isActive', "isActive",
                                    'createdAt', "createdAt",
                                    'updatedAt', "updatedAt"
                            )
                    ) as items
                FROM payment_images
                WHERE "isActive" = true
                GROUP BY payment_id
            ) payment_images ON p.id = payment_images.payment_id
        ORDER BY
            p."createdAt" DESC;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            payments_data = []
            for row in results:
                payment_dict = dict(zip(columns, row))
                payments_data.append(payment_dict)

            logger.info(f"Extraídos {len(payments_data)} pagos desde PostgreSQL")
            return payments_data

        except Exception as e:
            logger.error(f"Error extrayendo pagos: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        logger.info("Validando datos de origen para pagos")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar pagos sin usuario
            orphan_payments_query = """
            SELECT COUNT(*) 
            FROM payments p 
            LEFT JOIN users u ON p.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_results, _ = self.postgres_conn.execute_query(orphan_payments_query)
            orphan_payments = orphan_results[0][0]

            if orphan_payments > 0:
                validation_results['errors'].append(f"{orphan_payments} pagos sin usuario válido")
                validation_results['valid'] = False

            # Validar pagos sin configuración válida
            invalid_config_query = """
            SELECT COUNT(*) 
            FROM payments p 
            LEFT JOIN payment_configs pc ON p.payment_config_id = pc.id 
            WHERE pc.id IS NULL
            """
            invalid_config_results, _ = self.postgres_conn.execute_query(invalid_config_query)
            invalid_configs = invalid_config_results[0][0]

            if invalid_configs > 0:
                validation_results['errors'].append(f"{invalid_configs} pagos con configuración inválida")
                validation_results['valid'] = False

            # Validar montos válidos
            invalid_amounts_query = """
            SELECT COUNT(*) 
            FROM payments 
            WHERE amount <= 0 OR amount IS NULL
            """
            invalid_amounts_results, _ = self.postgres_conn.execute_query(invalid_amounts_query)
            invalid_amounts = invalid_amounts_results[0][0]

            if invalid_amounts > 0:
                validation_results['errors'].append(f"{invalid_amounts} pagos con montos inválidos")
                validation_results['valid'] = False

            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'CON ERRORES'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        self.postgres_conn.disconnect()
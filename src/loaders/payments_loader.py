from typing import List, Dict, Any
from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentsLoader:

    def __init__(self):
        self.postgres_conn = PaymentsPostgresConnection()
        self.stats = {
            'payments_inserted': 0,
            'payment_items_inserted': 0,
            'payments_deleted': 0,
            'payment_items_deleted': 0,
            'errors': []
        }

    def _check_tables_exist(self):
        check_payments = """SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'payments');"""
        result, _ = self.postgres_conn.execute_query(check_payments)
        if not result[0][0]:
            raise RuntimeError("Tabla 'payments' no existe. Debe ser creada por las migraciones del microservicio.")

        check_items = """SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'payment_items');"""
        result, _ = self.postgres_conn.execute_query(check_items)
        if not result[0][0]:
            raise RuntimeError("Tabla 'payment_items' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> Dict[str, int]:
        logger.info("Eliminando datos de pagos existentes")

        try:
            delete_items_query = "DELETE FROM payment_items"
            items_deleted, _ = self.postgres_conn.execute_query(delete_items_query)

            delete_payments_query = "DELETE FROM payments"
            payments_deleted, _ = self.postgres_conn.execute_query(delete_payments_query)

            self.postgres_conn.execute_query("SELECT setval('payments_id_seq', COALESCE((SELECT MAX(id) FROM payments), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('payment_items_id_seq', COALESCE((SELECT MAX(id) FROM payment_items), 0) + 1, false);")

            self.stats['payments_deleted'] = payments_deleted
            self.stats['payment_items_deleted'] = items_deleted

            logger.info(f"Eliminados {payments_deleted} pagos y {items_deleted} items")

            return {
                'payments_deleted': payments_deleted,
                'payment_items_deleted': items_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_payments(self, payments_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Iniciando carga de {len(payments_data)} pagos en PostgreSQL")

        try:
            self._check_tables_exist()
            if clear_existing:
                self.clear_existing_data()

            if not payments_data:
                logger.warning("No hay pagos para insertar")
                return {'success': True, 'inserted_count': 0, 'deleted_count': self.stats['payments_deleted']}

            inserted_count = self._insert_payments_with_original_ids(payments_data)
            self.stats['payments_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} pagos exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['payments_deleted']
            }

        except Exception as e:
            error_msg = f"Error cargando pagos: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'deleted_count': self.stats['payments_deleted'], 'error': str(e)}

    def load_payment_items(self, payment_items_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Iniciando carga de {len(payment_items_data)} items de pago en PostgreSQL")

        try:
            if not payment_items_data:
                logger.warning("No hay items de pago para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_payment_items(payment_items_data)
            self.stats['payment_items_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} items de pago exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando items de pago: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def _insert_payments_with_original_ids(self, payments_data: List[Dict[str, Any]]) -> int:

        insert_query = """
        INSERT INTO payments (
            id, user_id, user_email, user_name, payment_config_id, amount,
            status, payment_method, operation_code, bank_name, operation_date,
            ticket_number, rejection_reason, reviewed_by_id, reviewed_by_email,
            reviewed_at, is_archived, related_entity_type, related_entity_id,
            metadata, external_reference, gateway_transaction_id, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for payment in payments_data:
            metadata_value = payment['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                payment['id'], payment['user_id'], payment['user_email'], payment['user_name'],
                payment['payment_config_id'], payment['amount'], payment['status'], payment['payment_method'],
                payment['operation_code'], payment['bank_name'], payment['operation_date'], payment['ticket_number'],
                payment['rejection_reason'], payment['reviewed_by_id'], payment['reviewed_by_email'], payment['reviewed_at'],
                payment['is_archived'], payment['related_entity_type'], payment['related_entity_id'], metadata_value,
                payment['external_reference'], payment['gateway_transaction_id'], payment['created_at'], payment['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            max_id = max(payment['id'] for payment in payments_data)
            self.postgres_conn.execute_query("SELECT setval('payments_id_seq', %s, true);", (max_id,))
            logger.info(f"Secuencia de pagos actualizada para continuar desde ID {max_id}")
            
            return len(payments_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de pagos: {str(e)}")
            raise

    def _insert_payment_items(self, payment_items_data: List[Dict[str, Any]]) -> int:

        insert_query = """
        INSERT INTO payment_items (
            id, payment_id, item_type, url, url_key, points_transaction_id, 
            amount, bank_name, transaction_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for item in payment_items_data:
            params = (
                item['id'], item['payment_id'], item['item_type'], item['url'],
                item['url_key'], item['points_transaction_id'], item['amount'],
                item['bank_name'], item['transaction_date']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            if payment_items_data:
                max_id = max(item['id'] for item in payment_items_data)
                self.postgres_conn.execute_query("SELECT setval('payment_items_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de payment_items actualizada para continuar desde ID {max_id}")

            return len(payment_items_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de items: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        logger.info("Validando integridad de datos de pagos en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            payments_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM payments")
            items_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM payment_items")
            total_payments = payments_count[0][0]
            total_items = items_count[0][0]

            status_results, _ = self.postgres_conn.execute_query("SELECT status, COUNT(*) FROM payments GROUP BY status")
            payments_by_status = {row[0]: row[1] for row in status_results}

            method_results, _ = self.postgres_conn.execute_query("SELECT payment_method, COUNT(*) FROM payments GROUP BY payment_method")
            payments_by_method = {row[0]: row[1] for row in method_results}

            validation_results['stats'] = {
                'total_payments': total_payments,
                'total_payment_items': total_items,
                'payments_by_status': payments_by_status,
                'payments_by_method': payments_by_method
            }

            missing_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM payments 
                WHERE user_email IS NULL OR user_email = '' 
                   OR payment_config_id IS NULL 
                   OR amount IS NULL OR amount <= 0
            """)
            
            if missing_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_data[0][0]} pagos con campos obligatorios inválidos")
                validation_results['valid'] = False

            orphan_items, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM payment_items pi 
                LEFT JOIN payments p ON pi.payment_id = p.id 
                WHERE p.id IS NULL
            """)
            
            if orphan_items[0][0] > 0:
                validation_results['errors'].append(f"{orphan_items[0][0]} items huérfanos sin pago asociado")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        return {
            'payments_inserted': self.stats['payments_inserted'],
            'payment_items_inserted': self.stats['payment_items_inserted'],
            'payments_deleted': self.stats['payments_deleted'],
            'payment_items_deleted': self.stats['payment_items_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        self.postgres_conn.disconnect()
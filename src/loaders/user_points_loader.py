from typing import List, Dict, Any
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UserPointsLoader:

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()
        self.stats = {
            'user_points_inserted': 0,
            'transactions_inserted': 0,
            'transaction_payments_inserted': 0,
            'user_points_deleted': 0,
            'transactions_deleted': 0,
            'transaction_payments_deleted': 0,
            'errors': []
        }

    def _check_tables_exist(self):
        """Verifica que las tablas necesarias existan"""
        tables_to_check = ['user_points', 'points_transactions', 'points_transaction_payments']
        
        for table in tables_to_check:
            check_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table}'
            );
            """
            result, _ = self.postgres_conn.execute_query(check_query)
            if not result[0][0]:
                raise RuntimeError(f"Tabla '{table}' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> Dict[str, int]:
        """Elimina todos los datos existentes en las tablas de puntos"""
        logger.info("Eliminando datos de puntos existentes")

        try:
            # Eliminar en orden por dependencias FK
            delete_payments_query = "DELETE FROM points_transaction_payments"
            payments_deleted, _ = self.postgres_conn.execute_query(delete_payments_query)

            delete_transactions_query = "DELETE FROM points_transactions"
            transactions_deleted, _ = self.postgres_conn.execute_query(delete_transactions_query)

            delete_user_points_query = "DELETE FROM user_points"
            user_points_deleted, _ = self.postgres_conn.execute_query(delete_user_points_query)

            # Resetear secuencias
            self.postgres_conn.execute_query("SELECT setval('user_points_id_seq', COALESCE((SELECT MAX(id) FROM user_points), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('points_transactions_id_seq', COALESCE((SELECT MAX(id) FROM points_transactions), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('points_transaction_payments_id_seq', COALESCE((SELECT MAX(id) FROM points_transaction_payments), 0) + 1, false);")

            self.stats['user_points_deleted'] = user_points_deleted
            self.stats['transactions_deleted'] = transactions_deleted
            self.stats['transaction_payments_deleted'] = payments_deleted

            logger.info(f"Eliminados {user_points_deleted} user_points, {transactions_deleted} transactions, {payments_deleted} transaction_payments")

            return {
                'user_points_deleted': user_points_deleted,
                'transactions_deleted': transactions_deleted,
                'transaction_payments_deleted': payments_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_user_points(self, user_points_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """Carga los puntos de usuarios en la base de datos"""
        logger.info(f"Iniciando carga de {len(user_points_data)} user_points en PostgreSQL")

        try:
            self._check_tables_exist()
            
            if clear_existing:
                self.clear_existing_data()

            if not user_points_data:
                logger.warning("No hay user_points para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['user_points_deleted']
                }

            inserted_count = self._insert_user_points_with_original_ids(user_points_data)
            self.stats['user_points_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} user_points exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['user_points_deleted']
            }

        except Exception as e:
            error_msg = f"Error cargando user_points: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['user_points_deleted'],
                'error': str(e)
            }

    def load_transactions(self, transactions_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga las transacciones de puntos en la base de datos"""
        logger.info(f"Iniciando carga de {len(transactions_data)} transactions en PostgreSQL")

        try:
            if not transactions_data:
                logger.warning("No hay transactions para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_transactions_with_original_ids(transactions_data)
            self.stats['transactions_inserted'] = inserted_count
            logger.info(f"Insertadas {inserted_count} transactions exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando transactions: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def load_transaction_payments(self, transaction_payments_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga los pagos de transacciones en la base de datos"""
        logger.info(f"Iniciando carga de {len(transaction_payments_data)} transaction_payments en PostgreSQL")

        try:
            if not transaction_payments_data:
                logger.warning("No hay transaction_payments para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_transaction_payments_with_original_ids(transaction_payments_data)
            self.stats['transaction_payments_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} transaction_payments exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando transaction_payments: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def _insert_user_points_with_original_ids(self, user_points_data: List[Dict[str, Any]]) -> int:
        """Inserta user_points conservando los IDs originales"""
        insert_query = """
        INSERT INTO user_points (
            id, user_id, user_email, user_name, available_points, total_earned_points,
            total_withdrawn_points, available_lot_points, total_earned_lot_points,
            total_withdrawn_lot_points, metadata, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for user_point in user_points_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = user_point['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                user_point['id'],
                user_point['user_id'],
                user_point['user_email'],
                user_point['user_name'],
                user_point['available_points'],
                user_point['total_earned_points'],
                user_point['total_withdrawn_points'],
                user_point['available_lot_points'],
                user_point['total_earned_lot_points'],
                user_point['total_withdrawn_lot_points'],
                metadata_value,
                user_point['created_at'],
                user_point['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia para continuar desde el ID más alto
            max_id = max(user_point['id'] for user_point in user_points_data)
            self.postgres_conn.execute_query("SELECT setval('user_points_id_seq', %s, true);", (max_id,))
            logger.info(f"Secuencia de user_points actualizada para continuar desde ID {max_id}")
            
            return len(user_points_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de user_points: {str(e)}")
            raise

    def _insert_transactions_with_original_ids(self, transactions_data: List[Dict[str, Any]]) -> int:
        """Inserta transactions conservando los IDs originales"""
        insert_query = """
        INSERT INTO points_transactions (
            id, user_id, user_email, user_name, type, amount, pending_amount,
            withdrawn_amount, status, is_archived, metadata, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for transaction in transactions_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = transaction['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                transaction['id'],
                transaction['user_id'],
                transaction['user_email'],
                transaction['user_name'],
                transaction['type'],
                transaction['amount'],
                transaction['pending_amount'],
                transaction['withdrawn_amount'],
                transaction['status'],
                transaction['is_archived'],
                metadata_value,
                transaction['created_at'],
                transaction['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if transactions_data:
                max_id = max(transaction['id'] for transaction in transactions_data)
                self.postgres_conn.execute_query("SELECT setval('points_transactions_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de transactions actualizada para continuar desde ID {max_id}")

            return len(transactions_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de transactions: {str(e)}")
            raise

    def _insert_transaction_payments_with_original_ids(self, transaction_payments_data: List[Dict[str, Any]]) -> int:
        """Inserta transaction_payments conservando los IDs originales"""
        insert_query = """
        INSERT INTO points_transaction_payments (
            id, points_transaction_id, payment_id, amount, payment_reference,
            payment_method, notes, metadata, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for payment in transaction_payments_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = payment['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                payment['id'],
                payment['points_transaction_id'],
                payment['payment_id'],
                payment['amount'],
                payment['payment_reference'],
                payment['payment_method'],
                payment['notes'],
                metadata_value,
                payment['created_at'],
                payment['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if transaction_payments_data:
                max_id = max(payment['id'] for payment in transaction_payments_data)
                self.postgres_conn.execute_query("SELECT setval('points_transaction_payments_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de transaction_payments actualizada para continuar desde ID {max_id}")

            return len(transaction_payments_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de transaction_payments: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos de puntos en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            user_points_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM user_points")
            transactions_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM points_transactions")
            payments_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM points_transaction_payments")

            total_user_points = user_points_count[0][0]
            total_transactions = transactions_count[0][0]
            total_payments = payments_count[0][0]

            validation_results['stats'] = {
                'total_user_points': total_user_points,
                'total_transactions': total_transactions,
                'total_transaction_payments': total_payments
            }

            # Validar campos obligatorios en user_points
            missing_user_points_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM user_points 
                WHERE user_email IS NULL OR user_email = '' 
                   OR available_points < 0 
                   OR total_earned_points < 0 
                   OR total_withdrawn_points < 0
            """)
            
            if missing_user_points_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_user_points_data[0][0]} user_points con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar consistencia en user_points
            inconsistent_user_points, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM user_points 
                WHERE total_withdrawn_points > total_earned_points
            """)
            
            if inconsistent_user_points[0][0] > 0:
                validation_results['errors'].append(f"{inconsistent_user_points[0][0]} user_points con puntos retirados mayores a ganados")
                validation_results['valid'] = False

            # Validar transacciones huérfanas
            orphan_transactions, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM points_transactions pt 
                LEFT JOIN user_points up ON pt.user_id = up.user_id 
                WHERE up.user_id IS NULL
            """)
            
            if orphan_transactions[0][0] > 0:
                validation_results['errors'].append(f"{orphan_transactions[0][0]} transactions huérfanas sin user_points asociado")
                validation_results['valid'] = False

            # Validar transaction_payments huérfanos
            orphan_payments, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM points_transaction_payments ptp 
                LEFT JOIN points_transactions pt ON ptp.points_transaction_id = pt.id 
                WHERE pt.id IS NULL
            """)
            
            if orphan_payments[0][0] > 0:
                validation_results['errors'].append(f"{orphan_payments[0][0]} transaction_payments huérfanos sin transaction asociada")
                validation_results['valid'] = False

            # Validar montos negativos en transacciones
            negative_amounts, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM points_transactions 
                WHERE amount < 0 OR pending_amount < 0 OR withdrawn_amount < 0
            """)
            
            if negative_amounts[0][0] > 0:
                validation_results['errors'].append(f"{negative_amounts[0][0]} transactions con montos negativos")
                validation_results['valid'] = False

            # Validar consistencia en transacciones
            inconsistent_transactions, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM points_transactions 
                WHERE withdrawn_amount > amount
            """)
            
            if inconsistent_transactions[0][0] > 0:
                validation_results['errors'].append(f"{inconsistent_transactions[0][0]} transactions con monto retirado mayor al total")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de la carga"""
        return {
            'user_points_inserted': self.stats['user_points_inserted'],
            'transactions_inserted': self.stats['transactions_inserted'],
            'transaction_payments_inserted': self.stats['transaction_payments_inserted'],
            'user_points_deleted': self.stats['user_points_deleted'],
            'transactions_deleted': self.stats['transactions_deleted'],
            'transaction_payments_deleted': self.stats['transaction_payments_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        self.postgres_conn.disconnect()
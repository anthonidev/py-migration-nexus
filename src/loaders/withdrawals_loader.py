# src/loaders/withdrawals_loader.py
import os
import sys
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WithdrawalsLoader:
    
    def __init__(self):
        self.payments_conn = PaymentsPostgresConnection()
    
    def cleanup_existing_data(self) -> bool:
        """Limpia los datos existentes en las tablas de withdrawals"""
        logger.info("Limpiando datos existentes de withdrawals en ms-payments")
        
        try:
            # Desactivar temporalmente las restricciones de clave foránea para la limpieza
            self.payments_conn.execute_query("SET session_replication_role = replica;")
            
            # Limpiar en orden correcto (hijos primero)
            cleanup_queries = [
                "DELETE FROM withdrawal_points",
                "DELETE FROM withdrawals"
            ]
            
            for query in cleanup_queries:
                result = self.payments_conn.execute_query(query)
                logger.info(f"Ejecutado: {query} - Filas afectadas: {result[0] if result[0] is not None else 0}")
            
            # Reactivar las restricciones de clave foránea
            self.payments_conn.execute_query("SET session_replication_role = DEFAULT;")
            
            logger.info("Limpieza de datos completada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error limpiando datos existentes: {str(e)}")
            return False
    
    def load_withdrawals(self, withdrawals_data: List[Dict[str, Any]]) -> int:
        """Carga los withdrawals en la base de datos de ms-payments"""
        logger.info(f"Iniciando carga de {len(withdrawals_data)} withdrawals")
        
        if not withdrawals_data:
            logger.warning("No hay withdrawals para cargar")
            return 0
        
        try:
            # Query de inserción con ID específico
            insert_query = """
            INSERT INTO withdrawals (
                id, user_id, user_email, user_name, amount, status, 
                rejection_reason, created_at, updated_at, reviewed_by_id, 
                reviewed_by_email, reviewed_at, is_archived, bank_name, 
                account_number, cci, metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Preparar datos para inserción
            insert_data = []
            for withdrawal in withdrawals_data:
                # Convertir metadata dict a JSON string
                metadata_json = None
                if withdrawal['metadata']:
                    import json
                    metadata_json = json.dumps(withdrawal['metadata'])
                
                row = (
                    withdrawal['id'],
                    withdrawal['user_id'],
                    withdrawal['user_email'],
                    withdrawal['user_name'],
                    withdrawal['amount'],
                    withdrawal['status'],
                    withdrawal['rejection_reason'],
                    withdrawal['created_at'],
                    withdrawal['updated_at'],
                    withdrawal['reviewed_by_id'],
                    withdrawal['reviewed_by_email'],
                    withdrawal['reviewed_at'],
                    withdrawal['is_archived'],
                    withdrawal['bank_name'],
                    withdrawal['account_number'],
                    withdrawal['cci'],
                    metadata_json
                )
                insert_data.append(row)
            
            # Ejecutar inserción en lotes
            rows_inserted = self.payments_conn.execute_bulk_insert(insert_query, insert_data)
            
            # Actualizar la secuencia del ID para futuros inserts
            self._update_sequence('withdrawals', 'id', withdrawals_data)
            
            logger.info(f"Insertados {rows_inserted} withdrawals exitosamente")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Error cargando withdrawals: {str(e)}")
            raise
    
    def load_withdrawal_points(self, withdrawal_points_data: List[Dict[str, Any]]) -> int:
        """Carga los withdrawal_points en la base de datos de ms-payments"""
        logger.info(f"Iniciando carga de {len(withdrawal_points_data)} withdrawal_points")
        
        if not withdrawal_points_data:
            logger.warning("No hay withdrawal_points para cargar")
            return 0
        
        try:
            # Query de inserción con ID específico
            insert_query = """
            INSERT INTO withdrawal_points (
                id, withdrawal_id, points_transaction_id, points_amount, 
                amount_used, metadata, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Preparar datos para inserción
            insert_data = []
            for point in withdrawal_points_data:
                # Convertir metadata dict a JSON string
                metadata_json = None
                if point['metadata']:
                    import json
                    metadata_json = json.dumps(point['metadata'])
                
                row = (
                    point['id'],
                    point['withdrawal_id'],
                    point['points_transaction_id'],
                    point['points_amount'],
                    point['amount_used'],
                    metadata_json,
                    point['created_at'],
                    point['updated_at']
                )
                insert_data.append(row)
            
            # Ejecutar inserción en lotes
            rows_inserted = self.payments_conn.execute_bulk_insert(insert_query, insert_data)
            
            # Actualizar la secuencia del ID para futuros inserts
            self._update_sequence('withdrawal_points', 'id', withdrawal_points_data)
            
            logger.info(f"Insertados {rows_inserted} withdrawal_points exitosamente")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Error cargando withdrawal_points: {str(e)}")
            raise
    
    def _update_sequence(self, table_name: str, id_column: str, data: List[Dict[str, Any]]):
        """Actualiza la secuencia de ID para que los próximos inserts sean correctos"""
        if not data:
            return
        
        try:
            # Obtener el ID máximo de los datos insertados
            max_id = max(item['id'] for item in data)
            
            # Actualizar la secuencia
            sequence_name = f"{table_name}_{id_column}_seq"
            update_sequence_query = f"SELECT setval('{sequence_name}', %s, true)"
            
            self.payments_conn.execute_query(update_sequence_query, (max_id,))
            logger.info(f"Secuencia {sequence_name} actualizada a {max_id}")
            
        except Exception as e:
            logger.warning(f"Error actualizando secuencia {table_name}: {str(e)}")
    
    def verify_data_integrity(self) -> Dict[str, Any]:
        """Verifica la integridad de los datos cargados"""
        logger.info("Verificando integridad de datos de withdrawals")
        
        integrity_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            # Contar withdrawals
            withdrawals_count_query = "SELECT COUNT(*) FROM withdrawals"
            withdrawals_count, _ = self.payments_conn.execute_query(withdrawals_count_query)
            withdrawals_total = withdrawals_count[0][0]
            
            # Contar withdrawal_points
            points_count_query = "SELECT COUNT(*) FROM withdrawal_points"
            points_count, _ = self.payments_conn.execute_query(points_count_query)
            points_total = points_count[0][0]
            
            # Verificar integridad referencial
            orphan_points_query = """
            SELECT COUNT(*) FROM withdrawal_points wp 
            LEFT JOIN withdrawals w ON wp.withdrawal_id = w.id 
            WHERE w.id IS NULL
            """
            orphan_points, _ = self.payments_conn.execute_query(orphan_points_query)
            orphan_count = orphan_points[0][0]
            
            # Verificar withdrawals con datos faltantes
            missing_user_query = """
            SELECT COUNT(*) FROM withdrawals 
            WHERE user_id IS NULL OR user_email IS NULL OR user_email = ''
            """
            missing_user, _ = self.payments_conn.execute_query(missing_user_query)
            missing_user_count = missing_user[0][0]
            
            # Verificar withdrawals con amounts inválidos
            invalid_amount_query = """
            SELECT COUNT(*) FROM withdrawals 
            WHERE amount IS NULL OR amount <= 0
            """
            invalid_amount, _ = self.payments_conn.execute_query(invalid_amount_query)
            invalid_amount_count = invalid_amount[0][0]
            
            # Verificar datos bancarios faltantes
            missing_bank_query = """
            SELECT COUNT(*) FROM withdrawals 
            WHERE bank_name IS NULL OR bank_name = '' 
               OR account_number IS NULL OR account_number = ''
            """
            missing_bank, _ = self.payments_conn.execute_query(missing_bank_query)
            missing_bank_count = missing_bank[0][0]
            
            # Almacenar estadísticas
            integrity_results['stats'] = {
                'total_withdrawals': withdrawals_total,
                'total_withdrawal_points': points_total,
                'orphan_points': orphan_count,
                'missing_user_data': missing_user_count,
                'invalid_amounts': invalid_amount_count,
                'missing_bank_data': missing_bank_count
            }
            
            # Validar resultados
            if orphan_count > 0:
                integrity_results['errors'].append(f"{orphan_count} withdrawal_points huérfanos")
                integrity_results['valid'] = False
            
            if missing_user_count > 0:
                integrity_results['errors'].append(f"{missing_user_count} withdrawals sin datos de usuario")
                integrity_results['valid'] = False
            
            if invalid_amount_count > 0:
                integrity_results['errors'].append(f"{invalid_amount_count} withdrawals con amounts inválidos")
                integrity_results['valid'] = False
            
            if missing_bank_count > 0:
                integrity_results['warnings'].append(f"{missing_bank_count} withdrawals sin datos bancarios completos")
            
            logger.info(f"Verificación de integridad completada: {integrity_results['stats']}")
            return integrity_results
            
        except Exception as e:
            logger.error(f"Error verificando integridad: {str(e)}")
            integrity_results['valid'] = False
            integrity_results['errors'].append(f"Error en verificación: {str(e)}")
            return integrity_results
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.payments_conn:
            self.payments_conn.disconnect()
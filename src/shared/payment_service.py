from typing import Optional, Dict, Any, List
from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentService:

    def __init__(self):
        self.postgres_conn = PaymentsPostgresConnection()

    def get_payment_by_id(self, payment_id: int) -> Optional[Dict[str, Any]]:
     
        try:
            query = """
            SELECT 
                id,
                operation_code,
                payment_method,
                status,
                amount
            FROM payments 
            WHERE id = %s
            """
            
            results, columns = self.postgres_conn.execute_query(query, (payment_id,))
            
            if not results:
                logger.warning(f"Pago no encontrado: ID {payment_id}")
                return None
            
            payment_data = dict(zip(columns, results[0]))
            
            result = {
                'id': payment_data['id'],
                'operationCode': payment_data['operation_code'],
                'paymentMethod': payment_data['payment_method'],
                'status': payment_data['status'],
                'amount': float(payment_data['amount']) if payment_data['amount'] else 0.0
            }
            
            logger.info(f"Pago encontrado: ID {payment_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo pago ID {payment_id}: {str(e)}")
            return None

    def get_payments_batch(self, payment_ids: List[int]) -> Dict[int, Dict[str, Any]]:
       
        if not payment_ids:
            logger.warning("Lista de IDs de pagos vacía")
            return {}
        
        try:
            placeholders = ','.join(['%s'] * len(payment_ids))
            
            query = f"""
            SELECT 
                id,
                operation_code,
                payment_method,
                status,
                amount
            FROM payments 
            WHERE id IN ({placeholders})
            """
            
            results, columns = self.postgres_conn.execute_query(query, payment_ids)
            
            if not results:
                logger.warning(f"Ningún pago encontrado para los IDs: {payment_ids}")
                return {}
            
            payments_dict = {}
            
            for row in results:
                payment_data = dict(zip(columns, row))
                payment_id = payment_data['id']
                
                payments_dict[payment_id] = {
                    'operationCode': payment_data['operation_code'],
                    'paymentMethod': payment_data['payment_method'],
                    'status': payment_data['status'],
                    'amount': float(payment_data['amount']) if payment_data['amount'] else 0.0
                }
            
            found_count = len(payments_dict)
            total_requested = len(payment_ids)
            
            if found_count < total_requested:
                missing_ids = set(payment_ids) - set(payments_dict.keys())
                logger.warning(f"No se encontraron {total_requested - found_count} pagos: {missing_ids}")
            
            logger.info(f"Pagos obtenidos en lote: {found_count}/{total_requested}")
            return payments_dict
            
        except Exception as e:
            logger.error(f"Error obteniendo pagos en lote {payment_ids}: {str(e)}")
            return {}

    def close_connection(self):
        if self.postgres_conn:
            self.postgres_conn.disconnect()
# src/loaders/orders_loader.py
from typing import List, Dict, Any
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.orders_postgres_connection import OrdersPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class OrdersLoader:
    
    def __init__(self):
        self.postgres_conn = OrdersPostgresConnection()
    
    def _check_tables_exist(self):
        """Verifica que las tablas necesarias existan"""
        tables_to_check = ['orders', 'orders_details', 'orders_history', 'products']
        
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
        
        logger.info("✅ Todas las tablas necesarias existen")
    
    def load_orders_data(self, transformation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Carga los datos transformados en la base de datos de órdenes"""
        logger.info("Iniciando carga de datos de órdenes")
        
        if not transformation_result['success']:
            return {
                'success': False,
                'errors': ['Los datos de transformación contienen errores']
            }
        
        try:
            # Limpiar datos existentes (en orden correcto para evitar FK constraints)
            logger.info("Limpiando datos existentes...")
            self._clean_existing_data()
            
            orders = transformation_result['orders']
            orders_details = transformation_result['orders_details']
            orders_history = transformation_result['orders_history']
            
            orders_inserted = 0
            orders_details_inserted = 0
            orders_history_inserted = 0
            
            # 1. Insertar órdenes principales
            if orders:
                logger.info(f"Insertando {len(orders)} órdenes...")
                orders_inserted = self._insert_orders(orders)
            
            # 2. Insertar detalles de órdenes
            if orders_details:
                logger.info(f"Insertando {len(orders_details)} detalles de órdenes...")
                orders_details_inserted = self._insert_orders_details(orders_details)
            
            # 3. Insertar historial de órdenes
            if orders_history:
                logger.info(f"Insertando {len(orders_history)} historiales de órdenes...")
                orders_history_inserted = self._insert_orders_history(orders_history)
            
            # Resetear secuencias para mantener los IDs originales
            self._reset_sequences()
            
            result = {
                'success': True,
                'orders_inserted': orders_inserted,
                'orders_details_inserted': orders_details_inserted,
                'orders_history_inserted': orders_history_inserted
            }
            
            logger.info(f"Carga completada - Órdenes: {orders_inserted}, Detalles: {orders_details_inserted}, Historiales: {orders_history_inserted}")
            return result
            
        except Exception as e:
            logger.error(f"Error cargando datos de órdenes: {str(e)}")
            return {
                'success': False,
                'errors': [f"Error en carga: {str(e)}"]
            }
    
    def _clean_existing_data(self):
        """Limpia los datos existentes en el orden correcto"""
        # Orden importante: primero las tablas dependientes, luego las principales
        tables = ['orders_history', 'orders_details', 'orders']
        
        for table in tables:
            delete_query = f"DELETE FROM {table}"
            self.postgres_conn.execute_query(delete_query)
            logger.info(f"✅ Tabla {table} limpiada")
    
    def _insert_orders(self, orders: List[Dict[str, Any]]) -> int:
        """Inserta las órdenes principales"""
        insert_query = """
        INSERT INTO orders (
            id, user_id, user_email, user_name, total_items, total_amount, 
            status, metadata, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params_list = []
        for order in orders:
            # Serializar metadata como JSON si existe
            metadata_json = None
            if order.get('metadata'):
                metadata_json = json.dumps(order['metadata']) if isinstance(order['metadata'], dict) else order['metadata']
            
            params = (
                order['id'],
                order['userId'],
                order['userEmail'],
                order['userName'],
                order['totalItems'],
                order['totalAmount'],
                order['status'],
                metadata_json,
                order['createdAt'],
                order['updatedAt']
            )
            params_list.append(params)
        
        return self.postgres_conn.execute_bulk_insert(insert_query, params_list)
    
    def _insert_orders_details(self, orders_details: List[Dict[str, Any]]) -> int:
        """Inserta los detalles de las órdenes"""
        insert_query = """
        INSERT INTO orders_details (
            id, order_id, product_id, price, quantity, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params_list = []
        for detail in orders_details:
            params = (
                detail['id'],
                detail['order_id'],
                detail['product_id'],
                detail['price'],
                detail['quantity'],
                detail['createdAt'],
                detail['updatedAt']
            )
            params_list.append(params)
        
        return self.postgres_conn.execute_bulk_insert(insert_query, params_list)
    
    def _insert_orders_history(self, orders_history: List[Dict[str, Any]]) -> int:
        """Inserta el historial de las órdenes"""
        insert_query = """
        INSERT INTO orders_history (
            id, order_id, user_id, user_email, user_name, action, 
            changes, notes, metadata, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params_list = []
        for history in orders_history:
            # Serializar campos JSON si existen
            changes_json = None
            if history.get('changes'):
                changes_json = json.dumps(history['changes']) if isinstance(history['changes'], dict) else history['changes']
                
            metadata_json = None
            if history.get('metadata'):
                metadata_json = json.dumps(history['metadata']) if isinstance(history['metadata'], dict) else history['metadata']
            
            params = (
                history['id'],
                history['order_id'],
                history['userId'],
                history['userEmail'],
                history['userName'],
                history['action'],
                changes_json,
                history['notes'],
                metadata_json,
                history['createdAt']
            )
            params_list.append(params)
        
        return self.postgres_conn.execute_bulk_insert(insert_query, params_list)
    
    def _reset_sequences(self):
        """Resetea las secuencias para mantener los IDs originales"""
        try:
            # Para la tabla orders (ID numérico)
            max_order_id_query = "SELECT COALESCE(MAX(id), 0) FROM orders"
            result, _ = self.postgres_conn.execute_query(max_order_id_query)
            max_order_id = result[0][0]
            
            if max_order_id > 0:
                reset_orders_seq = f"SELECT setval('orders_id_seq', {max_order_id}, true)"
                self.postgres_conn.execute_query(reset_orders_seq)
                logger.info(f"✅ Secuencia de orders reseteada a {max_order_id}")
            
            # Para orders_details y orders_history se usan UUIDs, no necesitan reset de secuencia
            logger.info("✅ Secuencias reseteadas correctamente")
            
        except Exception as e:
            logger.warning(f"Advertencia al resetear secuencias: {str(e)}")
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos cargados")
        
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            # Contar registros totales
            orders_count_query = "SELECT COUNT(*) FROM orders"
            orders_result, _ = self.postgres_conn.execute_query(orders_count_query)
            total_orders = orders_result[0][0]
            
            details_count_query = "SELECT COUNT(*) FROM orders_details"
            details_result, _ = self.postgres_conn.execute_query(details_count_query)
            total_details = details_result[0][0]
            
            history_count_query = "SELECT COUNT(*) FROM orders_history"
            history_result, _ = self.postgres_conn.execute_query(history_count_query)
            total_history = history_result[0][0]
            
            validation_result['stats'] = {
                'total_orders': total_orders,
                'total_order_details': total_details,
                'total_order_history': total_history
            }
            
            # Validar que no hay órdenes huérfanas en detalles
            orphan_details_query = """
            SELECT COUNT(*) 
            FROM orders_details od 
            LEFT JOIN orders o ON od.order_id = o.id 
            WHERE o.id IS NULL
            """
            orphan_result, _ = self.postgres_conn.execute_query(orphan_details_query)
            orphan_details = orphan_result[0][0]
            
            if orphan_details > 0:
                validation_result['errors'].append(f"{orphan_details} detalles de órdenes huérfanos")
                validation_result['valid'] = False
            
            # Validar que no hay órdenes huérfanas en historial
            orphan_history_query = """
            SELECT COUNT(*) 
            FROM orders_history oh 
            LEFT JOIN orders o ON oh.order_id = o.id 
            WHERE o.id IS NULL
            """
            orphan_history_result, _ = self.postgres_conn.execute_query(orphan_history_query)
            orphan_history = orphan_history_result[0][0]
            
            if orphan_history > 0:
                validation_result['errors'].append(f"{orphan_history} historiales de órdenes huérfanos")
                validation_result['valid'] = False
            
            # Validar que no hay referencias a productos inexistentes
            invalid_products_query = """
            SELECT COUNT(*) 
            FROM orders_details od 
            LEFT JOIN products p ON od.product_id = p.id 
            WHERE p.id IS NULL
            """
            invalid_products_result, _ = self.postgres_conn.execute_query(invalid_products_query)
            invalid_products = invalid_products_result[0][0]
            
            if invalid_products > 0:
                validation_result['errors'].append(f"{invalid_products} detalles con productos inexistentes")
                validation_result['valid'] = False
            
            # Validar consistencia de cantidades totales
            inconsistent_totals_query = """
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN (
                SELECT order_id, SUM(quantity) as calculated_total
                FROM orders_details 
                GROUP BY order_id
            ) calc ON o.id = calc.order_id
            WHERE o.total_items != COALESCE(calc.calculated_total, 0)
            """
            inconsistent_result, _ = self.postgres_conn.execute_query(inconsistent_totals_query)
            inconsistent_totals = inconsistent_result[0][0]
            
            if inconsistent_totals > 0:
                validation_result['warnings'].append(f"{inconsistent_totals} órdenes con cantidades inconsistentes")
            
            # Validar que todas las órdenes tienen al menos un detalle
            no_details_query = """
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN orders_details od ON o.id = od.order_id
            WHERE od.order_id IS NULL
            """
            no_details_result, _ = self.postgres_conn.execute_query(no_details_query)
            no_details = no_details_result[0][0]
            
            if no_details > 0:
                validation_result['warnings'].append(f"{no_details} órdenes sin detalles")
            
            logger.info(f"Validación de integridad: {'EXITOSA' if validation_result['valid'] else 'CON ERRORES'}")
            return validation_result
            
        except Exception as e:
            logger.error(f"Error en validación de integridad: {str(e)}")
            validation_result['valid'] = False
            validation_result['errors'].append(f"Error en validación: {str(e)}")
            return validation_result
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.postgres_conn:
            self.postgres_conn.disconnect()
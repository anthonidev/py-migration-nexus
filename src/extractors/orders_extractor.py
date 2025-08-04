from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class OrdersExtractor:
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
    
    def extract_orders_data(self) -> List[Dict[str, Any]]:
        """Extrae todas las órdenes con sus detalles e historial desde PostgreSQL"""
        logger.info("Iniciando extracción de órdenes desde PostgreSQL")
        
        query = """
        SELECT
            o.id,
            o."totalItems",
            o."totalAmount",
            o.status,
            o.metadata,
            o."createdAt",
            o."updatedAt",
            JSON_BUILD_OBJECT(
                    'userEmail', u.email
            ) AS "user",
            COALESCE(order_history_agg.order_history, '[]'::json) AS "orderHistory",
            COALESCE(order_details_agg.order_details, '[]'::json) AS "orderDetails"
        FROM
            public.orders o
                INNER JOIN public.users u ON o.user_id = u.id
                LEFT JOIN (
                SELECT
                    oh.order_id,
                    JSON_AGG(
                            JSON_BUILD_OBJECT(
                                    'id', oh.id,
                                    'performedBy', CASE
                                                       WHEN oh.performed_by_id IS NOT NULL THEN
                                                           JSON_BUILD_OBJECT(
                                                                   'userEmail', pu.email
                                                           )
                                        END,
                                    'action', oh.action,
                                    'changes', oh.changes,
                                    'notes', oh.notes,
                                    'metadata', oh.metadata,
                                    'createdAt', oh."createdAt"
                            )
                            ORDER BY oh."createdAt"
                    ) AS order_history
                FROM
                    public.orders_history oh
                        LEFT JOIN public.users pu ON oh.performed_by_id = pu.id
                GROUP BY
                    oh.order_id
            ) order_history_agg ON o.id = order_history_agg.order_id
                LEFT JOIN (
                SELECT
                    od.order_id,
                    JSON_AGG(
                            JSON_BUILD_OBJECT(
                                    'id', od.id,
                                    'price', od.price,
                                    'quantity', od.quantity,
                                    'createdAt', od."createdAt",
                                    'updatedAt', od."updatedAt",
                                    'product', JSON_BUILD_OBJECT(
                                            'id', p.id
                                           )
                            )
                            ORDER BY od."createdAt"
                    ) AS order_details
                FROM
                    public.orders_details od
                        LEFT JOIN public.products p ON od.product_id = p.id
                GROUP BY
                    od.order_id
            ) order_details_agg ON o.id = order_details_agg.order_id
        ORDER BY
            o."createdAt" DESC;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            orders_data = []
            for row in results:
                order_dict = dict(zip(columns, row))
                orders_data.append(order_dict)
            
            logger.info(f"Extraídas {len(orders_data)} órdenes desde PostgreSQL")
            return orders_data
            
        except Exception as e:
            logger.error(f"Error extrayendo órdenes: {str(e)}")
            raise
    
    def validate_source_data(self) -> Dict[str, Any]:
        """Valida que los datos de origen sean consistentes"""
        logger.info("Validando datos de origen para órdenes")
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validar órdenes sin usuario
            orphan_orders_query = """
            SELECT COUNT(*) 
            FROM orders o 
            LEFT JOIN users u ON o.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_results, _ = self.postgres_conn.execute_query(orphan_orders_query)
            orphan_orders = orphan_results[0][0]
            
            if orphan_orders > 0:
                validation_results['errors'].append(f"{orphan_orders} órdenes sin usuario válido")
                validation_results['valid'] = False
            
            # Validar detalles de órdenes sin producto válido
            invalid_details_query = """
            SELECT COUNT(*) 
            FROM orders_details od 
            LEFT JOIN products p ON od.product_id = p.id 
            WHERE p.id IS NULL
            """
            invalid_details_results, _ = self.postgres_conn.execute_query(invalid_details_query)
            invalid_details = invalid_details_results[0][0]
            
            if invalid_details > 0:
                validation_results['errors'].append(f"{invalid_details} detalles de órdenes con productos inválidos")
                validation_results['valid'] = False
            
            # Validar órdenes con montos negativos o nulos
            invalid_amounts_query = """
            SELECT COUNT(*) 
            FROM orders 
            WHERE "totalAmount" <= 0 OR "totalAmount" IS NULL
            """
            invalid_amounts_results, _ = self.postgres_conn.execute_query(invalid_amounts_query)
            invalid_amounts = invalid_amounts_results[0][0]
            
            if invalid_amounts > 0:
                validation_results['errors'].append(f"{invalid_amounts} órdenes con montos inválidos")
                validation_results['valid'] = False
            
            # Validar órdenes con cantidad de items inconsistente
            inconsistent_items_query = """
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN (
                SELECT order_id, SUM(quantity) as calculated_total
                FROM orders_details 
                GROUP BY order_id
            ) calc ON o.id = calc.order_id
            WHERE o."totalItems" != COALESCE(calc.calculated_total, 0)
            """
            inconsistent_results, _ = self.postgres_conn.execute_query(inconsistent_items_query)
            inconsistent_items = inconsistent_results[0][0]
            
            if inconsistent_items > 0:
                validation_results['warnings'].append(f"{inconsistent_items} órdenes con cantidad de items inconsistente")
            
            # Validar órdenes sin detalles
            no_details_query = """
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN orders_details od ON o.id = od.order_id
            WHERE od.order_id IS NULL
            """
            no_details_results, _ = self.postgres_conn.execute_query(no_details_query)
            no_details = no_details_results[0][0]
            
            if no_details > 0:
                validation_results['warnings'].append(f"{no_details} órdenes sin detalles")
            
            # Validar historiales con usuarios inválidos en performed_by
            invalid_history_users_query = """
            SELECT COUNT(*) 
            FROM orders_history oh
            LEFT JOIN users u ON oh.performed_by_id = u.id
            WHERE oh.performed_by_id IS NOT NULL AND u.id IS NULL
            """
            invalid_history_results, _ = self.postgres_conn.execute_query(invalid_history_users_query)
            invalid_history_users = invalid_history_results[0][0]
            
            if invalid_history_users > 0:
                validation_results['warnings'].append(f"{invalid_history_users} historiales con usuarios inválidos en performed_by")
            
            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'CON ERRORES'}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results
    
    def close_connection(self):
        self.postgres_conn.disconnect()
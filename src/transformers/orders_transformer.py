# src/transformers/orders_transformer.py
from typing import List, Dict, Any
import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)

class OrdersTransformer:
    
    def __init__(self):
        self.user_service = UserService()
        self.transformation_errors = []
        self.transformation_stats = {
            'orders': [],
            'orders_details': [],
            'orders_history': []
        }
        # Mapeo de status del monolito a los enum del microservicio
        self.status_mapping = {
            'PENDING': 'PENDING',
            'APPROVED': 'APPROVED', 
            'SENT': 'SENT',
            'DELIVERED': 'DELIVERED',
            'REJECTED': 'REJECTED',
            'CANCELLED': 'REJECTED'  # Mapear CANCELLED a REJECTED
        }
        
        # Mapeo de actions del monolito a los enum del microservicio
        self.action_mapping = {
            'CREATED': 'CREATED',
            'APPROVED': 'APPROVED',
            'SENT': 'SENT', 
            'DELIVERED': 'DELIVERED',
            'REJECTED': 'REJECTED',
            'CANCELLED': 'CANCELLED'
        }
    
    def transform_orders_data(self, orders_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transforma los datos de órdenes del monolito al formato del microservicio"""
        logger.info("Iniciando transformación de datos de órdenes")
        
        try:
            # Obtener todos los usuarios únicos para optimizar consultas
            user_emails = set()
            for order in orders_data:
                user_emails.add(order['user']['userEmail'])
                
                # Agregar emails de los historiales
                if order.get('orderHistory'):
                    for history in order['orderHistory']:
                        if history.get('performedBy') and history['performedBy'].get('userEmail'):
                            user_emails.add(history['performedBy']['userEmail'])
            
            # Obtener información de usuarios en lote
            users_dict = self.user_service.get_users_batch(list(user_emails))
            
            orders = []
            orders_details = []
            orders_history = []
            
            transformation_errors = []
            
            # Almacenar estadísticas para el resumen
            self.transformation_stats['orders'] = []
            self.transformation_stats['orders_details'] = []
            self.transformation_stats['orders_history'] = []
            
            for order_data in orders_data:
                try:
                    # Obtener información del usuario
                    user_email = order_data['user']['userEmail']
                    user_info = users_dict.get(user_email)
                    
                    if not user_info:
                        transformation_errors.append(f"Usuario no encontrado: {user_email}")
                        continue
                    
                    # Transformar orden principal
                    transformed_order = {
                        'id': order_data['id'],  # Conservar el mismo ID
                        'userId': user_info['id'],
                        'userEmail': user_info['email'],
                        'userName': user_info.get('fullName', ''),  # Usar fullName del servicio
                        'totalItems': order_data['totalItems'],
                        'totalAmount': float(order_data['totalAmount']),
                        'status': self.status_mapping.get(order_data['status'], 'PENDING'),
                        'metadata': order_data.get('metadata'),
                        'createdAt': order_data['createdAt'],
                        'updatedAt': order_data['updatedAt']
                    }
                    orders.append(transformed_order)
                    self.transformation_stats['orders'].append(transformed_order)
                    
                    # Transformar detalles de la orden
                    if order_data.get('orderDetails'):
                        for detail in order_data['orderDetails']:
                            transformed_detail = {
                                'id': detail['id'],  # Conservar el mismo ID
                                'order_id': order_data['id'],
                                'product_id': detail['product']['id'],
                                'price': float(detail['price']),
                                'quantity': detail['quantity'],
                                'createdAt': detail['createdAt'],
                                'updatedAt': detail['updatedAt']
                            }
                            orders_details.append(transformed_detail)
                            self.transformation_stats['orders_details'].append(transformed_detail)
                    
                    # Transformar historial de la orden
                    if order_data.get('orderHistory'):
                        for history in order_data['orderHistory']:
                            # Determinar información del usuario que realizó la acción
                            history_user_info = None
                            if history.get('performedBy') and history['performedBy'].get('userEmail'):
                                history_user_email = history['performedBy']['userEmail']
                                history_user_info = users_dict.get(history_user_email)
                            
                            # Si no se encuentra el usuario que realizó la acción, usar el usuario de la orden
                            if not history_user_info:
                                history_user_info = user_info
                            
                            transformed_history = {
                                'id': history['id'],  # Conservar el mismo ID
                                'order_id': order_data['id'],
                                'userId': history_user_info['id'],
                                'userEmail': history_user_info['email'],
                                'userName': history_user_info.get('fullName', ''),  # Usar fullName del servicio
                                'action': self.action_mapping.get(history['action'], history['action']),
                                'changes': history.get('changes'),
                                'notes': history.get('notes'),
                                'metadata': history.get('metadata'),
                                'createdAt': history['createdAt']
                            }
                            orders_history.append(transformed_history)
                            self.transformation_stats['orders_history'].append(transformed_history)
                
                except Exception as e:
                    transformation_errors.append(f"Error transformando orden {order_data.get('id', 'unknown')}: {str(e)}")
                    continue
            
            # Almacenar errores para el resumen
            self.transformation_errors = transformation_errors
            
            result = {
                'success': len(transformation_errors) == 0,
                'errors': transformation_errors,
                'orders': orders,
                'orders_details': orders_details,
                'orders_history': orders_history,
                'stats': {
                    'total_orders': len(orders),
                    'total_details': len(orders_details),
                    'total_history': len(orders_history),
                    'transformation_errors': len(transformation_errors)
                }
            }
            
            logger.info(f"Transformación completada - Órdenes: {len(orders)}, Detalles: {len(orders_details)}, Historiales: {len(orders_history)}")
            if transformation_errors:
                logger.warning(f"Se encontraron {len(transformation_errors)} errores de transformación")
            
            return result
            
        except Exception as e:
            logger.error(f"Error crítico en transformación de órdenes: {str(e)}")
            return {
                'success': False,
                'errors': [f"Error crítico: {str(e)}"],
                'orders': [],
                'orders_details': [],
                'orders_history': [],
                'stats': {
                    'total_orders': 0,
                    'total_details': 0,
                    'total_history': 0,
                    'transformation_errors': 1
                }
            }
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """Devuelve un resumen de la transformación para compatibilidad con process_transformation_summary"""
        return {
            'orders_transformed': len(self.transformation_stats.get('orders', [])),
            'details_transformed': len(self.transformation_stats.get('orders_details', [])),
            'history_transformed': len(self.transformation_stats.get('orders_history', [])),
            'total_errors': len(self.transformation_errors),
            'errors': self.transformation_errors,
            'warnings': []
        }
    
    def close_connections(self):
        """Cierra las conexiones de los servicios utilizados"""
        if hasattr(self, 'user_service'):
            self.user_service.close_connection()
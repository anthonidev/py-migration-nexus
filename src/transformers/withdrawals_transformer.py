import os
import sys
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WithdrawalsTransformer:
    
    def __init__(self):
        self.user_service = UserService()
        self.users_cache = {}
        self.stats = {
            'withdrawals_transformed': 0,
            'withdrawal_points_transformed': 0,
            'errors': []
        }
        
        # Mapeo de estados para withdrawals
        self.status_mapping = {
            'PENDING': 'PENDING',
            'APPROVED': 'APPROVED', 
            'REJECTED': 'REJECTED',
            # Valores por defecto para estados no reconocidos
            None: 'PENDING',
            '': 'PENDING'
        }
    
    def transform_withdrawals_data(self, withdrawals_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma los datos de withdrawals para ms-payments"""
        logger.info(f"Iniciando transformación de {len(withdrawals_data)} withdrawals")
        
        # 1. Obtener todos los emails únicos para consulta masiva
        unique_emails = set()
        for withdrawal in withdrawals_data:
            if withdrawal.get('user_email'):
                unique_emails.add(withdrawal['user_email'])
            if withdrawal.get('reviewed_by_email'):
                unique_emails.add(withdrawal['reviewed_by_email'])
        
        logger.info(f"Obteniendo información de {len(unique_emails)} usuarios únicos")
        
        # 2. Realizar consulta masiva de usuarios
        self.users_cache = self.user_service.get_users_batch(list(unique_emails))
        logger.info(f"Cache de usuarios cargado con {len(self.users_cache)} usuarios")
        
        transformed_withdrawals = []
        transformed_withdrawal_points = []
        
        for withdrawal in withdrawals_data:
            try:
                transformed_withdrawal, withdrawal_points = self._transform_single_withdrawal(withdrawal)
                transformed_withdrawals.append(transformed_withdrawal)
                transformed_withdrawal_points.extend(withdrawal_points)
                self.stats['withdrawals_transformed'] += 1
                
            except Exception as e:
                error_msg = f"Error transformando withdrawal {withdrawal.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        logger.info(f"Transformación completada: {self.stats['withdrawals_transformed']} withdrawals, {len(transformed_withdrawal_points)} withdrawal_points")
        return transformed_withdrawals, transformed_withdrawal_points
    
    def _transform_single_withdrawal(self, withdrawal: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Transforma un withdrawal individual"""
        withdrawal_id = withdrawal['id']
        
        # Obtener información del usuario desde el cache
        user_info = self._get_user_info_from_cache(withdrawal['user_email'])
        
        # Obtener información del reviewer si existe
        reviewer_info = None
        if withdrawal.get('reviewed_by_email'):
            reviewer_info = self._get_user_info_from_cache(withdrawal['reviewed_by_email'])
        
        # Validar amount
        amount = self._validate_decimal_field(
            withdrawal.get('amount'), 'amount', min_value=0.01
        )
        
        # Procesar status
        status = self.status_mapping.get(withdrawal.get('status'), 'PENDING')
        
        # Procesar fechas
        created_at = self._process_datetime(withdrawal.get('createdAt'))
        updated_at = self._process_datetime(withdrawal.get('updatedAt'))
        reviewed_at = self._process_datetime(withdrawal.get('reviewedAt'))
        
        # Procesar rejection reason
        rejection_reason = None
        if withdrawal.get('rejectionReason'):
            rejection_reason = str(withdrawal['rejectionReason']).strip()
            if not rejection_reason:
                rejection_reason = None
        
        # Procesar metadatos
        metadata = self._process_metadata(withdrawal.get('metadata'))
        
        # Validar campos bancarios requeridos
        bank_name = self._clean_text_field(withdrawal.get('bankName'), 100, 'bankName', required=True)
        account_number = self._clean_text_field(withdrawal.get('accountNumber'), 50, 'accountNumber', required=True)
        cci = self._clean_text_field(withdrawal.get('cci'), 50, 'cci', required=False)
        
        # Crear el withdrawal transformado
        transformed_withdrawal = {
            'id': withdrawal_id,  # Conservar el mismo ID
            'user_id': user_info['id'] if user_info else None,
            'user_email': user_info['email'] if user_info else withdrawal.get('user_email', ''),
            'user_name': user_info['fullName'] if user_info else None,
            'amount': amount,
            'status': status,
            'rejection_reason': rejection_reason,
            'created_at': created_at,
            'updated_at': updated_at,
            'reviewed_by_id': reviewer_info['id'] if reviewer_info else None,
            'reviewed_by_email': reviewer_info['email'] if reviewer_info else withdrawal.get('reviewed_by_email'),
            'reviewed_at': reviewed_at,
            'is_archived': bool(withdrawal.get('isArchived', False)),
            'bank_name': bank_name,
            'account_number': account_number,
            'cci': cci,
            'metadata': metadata
        }
        
        # Transformar withdrawal_points
        withdrawal_points = self._transform_withdrawal_points(withdrawal_id, withdrawal.get('withdrawal_points', []))
        
        return transformed_withdrawal, withdrawal_points
    
    def _transform_withdrawal_points(self, withdrawal_id: int, withdrawal_points_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforma los withdrawal_points de un withdrawal"""
        if not withdrawal_points_data:
            return []
        
        transformed_points = []
        
        for point_data in withdrawal_points_data:
            try:
                point_id = point_data['id']
                amount_used = self._validate_decimal_field(
                    point_data.get('amountUsed'), 'amountUsed', min_value=0.01
                )
                
                # Obtener información de la points_transaction
                points_transaction = point_data.get('pointsTransaction', {})
                points_transaction_id = str(points_transaction.get('id', '')) if points_transaction.get('id') else None
                points_amount = points_transaction.get('amount', 0) if points_transaction else 0
                
                # Crear metadata con información de la transacción de puntos
                points_metadata = self._create_points_metadata(points_transaction)
                
                # Procesar fechas - usar created_at para updated_at si no existe
                created_at = self._process_datetime(point_data.get('createdAt'))
                updated_at = created_at  # Como especificaste, usar created_at para updated_at
                
                transformed_point = {
                    'id': point_id,  # Conservar el mismo ID
                    'withdrawal_id': withdrawal_id,
                    'points_transaction_id': points_transaction_id,
                    'points_amount': points_amount,
                    'amount_used': amount_used,
                    'metadata': points_metadata,
                    'created_at': created_at,
                    'updated_at': updated_at
                }
                
                transformed_points.append(transformed_point)
                self.stats['withdrawal_points_transformed'] += 1
                
            except Exception as e:
                error_msg = f"Error transformando withdrawal_point {point_data.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        return transformed_points
    
    def _create_points_metadata(self, points_transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Crea metadata para withdrawal_points con información de la transacción de puntos"""
        if not points_transaction:
            return {}
        
        metadata = {
            'tipo_transaccion': points_transaction.get('type', ''),
            'monto_puntos': points_transaction.get('amount', 0),
            'estado_transaccion': points_transaction.get('status', ''),
            'monto_pendiente': points_transaction.get('pendingAmount', 0),
            'monto_retirado': points_transaction.get('withdrawnAmount', 0),
            'fecha_creacion': points_transaction.get('createdAt', ''),
            'metadata_original': points_transaction.get('metadata', {})
        }
        
        # Agregar información del plan de membresía si existe
        membership_plan = points_transaction.get('membershipPlan')
        if membership_plan:
            metadata['plan_membresia'] = {
                'id': membership_plan.get('id'),
                'nombre': membership_plan.get('name', ''),
                'precio': membership_plan.get('price', 0),
                'puntos_binarios': membership_plan.get('binaryPoints', 0),
                'porcentaje_comision': membership_plan.get('commissionPercentage', 0)
            }
        
        return metadata
    
    def _get_user_info_from_cache(self, email: str) -> Dict[str, Any]:
        """Obtiene información del usuario desde el cache"""
        if not email:
            return None
        
        user_info = self.users_cache.get(email.lower().strip())
        if not user_info:
            logger.warning(f"Usuario no encontrado en cache: {email}")
            return None
        
        return user_info
    
    def _validate_decimal_field(self, value, field_name: str, min_value: float = None, max_value: float = None, required: bool = True) -> float:
        """Valida y convierte campos decimales"""
        if value is None:
            if required:
                raise ValueError(f"Campo requerido {field_name} es nulo")
            return None
        
        try:
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    if required:
                        raise ValueError(f"Campo requerido {field_name} está vacío")
                    return None
            
            decimal_value = float(Decimal(str(value)))
            
            if min_value is not None and decimal_value < min_value:
                raise ValueError(f"{field_name} debe ser >= {min_value}, recibido: {decimal_value}")
            
            if max_value is not None and decimal_value > max_value:
                raise ValueError(f"{field_name} debe ser <= {max_value}, recibido: {decimal_value}")
            
            return decimal_value
            
        except (ValueError, TypeError, Exception) as e:
            raise ValueError(f"Valor inválido para {field_name}: {value} - {str(e)}")
    
    def _clean_text_field(self, value, max_length: int, field_name: str, required: bool = False) -> str:
        """Limpia y valida campos de texto"""
        if value is None:
            if required:
                raise ValueError(f"Campo requerido {field_name} es nulo")
            return None
        
        if isinstance(value, (int, float)):
            value = str(value)
        
        if not isinstance(value, str):
            if required:
                raise ValueError(f"Campo {field_name} debe ser texto")
            return None
        
        cleaned_value = value.strip()
        
        if not cleaned_value:
            if required:
                raise ValueError(f"Campo requerido {field_name} está vacío")
            return None
        
        if len(cleaned_value) > max_length:
            logger.warning(f"Campo {field_name} truncado de {len(cleaned_value)} a {max_length} caracteres")
            cleaned_value = cleaned_value[:max_length]
        
        return cleaned_value
    
    def _process_datetime(self, datetime_value) -> datetime:
        """Procesa campos de fecha y hora"""
        if datetime_value is None:
            return None
        
        if isinstance(datetime_value, str):
            try:
                # Intentar parsear diferentes formatos de fecha
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
                    try:
                        return datetime.strptime(datetime_value.replace('T', ' ').replace('Z', ''), fmt.replace('T', ' ').replace('Z', ''))
                    except ValueError:
                        continue
                
                # Si no funciona ningún formato, usar el valor tal como está
                logger.warning(f"Formato de fecha no reconocido: {datetime_value}")
                return datetime_value
                
            except Exception as e:
                logger.warning(f"Error procesando fecha {datetime_value}: {str(e)}")
                return None
        
        return datetime_value
    
    def _process_metadata(self, metadata_value) -> Dict[str, Any]:
        """Procesa campos de metadata JSON"""
        if metadata_value is None:
            return {}
        
        if isinstance(metadata_value, dict):
            return metadata_value
        
        if isinstance(metadata_value, str):
            try:
                return json.loads(metadata_value)
            except json.JSONDecodeError:
                logger.warning(f"Metadata JSON inválido: {metadata_value}")
                return {'original_metadata': metadata_value}
        
        return {'metadata_value': str(metadata_value)}
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """Retorna un resumen de la transformación"""
        return {
            'withdrawals_transformed': self.stats['withdrawals_transformed'],
            'withdrawal_points_transformed': self.stats['withdrawal_points_transformed'],
            'errors': self.stats['errors'],
            'total_errors': len(self.stats['errors'])
        }
    
    def close_connections(self):
        """Cierra las conexiones a servicios"""
        if self.user_service:
            self.user_service.close_connection()
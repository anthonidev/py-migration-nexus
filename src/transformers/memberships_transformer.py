import json
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date
from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MembershipsTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.stats = {
            'memberships_transformed': 0,
            'reconsumptions_transformed': 0,
            'history_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform_memberships_data(self, memberships_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        logger.info(f"Iniciando transformación de {len(memberships_data)} membresías")

        transformed_memberships = []
        transformed_reconsumptions = []
        transformed_history = []

        for membership in memberships_data:
            try:
                transformed_membership, reconsumptions, history = self._transform_single_membership(membership)
                transformed_memberships.append(transformed_membership)
                transformed_reconsumptions.extend(reconsumptions)
                transformed_history.extend(history)
                self.stats['memberships_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando membresía {membership.get('membership_id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(f"Transformación completada: {self.stats['memberships_transformed']} membresías, {len(transformed_reconsumptions)} reconsumptions, {len(transformed_history)} history")
        return transformed_memberships, transformed_reconsumptions, transformed_history

    def _transform_single_membership(self, membership: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        membership_id = membership['membership_id']

        # Obtener información del usuario
        user_info = self._get_user_info(membership['userEmail'])

        # Procesar fechas
        start_date = self._process_date(membership.get('startDate'))
        end_date = self._process_date(membership.get('endDate'))

        # Validar fechas
        if end_date and start_date and end_date < start_date:
            raise ValueError(f"La fecha de fin no puede ser anterior a la fecha de inicio para membresía {membership_id}")

        # Procesar estado
        status = self._map_membership_status(membership.get('status', ''))

        # Procesar monto mínimo de reconsumo
        minimum_reconsumption_amount = self._validate_decimal_field(
            membership.get('minimumReconsumptionAmount'), 
            'minimumReconsumptionAmount', 
            min_value=0.0,
            default_value=217.0
        )

        # Crear membresía transformada
        transformed_membership = {
            'id': membership_id,  # Conservar ID original
            'user_id': user_info['id'] if user_info else None,
            'user_email': user_info['email'] if user_info else membership['userEmail'],
            'user_name': user_info['fullName'] if user_info else None,
            'from_plan': False,  # Por defecto false
            'from_plan_id': None,  # Por defecto null
            'plan_id': membership['plan_id'],
            'start_date': start_date,
            'end_date': end_date,
            'status': status,
            'minimum_reconsumption_amount': 300.0,
            'auto_renewal': bool(membership.get('autoRenewal', False)),
            'metadata': {},  # Siempre objeto vacío según especificación
            'created_at': self._process_datetime(membership.get('createdAt')),
            'updated_at': self._process_datetime(membership.get('updatedAt'))
        }

        # Transformar reconsumptions
        reconsumptions = self._transform_reconsumptions(
            membership_id, 
            membership.get('reconsumptions', [])
        )

        # Transformar history
        history = self._transform_history(
            membership_id, 
            membership.get('membership_history', [])
        )

        return transformed_membership, reconsumptions, history

    def _transform_reconsumptions(self, membership_id: int, reconsumptions_data: Any) -> List[Dict[str, Any]]:
        if not reconsumptions_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(reconsumptions_data, str):
            try:
                reconsumptions_data = json.loads(reconsumptions_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear reconsumptions JSON para membresía {membership_id}")
                return []

        if not isinstance(reconsumptions_data, list):
            return []

        transformed_reconsumptions = []

        for reconsumption in reconsumptions_data:
            try:
                original_id = reconsumption.get('id')
                if not original_id:
                    logger.warning(f"Reconsumption de membresía {membership_id} sin ID original, se omitirá")
                    continue

                amount = self._validate_decimal_field(
                    reconsumption.get('amount'), 
                    'amount', 
                    min_value=0.0
                )

                status = self._map_reconsumption_status(reconsumption.get('status', ''))
                
                period_date = self._process_date(reconsumption.get('periodDate'))
                if not period_date:
                    logger.warning(f"Reconsumption {original_id} sin fecha de período, se omitirá")
                    continue

                payment_reference = self._clean_text_field(reconsumption.get('paymentReference'))
                notes = self._clean_text_field(reconsumption.get('notes'))

                # Procesar payment_details
                payment_details = self._process_json_field(reconsumption.get('paymentDetails'))

                transformed_reconsumption = {
                    'id': original_id,  # Conservar ID original
                    'membership_id': membership_id,
                    'amount': amount,
                    'status': status,
                    'period_date': period_date,
                    'payment_reference': payment_reference,
                    'payment_details': payment_details,
                    'notes': notes,
                    'created_at': self._process_datetime(reconsumption.get('createdAt')),
                    'updated_at': self._process_datetime(reconsumption.get('updatedAt'))
                }

                transformed_reconsumptions.append(transformed_reconsumption)
                self.stats['reconsumptions_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando reconsumption de membresía {membership_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_reconsumptions

    def _transform_history(self, membership_id: int, history_data: Any) -> List[Dict[str, Any]]:
        if not history_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(history_data, str):
            try:
                history_data = json.loads(history_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear history JSON para membresía {membership_id}")
                return []

        if not isinstance(history_data, list):
            return []

        transformed_history = []

        for history_item in history_data:
            try:
                original_id = history_item.get('id')
                if not original_id:
                    logger.warning(f"History item de membresía {membership_id} sin ID original, se omitirá")
                    continue

                action = self._map_history_action(history_item.get('action', ''))
                
                changes = self._process_json_field(history_item.get('changes'))
                metadata = self._process_json_field(history_item.get('metadata'))
                notes = self._clean_text_field(history_item.get('notes'))

                transformed_history_item = {
                    'id': original_id,  # Conservar ID original
                    'membership_id': membership_id,
                    'action': action,
                    'changes': changes,
                    'notes': notes,
                    'metadata': metadata,
                    'created_at': self._process_datetime(history_item.get('createdAt'))
                }

                transformed_history.append(transformed_history_item)
                self.stats['history_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando history item de membresía {membership_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_history

    def _get_user_info(self, email: str) -> Optional[Dict[str, Any]]:
        if not email:
            return None

        try:
            user_info = self.user_service.get_user_by_email(email)
            if not user_info:
                logger.warning(f"Usuario no encontrado: {email}")
            return user_info

        except Exception as e:
            logger.error(f"Error obteniendo información del usuario {email}: {str(e)}")
            return None

    def _map_membership_status(self, status: str) -> str:
        if not status:
            return 'PENDING'

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'ACTIVE': 'ACTIVE',
            'ACTIVO': 'ACTIVE',
            'INACTIVE': 'INACTIVE',
            'INACTIVO': 'INACTIVE',
            'EXPIRED': 'EXPIRED',
            'EXPIRADO': 'EXPIRED',
            'VENCIDO': 'EXPIRED',
            'DELETED': 'DELETED',
            'ELIMINADO': 'DELETED',
            'SUSPENDED': 'SUSPENDED',
            'SUSPENDIDO': 'SUSPENDED'
        }

        return status_mapping.get(status_upper, 'PENDING')

    def _map_reconsumption_status(self, status: str) -> str:
        if not status:
            return 'PENDING'

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'ACTIVE': 'ACTIVE',
            'ACTIVO': 'ACTIVE',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED'
        }

        return status_mapping.get(status_upper, 'PENDING')

    def _map_history_action(self, action: str) -> str:
        if not action:
            return 'STATUS_CHANGED'

        action_upper = action.upper().strip()

        action_mapping = {
            'CREATED': 'CREATED',
            'CREADO': 'CREATED',
            'RENEWED': 'RENEWED',
            'RENOVADO': 'RENEWED',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED',
            'REACTIVATED': 'REACTIVATED',
            'REACTIVADO': 'REACTIVATED',
            'EXPIRED': 'EXPIRED',
            'EXPIRADO': 'EXPIRED',
            'VENCIDO': 'EXPIRED',
            'STATUS_CHANGED': 'STATUS_CHANGED',
            'ESTADO_CAMBIADO': 'STATUS_CHANGED',
            'PAYMENT_RECEIVED': 'PAYMENT_RECEIVED',
            'PAGO_RECIBIDO': 'PAYMENT_RECEIVED',
            'PLAN_CHANGED': 'PLAN_CHANGED',
            'PLAN_CAMBIADO': 'PLAN_CHANGED',
            'RECONSUMPTION_ADDED': 'RECONSUMPTION_ADDED',
            'RECONSUMO_AGREGADO': 'RECONSUMPTION_ADDED',
            'PURCHASE': 'PURCHASE',
            'COMPRA': 'PURCHASE',
            'UPGRADE': 'UPGRADE',
            'MEJORA': 'UPGRADE'
        }

        return action_mapping.get(action_upper, 'STATUS_CHANGED')

    def _validate_decimal_field(self, value: Any, field_name: str, min_value: float = None, default_value: float = None) -> float:
        if value is None:
            if default_value is not None:
                return default_value
            raise ValueError(f"{field_name} es requerido")

        try:
            decimal_value = float(value)
        except (TypeError, ValueError):
            if default_value is not None:
                return default_value
            raise ValueError(f"{field_name} debe ser un número válido: {value}")

        if min_value is not None and decimal_value < min_value:
            if field_name == 'amount':
                raise ValueError("El monto no puede ser negativo")
            elif field_name == 'minimumReconsumptionAmount':
                raise ValueError("El monto mínimo de reconsumo no puede ser negativo")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return decimal_value

    def _clean_text_field(self, text: str, max_length: int = None) -> Optional[str]:
        if not text or not str(text).strip():
            return None

        cleaned = str(text).strip()

        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned if cleaned else None

    def _process_json_field(self, json_value: Any) -> Optional[Dict[str, Any]]:
        if not json_value:
            return None

        try:
            if isinstance(json_value, str):
                return json.loads(json_value)
            elif isinstance(json_value, dict):
                return json_value
            else:
                return None
        except (json.JSONDecodeError, TypeError):
            return None

    def _process_date(self, date_value: Any) -> Optional[date]:
        if date_value is None:
            return None

        if isinstance(date_value, date):
            return date_value

        if isinstance(date_value, datetime):
            return date_value.date()

        if isinstance(date_value, str):
            try:
                date_formats = [
                    '%Y-%m-%d',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%fZ',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                    '%d/%m/%Y',
                    '%Y/%m/%d'
                ]

                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_value, fmt)
                        return parsed_date.date()
                    except ValueError:
                        continue

                return None

            except Exception:
                return None

        return None

    def _process_datetime(self, dt_value: Any) -> Optional[datetime]:
        if dt_value is None:
            return None

        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            try:
                datetime_formats = [
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%fZ',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d'
                ]

                for fmt in datetime_formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue

                return None

            except Exception:
                return None

        return None

    def validate_transformation(self, memberships: List[Dict[str, Any]], 
                              reconsumptions: List[Dict[str, Any]], 
                              history: List[Dict[str, Any]]) -> Dict[str, Any]:
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            membership_ids = set()

            # Validar membresías
            for membership in memberships:
                # Validar ID único
                membership_id = membership['id']
                if membership_id in membership_ids:
                    validation_results['errors'].append(f"ID de membresía duplicado: {membership_id}")
                    validation_results['valid'] = False
                membership_ids.add(membership_id)

                # Validar campos obligatorios
                if not membership.get('user_email'):
                    validation_results['errors'].append(f"Membresía {membership_id}: email de usuario requerido")
                    validation_results['valid'] = False

                if not membership.get('plan_id'):
                    validation_results['errors'].append(f"Membresía {membership_id}: plan requerido")
                    validation_results['valid'] = False

                if not membership.get('start_date'):
                    validation_results['errors'].append(f"Membresía {membership_id}: fecha de inicio requerida")
                    validation_results['valid'] = False

                # Validar fechas
                start_date = membership.get('start_date')
                end_date = membership.get('end_date')
                if start_date and end_date and end_date < start_date:
                    validation_results['errors'].append(f"Membresía {membership_id}: fecha de fin anterior a fecha de inicio")
                    validation_results['valid'] = False

            # Validar reconsumptions
            for reconsumption in reconsumptions:
                if reconsumption['membership_id'] not in membership_ids:
                    validation_results['errors'].append(f"Reconsumption referencia membresía inexistente: {reconsumption['membership_id']}")
                    validation_results['valid'] = False

                if reconsumption.get('amount', 0) < 0:
                    validation_results['errors'].append(f"Reconsumption {reconsumption['id']}: monto negativo")
                    validation_results['valid'] = False

            # Validar history
            for history_item in history:
                if history_item['membership_id'] not in membership_ids:
                    validation_results['errors'].append(f"History item referencia membresía inexistente: {history_item['membership_id']}")
                    validation_results['valid'] = False

            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'memberships_transformed': self.stats['memberships_transformed'],
            'reconsumptions_transformed': self.stats['reconsumptions_transformed'],
            'history_transformed': self.stats['history_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }

    def close_connections(self):
        try:
            self.user_service.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexión del servicio de usuarios: {str(e)}")
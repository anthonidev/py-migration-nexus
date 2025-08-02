
import json
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentsTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.stats = {
            'payments_transformed': 0,
            'payment_items_transformed': 0,
            'errors': [],
            'warnings': [],
            'user_lookups': 0,
            'user_lookup_failures': 0
        }

    def transform_payments_data(self, payments_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

        logger.info(f"Iniciando transformación de {len(payments_data)} pagos")

        transformed_payments = []
        transformed_payment_items = []

        for payment in payments_data:
            try:
                transformed_payment, payment_items = self._transform_single_payment(
                    payment)
                transformed_payments.append(transformed_payment)
                transformed_payment_items.extend(payment_items)
                self.stats['payments_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando pago {payment.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(
            f"Transformación completada: {self.stats['payments_transformed']} pagos, {len(transformed_payment_items)} items")
        return transformed_payments, transformed_payment_items

    def _transform_single_payment(self, payment: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Transforma un pago individual"""

        payment_id = payment['id']

        # Obtener información del usuario
        user_info = self._get_user_info(payment['userEmail'])

        # Obtener información del revisor si existe
        reviewer_info = None
        if payment.get('reviewedByEmail'):
            reviewer_info = self._get_user_info(payment['reviewedByEmail'])

        # Procesar método de pago
        payment_method = self._map_payment_method(
            payment.get('paymentMethod', ''))

        # Procesar estado del pago
        payment_status = self._map_payment_status(payment.get('status', ''))

        # Procesar metadata
        metadata = self._process_metadata(payment.get('metadata'))

        # Extraer información bancaria del primer item si existe
        bank_name, operation_date = self._extract_bank_info_from_items(
            payment.get('items', []))

        # Crear pago transformado
        transformed_payment = {
            'id': payment_id,  # Conservar ID original
            'user_id': user_info['id'] if user_info else None,
            'user_email': user_info['email'] if user_info else payment['userEmail'],
            'user_name': user_info['fullName'] if user_info else None,
            'payment_config_id': payment['paymentConfigId'],
            'amount': float(payment['amount']) if payment['amount'] else 0.0,
            'status': payment_status,
            'payment_method': payment_method,
            'operation_code': self._clean_text_field(payment.get('operationCode'), uppercase=True),
            'bank_name': bank_name,
            'operation_date': operation_date,
            'ticket_number': self._clean_text_field(payment.get('ticketNumber')),
            'rejection_reason': self._clean_text_field(payment.get('rejectionReason'), max_length=500),
            'reviewed_by_id': reviewer_info['id'] if reviewer_info else None,
            'reviewed_by_email': reviewer_info['email'] if reviewer_info else payment.get('reviewedByEmail'),
            'reviewed_at': self._process_datetime(payment.get('reviewedAt')),
            'is_archived': bool(payment.get('isArchived', False)),
            'related_entity_type': payment.get('relatedEntityType'),
            'related_entity_id': payment.get('relatedEntityId'),
            'metadata': metadata,
            'external_reference': None,  # Campo nuevo, inicializar como None
            'gateway_transaction_id': None,  # Campo nuevo, inicializar como None
            'created_at': self._process_datetime(payment.get('createdAt')),
            'updated_at': self._process_datetime(payment.get('updatedAt'))
        }

        # Transformar items del pago
        payment_items = self._transform_payment_items(
            payment_id, payment.get('items', []), payment_method)

        return transformed_payment, payment_items

    def _transform_payment_items(self, payment_id: int, items_data: List[Dict[str, Any]], payment_method: str) -> List[Dict[str, Any]]:
        """Transforma los items de un pago"""

        if not items_data:
            return []

        transformed_items = []

        # Si items_data es un string JSON, parsearlo
        if isinstance(items_data, str):
            try:
                items_data = json.loads(items_data)
            except json.JSONDecodeError:
                logger.warning(
                    f"No se pudo parsear items JSON para pago {payment_id}")
                return []

        for item in items_data:
            try:
                original_item_id = item.get('id')
                if not original_item_id:
                    logger.warning(
                        f"Item del pago {payment_id} sin ID original, se omitirá")
                    continue

                item_type = self._determine_item_type(item, payment_method)

                url = item.get('url')
                if url and url.strip():
                    url = url.strip()
                else:
                    url = None

                url_key = None

                points_transaction_id = None
                if item.get('transactionReference'):
                    points_transaction_id = self._clean_text_field(
                        item['transactionReference'],
                        max_length=100,
                        uppercase=True
                    )

                transaction_date = self._process_datetime(
                    item.get('transactionDate'))

                has_image_data = url is not None
                has_transaction_data = points_transaction_id is not None

                if not has_image_data and not has_transaction_data:
                    warning = f"Item del pago {payment_id} sin imagen ni referencia de transacción, se omitirá"
                    logger.warning(warning)
                    self.stats['warnings'].append(warning)
                    continue

                transformed_item = {
                    'id': original_item_id,  # CONSERVAR ID ORIGINAL
                    'payment_id': payment_id,
                    'item_type': item_type,
                    'url': url,
                    'url_key': url_key,
                    'points_transaction_id': points_transaction_id,
                    'amount': float(item['amount']) if item.get('amount') else None,
                    'bank_name': self._clean_text_field(item.get('bankName'), max_length=100),
                    'transaction_date': transaction_date
                }

                transformed_items.append(transformed_item)
                self.stats['payment_items_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando item del pago {payment_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_items

    def _get_user_info(self, email: str) -> Optional[Dict[str, Any]]:
        if not email:
            return None

        try:
            self.stats['user_lookups'] += 1
            user_info = self.user_service.get_user_by_email(email)

            if not user_info:
                self.stats['user_lookup_failures'] += 1
                logger.warning(f"Usuario no encontrado: {email}")

            return user_info

        except Exception as e:
            self.stats['user_lookup_failures'] += 1
            logger.error(
                f"Error obteniendo información del usuario {email}: {str(e)}")
            return None

    def _map_payment_method(self, method: str) -> str:
        if not method:
            return 'VOUCHER'  # Valor por defecto

        method_upper = method.upper().strip()

        # Mapeo de métodos conocidos
        method_mapping = {
            'VOUCHER': 'VOUCHER',
            'POINTS': 'POINTS',
            'TRANSFERENCIA': 'BANK_TRANSFER',
            'BANK_TRANSFER': 'BANK_TRANSFER',
            'CARD': 'CARD',
            'TARJETA': 'CARD',
            'CASH': 'CASH',
            'EFECTIVO': 'CASH'
        }

        mapped_method = method_mapping.get(method_upper, 'VOUCHER')

        if method_upper not in method_mapping:
            warning = f"Método de pago desconocido '{method}', usando VOUCHER por defecto"
            logger.warning(warning)
            self.stats['warnings'].append(warning)

        return mapped_method

    def _map_payment_status(self, status: str) -> str:
        if not status:
            return 'PENDING'  

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'APPROVED': 'APPROVED',
            'APROBADO': 'APPROVED',
            'REJECTED': 'REJECTED',
            'RECHAZADO': 'REJECTED',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED',
            'PROCESSING': 'PROCESSING',
            'PROCESANDO': 'PROCESSING',
            'COMPLETED': 'COMPLETED',
        }

        mapped_status = status_mapping.get(status_upper, 'PENDING')

        if status_upper not in status_mapping:
            warning = f"Estado de pago desconocido '{status}', usando PENDING por defecto"
            logger.warning(warning)
            self.stats['warnings'].append(warning)

        return mapped_status

    def _determine_item_type(self, item_data: Dict[str, Any], payment_method: str) -> str:

        if (payment_method == 'POINTS' or
            (item_data.get('transactionReference') and
             'Puntos' in str(item_data.get('transactionReference', '')))):
            return 'POINTS_TRANSACTION'

        return 'VOUCHER_IMAGE'

    def _extract_bank_info_from_items(self, items_data: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[datetime]]:

        if not items_data:
            return None, None

        if isinstance(items_data, str):
            try:
                items_data = json.loads(items_data)
            except json.JSONDecodeError:
                return None, None

        if not items_data or not isinstance(items_data, list):
            return None, None

        first_item = items_data[0]

        bank_name = self._clean_text_field(first_item.get('bankName'))
        operation_date = self._process_datetime(
            first_item.get('transactionDate'))

        return bank_name, operation_date

    def _clean_text_field(self, text: str, max_length: int = None, uppercase: bool = False) -> Optional[str]:
        if not text or not str(text).strip():
            return None

        cleaned = str(text).strip()

        if uppercase:
            cleaned = cleaned.upper()

        if max_length and len(cleaned) > max_length:
            warning = f"Campo de texto excede {max_length} caracteres, será truncado: '{cleaned[:50]}...'"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            cleaned = cleaned[:max_length]

        return cleaned if cleaned else None

    def _process_metadata(self, metadata: Any) -> Optional[Dict[str, Any]]:
        if not metadata:
            return None

        try:
            if isinstance(metadata, str):
                return json.loads(metadata)
            elif isinstance(metadata, dict):
                return metadata
            else:
                return None
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Metadata inválido encontrado: {metadata}")
            return None

    def _process_datetime(self, dt_value: Any) -> Optional[datetime]:
        """Procesa campos de fecha/hora"""
        if dt_value is None:
            return None

        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            try:
                # Intentar parsear diferentes formatos
                datetime_formats = [
                    '%Y-%m-%d %H:%M:%S.%f',      # Con microsegundos
                    '%Y-%m-%d %H:%M:%S',         # Sin microsegundos
                    '%Y-%m-%dT%H:%M:%S.%fZ',     # ISO format con Z
                    '%Y-%m-%dT%H:%M:%S.%f',      # ISO format sin Z
                    '%Y-%m-%dT%H:%M:%S',         # ISO format básico
                    '%Y-%m-%d'                   # Solo fecha
                ]

                for fmt in datetime_formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue

                logger.warning(f"No se pudo parsear fecha/hora: {dt_value}")
                return None

            except Exception:
                return None

        return None

    def validate_transformation(self, payments: List[Dict[str, Any]], payment_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {
                'total_payments': len(payments),
                'total_payment_items': len(payment_items),
                'payments_with_users': 0,
                'payments_without_users': 0,
                'payments_by_status': {},
                'payments_by_method': {},
                'items_by_type': {}
            }
        }

        try:
            payment_ids = set()

            # Validar pagos
            for payment in payments:
                # Validar ID único
                payment_id = payment['id']
                if payment_id in payment_ids:
                    validation_results['errors'].append(
                        f"ID de pago duplicado: {payment_id}")
                    validation_results['valid'] = False
                payment_ids.add(payment_id)

                # Validar campos obligatorios
                if not payment.get('user_email'):
                    validation_results['errors'].append(
                        f"Pago {payment_id}: email de usuario requerido")
                    validation_results['valid'] = False

                if not payment.get('payment_config_id'):
                    validation_results['errors'].append(
                        f"Pago {payment_id}: configuración de pago requerida")
                    validation_results['valid'] = False

                if payment.get('amount', 0) <= 0:
                    validation_results['errors'].append(
                        f"Pago {payment_id}: monto debe ser mayor a 0")
                    validation_results['valid'] = False

                # Validar estado rechazado con razón
                if payment.get('status') == 'REJECTED' and not payment.get('rejection_reason'):
                    validation_results['warnings'].append(
                        f"Pago {payment_id}: rechazado sin razón")

                # Estadísticas
                if payment.get('user_id'):
                    validation_results['stats']['payments_with_users'] += 1
                else:
                    validation_results['stats']['payments_without_users'] += 1

                # Contar por estado
                status = payment.get('status', 'UNKNOWN')
                validation_results['stats']['payments_by_status'][status] = \
                    validation_results['stats']['payments_by_status'].get(
                        status, 0) + 1

                # Contar por método
                method = payment.get('payment_method', 'UNKNOWN')
                validation_results['stats']['payments_by_method'][method] = \
                    validation_results['stats']['payments_by_method'].get(
                        method, 0) + 1

            for item in payment_items:
                if item['payment_id'] not in payment_ids:
                    validation_results['errors'].append(
                        f"Item referencia pago inexistente: {item['payment_id']}"
                    )
                    validation_results['valid'] = False

                has_image = item.get('url') is not None
                has_transaction = item.get('points_transaction_id') is not None

                if not has_image and not has_transaction:
                    validation_results['warnings'].append(
                        f"Item del pago {item['payment_id']} sin imagen ni referencia de transacción"
                    )

                item_type = item.get('item_type', 'UNKNOWN')
                validation_results['stats']['items_by_type'][item_type] = \
                    validation_results['stats']['items_by_type'].get(
                        item_type, 0) + 1

            logger.info(
                f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(
                f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'payments_transformed': self.stats['payments_transformed'],
            'payment_items_transformed': self.stats['payment_items_transformed'],
            'user_lookups': self.stats['user_lookups'],
            'user_lookup_failures': self.stats['user_lookup_failures'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }

    def close_connections(self):
        try:
            self.user_service.close_connection()
        except Exception as e:
            logger.error(
                f"Error cerrando conexión del servicio de usuarios: {str(e)}")

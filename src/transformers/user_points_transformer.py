import json
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from src.shared.user_service import UserService
from src.shared.payment_service import PaymentService
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UserPointsTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.payment_service = PaymentService()
        self.stats = {
            'user_points_transformed': 0,
            'transactions_transformed': 0,
            'transaction_payments_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform_user_points_data(self, user_points_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma los datos de puntos de usuarios"""
        logger.info(f"Iniciando transformación de {len(user_points_data)} registros de puntos de usuarios")

        transformed_user_points = []
        transformed_transactions = []
        transformed_transaction_payments = []

        for user_point in user_points_data:
            try:
                transformed_user_point, transactions, transaction_payments = self._transform_single_user_point(user_point)
                transformed_user_points.append(transformed_user_point)
                transformed_transactions.extend(transactions)
                transformed_transaction_payments.extend(transaction_payments)
                self.stats['user_points_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando puntos de usuario {user_point.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(f"Transformación completada: {self.stats['user_points_transformed']} user_points, {len(transformed_transactions)} transactions, {len(transformed_transaction_payments)} transaction_payments")
        return transformed_user_points, transformed_transactions, transformed_transaction_payments

    def _transform_single_user_point(self, user_point: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma un registro individual de puntos de usuario"""
        user_point_id = user_point['id']

        # Obtener información del usuario
        user_info = self._get_user_info(user_point['userEmail'])

        # Validar y transformar montos
        available_points = self._validate_decimal_field(
            user_point.get('availablePoints'), 'availablePoints', min_value=0.0
        )
        total_earned_points = self._validate_decimal_field(
            user_point.get('totalEarnedPoints'), 'totalEarnedPoints', min_value=0.0
        )
        total_withdrawn_points = self._validate_decimal_field(
            user_point.get('totalWithdrawnPoints'), 'totalWithdrawnPoints', min_value=0.0
        )

        # Validar consistencia
        if total_withdrawn_points > total_earned_points:
            raise ValueError(f"UserPoint {user_point_id}: puntos retirados ({total_withdrawn_points}) mayores a ganados ({total_earned_points})")

        # Crear user_point transformado
        transformed_user_point = {
            'id': user_point_id,  # Conservar ID original
            'user_id': user_info['id'] if user_info else None,
            'user_email': user_info['email'] if user_info else user_point['userEmail'],
            'user_name': user_info['fullName'] if user_info else None,
            'available_points': available_points,
            'total_earned_points': total_earned_points,
            'total_withdrawn_points': total_withdrawn_points,
            'available_lot_points': 0.0,  # Campo nuevo, inicializar en 0
            'total_earned_lot_points': 0.0,  # Campo nuevo, inicializar en 0
            'total_withdrawn_lot_points': 0.0,  # Campo nuevo, inicializar en 0
            'metadata': {},  # Siempre objeto vacío
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }

        # Transformar transacciones
        transactions = self._transform_transactions(
            user_point_id, 
            user_point.get('transactions', []),
            user_info
        )

        # Obtener transaction_payments de todas las transacciones
        transaction_payments = []
        for transaction in transactions:
            payments = self._transform_transaction_payments(transaction)
            transaction_payments.extend(payments)

        return transformed_user_point, transactions, transaction_payments

    def _transform_transactions(self, user_point_id: int, transactions_data: Any, user_info: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforma las transacciones de puntos"""
        if not transactions_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(transactions_data, str):
            try:
                transactions_data = json.loads(transactions_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear transactions JSON para user_point {user_point_id}")
                return []

        if not isinstance(transactions_data, list):
            return []

        transformed_transactions = []

        for transaction in transactions_data:
            try:
                original_id = transaction.get('id')
                if not original_id:
                    logger.warning(f"Transaction de user_point {user_point_id} sin ID original, se omitirá")
                    continue

                # Mapear tipo de transacción
                transaction_type = self._map_transaction_type(transaction.get('type', ''))
                
                # Mapear estado de transacción
                status = self._map_transaction_status(transaction.get('status', ''))
               

                # Validar montos
                amount = self._validate_decimal_field(
                    transaction.get('amount'), 'amount', min_value=0.0
                )
                pending_amount = self._validate_decimal_field(
                    transaction.get('pendingAmount'), 'pendingAmount', 
                    min_value=0.0, default_value=0.0
                )
                withdrawn_amount = self._validate_decimal_field(
                    transaction.get('withdrawnAmount'), 'withdrawnAmount', 
                    min_value=0.0, default_value=0.0
                )

                # Validar consistencia
                if withdrawn_amount > amount:
                    raise ValueError(f"Transaction {original_id}: monto retirado ({withdrawn_amount}) mayor al monto total ({amount})")

                is_archived = bool(transaction.get('isArchived', False))
                
                # Procesar metadata
                metadata = self._process_json_field(transaction.get('metadata'))
                
                if amount == 0:
                    status = 'CANCELLED'

                transformed_transaction = {
                    'id': original_id,  # Conservar ID original
                    'user_id': user_info['id'] if user_info else None,
                    'user_email': user_info['email'] if user_info else '',
                    'user_name': user_info['fullName'] if user_info else None,
                    'type': transaction_type,
                    'amount': amount,
                    'pending_amount': pending_amount,
                    'withdrawn_amount': withdrawn_amount,
                    'status': status,
                    'is_archived': is_archived,
                    'metadata': metadata,
                    'created_at': self._process_datetime(transaction.get('createdAt')),
                    'updated_at': self._process_datetime(transaction.get('updatedAt')),
                    'payments_data': transaction.get('payments', [])  # Guardamos temporalmente para procesar después
                }

                transformed_transactions.append(transformed_transaction)
                self.stats['transactions_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando transaction de user_point {user_point_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_transactions

    def _transform_transaction_payments(self, transaction: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transforma los pagos de una transacción"""
        payments_data = transaction.get('payments_data', [])
        
        if not payments_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(payments_data, str):
            try:
                payments_data = json.loads(payments_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear payments JSON para transaction {transaction['id']}")
                return []

        if not isinstance(payments_data, list):
            return []

        # Obtener información de todos los pagos de una vez
        payment_ids = [payment.get('payment_id') for payment in payments_data if payment.get('payment_id')]
        payments_info = self.payment_service.get_payments_batch(payment_ids) if payment_ids else {}

        transformed_payments = []

        for payment in payments_data:
            try:
                payment_id = payment.get('payment_id')
                if not payment_id:
                    logger.warning(f"Payment de transaction {transaction['id']} sin payment_id, se omitirá")
                    continue

                # Obtener información del pago
                payment_info = payments_info.get(payment_id)

                # Generar ID único para el payment (ya que no existe en la estructura original)
                # Usaremos una combinación de transaction_id + payment_id
                payment_unique_id = int(f"{transaction['id']}{payment_id}")

                transformed_payment = {
                    'id': payment_unique_id,  # ID generado
                    'points_transaction_id': transaction['id'],
                    'payment_id': payment_id,
                    'amount': transaction['amount'],  # Usar amount de la transacción
                    'payment_reference': payment_info['operationCode'] if payment_info else None,
                    'payment_method': payment_info['paymentMethod'] if payment_info else None,
                    'notes': (
                        f"Pago {payment_info['paymentMethod']} - {payment_info['status']}" 
                        if payment_info 
                        else None
                    ),
                    'metadata': {},  # Siempre objeto vacío
                    'created_at': self._process_datetime(payment.get('createdAt')),
                    'updated_at': self._process_datetime(payment.get('updatedAt'))
                }

                transformed_payments.append(transformed_payment)
                self.stats['transaction_payments_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando payment de transaction {transaction['id']}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_payments

    def _get_user_info(self, email: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del usuario desde el servicio de usuarios"""
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

    def _map_transaction_type(self, transaction_type: str) -> str:
        """Mapea tipos de transacción"""
        if not transaction_type:
            return 'BINARY_COMMISSION'  # Default

        type_upper = transaction_type.upper().strip()

        type_mapping = {
            'BINARY_COMMISSION': 'BINARY_COMMISSION',
            'COMISION_BINARIA': 'BINARY_COMMISSION',
            'BINARY': 'BINARY_COMMISSION',
            'DIRECT_BONUS': 'DIRECT_BONUS',
            'BONO_DIRECTO': 'DIRECT_BONUS',
            'DIRECT': 'DIRECT_BONUS',
            'WITHDRAWAL': 'WITHDRAWAL',
            'RETIRO': 'WITHDRAWAL',
            'WITHDRAW': 'WITHDRAWAL'
        }

        return type_mapping.get(type_upper, 'BINARY_COMMISSION')

    def _map_transaction_status(self, status: str) -> str:
        """Mapea estados de transacción"""
        if not status:
            return 'PENDING'  # Default

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'COMPLETED': 'COMPLETED',
            'COMPLETADO': 'COMPLETED',
            'FINALIZADO': 'COMPLETED',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED',
            'FAILED': 'FAILED',
            'FALLIDO': 'FAILED',
            'ERROR': 'FAILED'
        }

        return status_mapping.get(status_upper, 'PENDING')

    def _validate_decimal_field(self, value: Any, field_name: str, min_value: float = None, default_value: float = None) -> float:
        """Valida campos decimales"""
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
            if field_name in ['availablePoints', 'totalEarnedPoints', 'totalWithdrawnPoints']:
                raise ValueError(f"Los puntos no pueden ser negativos: {field_name}")
            elif field_name == 'amount':
                raise ValueError("El monto no puede ser negativo")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return decimal_value

    def _process_json_field(self, json_value: Any) -> Dict[str, Any]:
        """Procesa campos JSON"""
        if not json_value:
            return {}

        try:
            if isinstance(json_value, str):
                return json.loads(json_value)
            elif isinstance(json_value, dict):
                return json_value
            else:
                return {}
        except (json.JSONDecodeError, TypeError):
            return {}

    def _process_datetime(self, dt_value: Any) -> Optional[datetime]:
        """Procesa campos de fecha/hora"""
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

    def validate_transformation(self, user_points: List[Dict[str, Any]], 
                              transactions: List[Dict[str, Any]], 
                              transaction_payments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida la transformación de datos"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            user_point_ids = set()
            transaction_ids = set()

            # Validar user_points
            for user_point in user_points:
                # Validar ID único
                user_point_id = user_point['id']
                if user_point_id in user_point_ids:
                    validation_results['errors'].append(f"ID de user_point duplicado: {user_point_id}")
                    validation_results['valid'] = False
                user_point_ids.add(user_point_id)

                # Validar campos obligatorios
                if not user_point.get('user_email'):
                    validation_results['errors'].append(f"UserPoint {user_point_id}: email de usuario requerido")
                    validation_results['valid'] = False

                # Validar rangos numéricos
                if user_point['available_points'] < 0:
                    validation_results['errors'].append(f"UserPoint {user_point_id}: puntos disponibles negativos")
                    validation_results['valid'] = False

                if user_point['total_earned_points'] < 0:
                    validation_results['errors'].append(f"UserPoint {user_point_id}: puntos ganados negativos")
                    validation_results['valid'] = False

                if user_point['total_withdrawn_points'] < 0:
                    validation_results['errors'].append(f"UserPoint {user_point_id}: puntos retirados negativos")
                    validation_results['valid'] = False

                # Validar consistencia
                if user_point['total_withdrawn_points'] > user_point['total_earned_points']:
                    validation_results['errors'].append(f"UserPoint {user_point_id}: puntos retirados mayores a ganados")
                    validation_results['valid'] = False

            # Validar transactions
            for transaction in transactions:
                # Validar ID único
                transaction_id = transaction['id']
                if transaction_id in transaction_ids:
                    validation_results['errors'].append(f"ID de transaction duplicado: {transaction_id}")
                    validation_results['valid'] = False
                transaction_ids.add(transaction_id)

                # Validar campos obligatorios
                if not transaction.get('user_email'):
                    validation_results['errors'].append(f"Transaction {transaction_id}: email de usuario requerido")
                    validation_results['valid'] = False

                # Validar rangos numéricos
                if transaction['amount'] < 0:
                    validation_results['errors'].append(f"Transaction {transaction_id}: monto negativo")
                    validation_results['valid'] = False

                if transaction['pending_amount'] < 0:
                    validation_results['errors'].append(f"Transaction {transaction_id}: monto pendiente negativo")
                    validation_results['valid'] = False

                if transaction['withdrawn_amount'] < 0:
                    validation_results['errors'].append(f"Transaction {transaction_id}: monto retirado negativo")
                    validation_results['valid'] = False

                # Validar consistencia
                if transaction['withdrawn_amount'] > transaction['amount']:
                    validation_results['errors'].append(f"Transaction {transaction_id}: monto retirado mayor al total")
                    validation_results['valid'] = False

            # Validar transaction_payments
            for payment in transaction_payments:
                if payment['points_transaction_id'] not in transaction_ids:
                    validation_results['errors'].append(f"Payment referencia transaction inexistente: {payment['points_transaction_id']}")
                    validation_results['valid'] = False

            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        """Retorna resumen de la transformación"""
        return {
            'user_points_transformed': self.stats['user_points_transformed'],
            'transactions_transformed': self.stats['transactions_transformed'],
            'transaction_payments_transformed': self.stats['transaction_payments_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }

    def close_connections(self):
        """Cierra las conexiones"""
        try:
            self.user_service.close_connection()
            self.payment_service.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexiones: {str(e)}")
            
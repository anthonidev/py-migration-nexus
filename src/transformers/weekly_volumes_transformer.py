import json
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date
from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)

class WeeklyVolumesTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.stats = {
            'weekly_volumes_transformed': 0,
            'history_transformed': 0,
            'errors': [],
            'warnings': []
        }
        self.users_cache = {}  # Cache para usuarios obtenidos

    def transform_weekly_volumes_data(self, weekly_volumes_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma los datos de volúmenes semanales"""
        logger.info(f"Iniciando transformación de {len(weekly_volumes_data)} volúmenes semanales")

        # 1. Obtener todos los emails únicos para consulta masiva
        unique_emails = list(set(volume.get('userEmail') for volume in weekly_volumes_data if volume.get('userEmail')))
        logger.info(f"Obteniendo información de {len(unique_emails)} usuarios únicos")
        
        # 2. Realizar consulta masiva de usuarios
        self.users_cache = self.user_service.get_users_batch(unique_emails)
        logger.info(f"Cache de usuarios cargado con {len(self.users_cache)} usuarios")

        transformed_volumes = []
        transformed_history = []

        for volume in weekly_volumes_data:
            try:
                transformed_volume, history_items = self._transform_single_weekly_volume(volume)
                transformed_volumes.append(transformed_volume)
                transformed_history.extend(history_items)
                self.stats['weekly_volumes_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando volumen semanal {volume.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(f"Transformación completada: {self.stats['weekly_volumes_transformed']} volúmenes, {len(transformed_history)} history")
        return transformed_volumes, transformed_history

    def _transform_single_weekly_volume(self, volume: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Transforma un volumen semanal individual"""
        volume_id = volume['id']

        # Obtener información del usuario desde el cache
        
        user_info = self._get_user_info_from_cache(volume['userEmail'])
        

        # Validar y transformar volúmenes
        left_volume = self._validate_decimal_field(
            volume.get('leftVolume'), 'leftVolume', min_value=0.0
        )
        right_volume = self._validate_decimal_field(
            volume.get('rightVolume'), 'rightVolume', min_value=0.0
        )
        
        # Commission earned puede ser null
        commission_earned = None
        if volume.get('commissionEarned') is not None:
            commission_earned = self._validate_decimal_field(
                volume.get('commissionEarned'), 'commissionEarned', min_value=0.0, allow_none=True
            )

        # Procesar fechas
        week_start_date = self._process_date(volume.get('weekStartDate'))
        week_end_date = self._process_date(volume.get('weekEndDate'))

        # Validar fechas
        if not week_start_date or not week_end_date:
            raise ValueError(f"Volumen {volume_id}: fechas de semana requeridas")
        
        if week_end_date <= week_start_date:
            raise ValueError(f"Volumen {volume_id}: fecha de fin debe ser posterior a fecha de inicio")

        # Procesar estado
        status = self._map_volume_status(volume.get('status', ''))

        # Procesar lado seleccionado
        selected_side = self._map_volume_side(volume.get('selectedSide'))

        # Procesar fecha de procesamiento
        processed_at = self._process_datetime(volume.get('processedAt'))

        # Crear volumen transformado
        transformed_volume = {
            'id': volume_id,  # Conservar ID original
            'user_id': user_info['id'] if user_info else None,
            'user_email': user_info['email'] if user_info else volume['userEmail'],
            'user_name': user_info['fullName'] if user_info else None,
            'left_volume': left_volume,
            'right_volume': right_volume,
            'commission_earned': commission_earned,
            'week_start_date': week_start_date,
            'week_end_date': week_end_date,
            'status': status,
            'selected_side': selected_side,
            'processed_at': processed_at,
            'metadata': {},  # Siempre objeto vacío según especificación
            'created_at': self._process_datetime(volume.get('createdAt')),
            'updated_at': self._process_datetime(volume.get('createdAt'))  # usar createdAt como updatedAt inicial
        }

        # Transformar history
        history_items = self._transform_volume_history(
            volume_id, 
            volume.get('history', [])
        )

        return transformed_volume, history_items

    def _transform_volume_history(self, weekly_volume_id: int, history_data: Any) -> List[Dict[str, Any]]:
        """Transforma el historial de volúmenes"""
        if not history_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(history_data, str):
            try:
                history_data = json.loads(history_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear history JSON para volumen {weekly_volume_id}")
                return []

        if not isinstance(history_data, list):
            return []

        transformed_history = []

        for history_item in history_data:
            try:
                original_id = history_item.get('id')
                # Nota: No necesitamos validar el ID original ya que generaremos uno nuevo

                # Validar volumen
                volume = self._validate_decimal_field(
                    history_item.get('volume'), 'volume', min_value=0.0
                )

                # Procesar lado del volumen
                volume_side = self._map_volume_side(history_item.get('volumeSide'))

                # Procesar payment_id
                payment_id = self._clean_text_field(history_item.get('payment_id'))

                transformed_history_item = {
                    # No incluir ID - se generará automáticamente como autoincremental
                    'weekly_volume_id': weekly_volume_id,
                    'payment_id': payment_id,
                    'volume_side': volume_side,
                    'volume': volume,
                    'metadata': {},  # Siempre objeto vacío según especificación
                    'created_at': self._process_datetime(history_item.get('createdAt')),
                    'updated_at': self._process_datetime(history_item.get('updatedAt'))
                }

                transformed_history.append(transformed_history_item)
                self.stats['history_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando history item de volumen {weekly_volume_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_history

    def _get_user_info_from_cache(self, email: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del usuario desde el cache cargado"""
        if not email:
            return None

        # Normalizar email para buscar en cache
        normalized_email = email.lower().strip()
        
        user_info = self.users_cache.get(normalized_email)
        
        if not user_info:
            logger.warning(f"Usuario no encontrado en cache: {email}")
        
        return user_info
        
    def _map_volume_status(self, status: str) -> str:
        """Mapea estados de volumen"""
        if not status:
            return 'PENDING'  # Default

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'PROCESSED': 'PROCESSED',
            'PROCESADO': 'PROCESSED',
            'FINALIZADO': 'PROCESSED',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED',
            'CANCELED': 'CANCELLED'
        }

        return status_mapping.get(status_upper, 'PENDING')

    def _map_volume_side(self, side: str) -> Optional[str]:
        """Mapea lados de volumen"""
        if not side:
            return None

        side_upper = side.upper().strip()

        side_mapping = {
            'LEFT': 'LEFT',
            'IZQUIERDO': 'LEFT',
            'IZQUIERDA': 'LEFT',
            'RIGHT': 'RIGHT',
            'DERECHO': 'RIGHT',
            'DERECHA': 'RIGHT'
        }

        return side_mapping.get(side_upper, None)

    def _validate_decimal_field(self, value: Any, field_name: str, min_value: float = None, allow_none: bool = False) -> float:
        """Valida campos decimales"""
        if value is None:
            if allow_none:
                return None
            raise ValueError(f"{field_name} es requerido")

        try:
            decimal_value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número válido: {value}")

        if min_value is not None and decimal_value < min_value:
            if field_name in ['leftVolume', 'rightVolume']:
                raise ValueError(f"El volumen no puede ser negativo: {field_name}")
            elif field_name == 'volume':
                raise ValueError("El volumen no puede ser negativo")
            elif field_name == 'commissionEarned':
                raise ValueError("La comisión ganada no puede ser negativa")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return decimal_value

    def _clean_text_field(self, text: str, max_length: int = None) -> Optional[str]:
        """Limpia campos de texto"""
        if not text or not str(text).strip():
            return None

        cleaned = str(text).strip()

        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned if cleaned else None

    def _process_date(self, date_value: Any) -> Optional[date]:
        """Procesa campos de fecha"""
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

    def validate_transformation(self, weekly_volumes: List[Dict[str, Any]], 
                              history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida la transformación de datos"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            volume_ids = set()

            # Validar volúmenes semanales
            for volume in weekly_volumes:
                # Validar ID único
                volume_id = volume['id']
                if volume_id in volume_ids:
                    validation_results['errors'].append(f"ID de volumen duplicado: {volume_id}")
                    validation_results['valid'] = False
                volume_ids.add(volume_id)

                # Validar campos obligatorios
                if not volume.get('user_email'):
                    validation_results['errors'].append(f"Volumen {volume_id}: email de usuario requerido")
                    validation_results['valid'] = False

                if not volume.get('week_start_date'):
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de inicio de semana requerida")
                    validation_results['valid'] = False

                if not volume.get('week_end_date'):
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de fin de semana requerida")
                    validation_results['valid'] = False

                # Validar rangos numéricos
                if volume['left_volume'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: volumen izquierdo negativo")
                    validation_results['valid'] = False

                if volume['right_volume'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: volumen derecho negativo")
                    validation_results['valid'] = False

                if volume.get('commission_earned') is not None and volume['commission_earned'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: comisión ganada negativa")
                    validation_results['valid'] = False

                # Validar fechas
                week_start = volume.get('week_start_date')
                week_end = volume.get('week_end_date')
                if week_start and week_end and week_end <= week_start:
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de fin anterior o igual a fecha de inicio")
                    validation_results['valid'] = False

            # Validar history
            for history_item in history:
                if history_item['weekly_volume_id'] not in volume_ids:
                    validation_results['errors'].append(f"History item referencia volumen inexistente: {history_item['weekly_volume_id']}")
                    validation_results['valid'] = False

                if history_item.get('volume', 0) < 0:
                    validation_results['errors'].append(f"History item con volumen negativo")
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
            'weekly_volumes_transformed': self.stats['weekly_volumes_transformed'],
            'history_transformed': self.stats['history_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings'],
            'users_cached': len(self.users_cache)  # Agregar estadística de cache
        }

    def close_connections(self):
        """Cierra las conexiones"""
        try:
            self.user_service.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexión del servicio de usuarios: {str(e)}")


    def _map_volume_status(self, status: str) -> str:
        """Mapea estados de volumen"""
        if not status:
            return 'PENDING'  # Default

        status_upper = status.upper().strip()

        status_mapping = {
            'PENDING': 'PENDING',
            'PENDIENTE': 'PENDING',
            'PROCESSED': 'PROCESSED',
            'PROCESADO': 'PROCESSED',
            'FINALIZADO': 'PROCESSED',
            'CANCELLED': 'CANCELLED',
            'CANCELADO': 'CANCELLED',
            'CANCELED': 'CANCELLED'
        }

        return status_mapping.get(status_upper, 'PENDING')

    def _map_volume_side(self, side: str) -> Optional[str]:
        """Mapea lados de volumen"""
        if not side:
            return None

        side_upper = side.upper().strip()

        side_mapping = {
            'LEFT': 'LEFT',
            'IZQUIERDO': 'LEFT',
            'IZQUIERDA': 'LEFT',
            'RIGHT': 'RIGHT',
            'DERECHO': 'RIGHT',
            'DERECHA': 'RIGHT'
        }

        return side_mapping.get(side_upper, None)

    def _validate_decimal_field(self, value: Any, field_name: str, min_value: float = None, allow_none: bool = False) -> float:
        """Valida campos decimales"""
        if value is None:
            if allow_none:
                return None
            raise ValueError(f"{field_name} es requerido")

        try:
            decimal_value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número válido: {value}")

        if min_value is not None and decimal_value < min_value:
            if field_name in ['leftVolume', 'rightVolume']:
                raise ValueError(f"El volumen no puede ser negativo: {field_name}")
            elif field_name == 'volume':
                raise ValueError("El volumen no puede ser negativo")
            elif field_name == 'commissionEarned':
                raise ValueError("La comisión ganada no puede ser negativa")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return decimal_value

    def _clean_text_field(self, text: str, max_length: int = None) -> Optional[str]:
        """Limpia campos de texto"""
        if not text or not str(text).strip():
            return None

        cleaned = str(text).strip()

        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]

        return cleaned if cleaned else None

    def _process_date(self, date_value: Any) -> Optional[date]:
        """Procesa campos de fecha"""
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

    def validate_transformation(self, weekly_volumes: List[Dict[str, Any]], 
                              history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida la transformación de datos"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            volume_ids = set()

            # Validar volúmenes semanales
            for volume in weekly_volumes:
                # Validar ID único
                volume_id = volume['id']
                if volume_id in volume_ids:
                    validation_results['errors'].append(f"ID de volumen duplicado: {volume_id}")
                    validation_results['valid'] = False
                volume_ids.add(volume_id)

                # Validar campos obligatorios
                if not volume.get('user_email'):
                    validation_results['errors'].append(f"Volumen {volume_id}: email de usuario requerido")
                    validation_results['valid'] = False

                if not volume.get('week_start_date'):
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de inicio de semana requerida")
                    validation_results['valid'] = False

                if not volume.get('week_end_date'):
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de fin de semana requerida")
                    validation_results['valid'] = False

                # Validar rangos numéricos
                if volume['left_volume'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: volumen izquierdo negativo")
                    validation_results['valid'] = False

                if volume['right_volume'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: volumen derecho negativo")
                    validation_results['valid'] = False

                if volume.get('commission_earned') is not None and volume['commission_earned'] < 0:
                    validation_results['errors'].append(f"Volumen {volume_id}: comisión ganada negativa")
                    validation_results['valid'] = False

                # Validar fechas
                week_start = volume.get('week_start_date')
                week_end = volume.get('week_end_date')
                if week_start and week_end and week_end <= week_start:
                    validation_results['errors'].append(f"Volumen {volume_id}: fecha de fin anterior o igual a fecha de inicio")
                    validation_results['valid'] = False

            # Validar history
            for history_item in history:
                if history_item['weekly_volume_id'] not in volume_ids:
                    validation_results['errors'].append(f"History item referencia volumen inexistente: {history_item['weekly_volume_id']}")
                    validation_results['valid'] = False

                if history_item.get('volume', 0) < 0:
                    validation_results['errors'].append(f"History item {history_item['id']}: volumen negativo")
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
            'weekly_volumes_transformed': self.stats['weekly_volumes_transformed'],
            'history_transformed': self.stats['history_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }

    def close_connections(self):
        """Cierra las conexiones"""
        try:
            self.user_service.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexión del servicio de usuarios: {str(e)}")
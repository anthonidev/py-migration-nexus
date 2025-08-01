
from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentConfigsTransformer:

    def __init__(self):
        self.stats = {
            'configs_transformed': 0,
            'errors': [],
            'warnings': [],
            'code_transformations': []
        }

    def transform_payment_configs(self, configs_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        logger.info(
            f"Iniciando transformación de {len(configs_data)} configuraciones de pago")

        transformed_configs = []

        for config in configs_data:
            try:
                transformed_config = self._transform_single_config(config)
                transformed_configs.append(transformed_config)
                self.stats['configs_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando configuración {config.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(
            f"Transformación completada: {self.stats['configs_transformed']} configuraciones exitosas")
        return transformed_configs

    def _transform_single_config(self, config: Dict[str, Any]) -> Dict[str, Any]:

        # Mantener el ID original (muy importante según requisitos)
        original_id = config['id']

        # Transformar y limpiar el código según las reglas de validación de la entidad
        original_code = config.get('code', '')
        transformed_code = self._transform_code(original_code)

        if original_code != transformed_code:
            self.stats['code_transformations'].append({
                'id': original_id,
                'original': original_code,
                'transformed': transformed_code
            })

        # Transformar y limpiar nombre
        name = self._clean_text_field(config.get('name', ''), 100, 'name')

        # Transformar y limpiar descripción
        description = self._clean_text_field(
            config.get('description', ''), 500, 'description')

        # Procesar isActive
        is_active = bool(config.get('isActive', True))

        # Procesar fechas
        created_at = self._process_datetime(config.get('createdAt'))
        updated_at = self._process_datetime(config.get('updatedAt'))

        transformed_config = {
            'id': original_id,  # Conservar ID original
            'code': transformed_code,
            'name': name,
            'description': description if description else None,  # NULL si está vacío
            'is_active': is_active,
            'created_at': created_at,
            'updated_at': updated_at
        }

        return transformed_config

    def _transform_code(self, code: str) -> str:
        if not code:
            raise ValueError("Código no puede estar vacío")

        # Aplicar transformaciones según @BeforeInsert/@BeforeUpdate
        # 1. Convertir a mayúsculas
        # 2. Quitar espacios al inicio y final
        # 3. Reemplazar espacios múltiples con guión bajo
        transformed = code.upper().strip()

        # Reemplazar espacios múltiples con un solo guión bajo
        import re
        transformed = re.sub(r'\s+', '_', transformed)

        # Validar longitud máxima (50 caracteres según la entidad)
        if len(transformed) > 50:
            warning = f"Código '{transformed}' excede 50 caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            transformed = transformed[:50]

        return transformed

    def _clean_text_field(self, text: str, max_length: int, field_name: str) -> str:
        """Limpia campos de texto según las reglas de la entidad"""
        if not text:
            return ''

        # Quitar espacios al inicio y final
        cleaned = text.strip()

        # Validar longitud máxima
        if len(cleaned) > max_length:
            warning = f"Campo '{field_name}' excede {max_length} caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            cleaned = cleaned[:max_length]

        return cleaned

    def _process_datetime(self, dt_value: Any) -> datetime:
        """Procesa campos de fecha/hora"""
        if dt_value is None:
            return datetime.utcnow()

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
                ]

                for fmt in datetime_formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue

                logger.warning(
                    f"No se pudo parsear fecha/hora: {dt_value}, usando fecha actual")
                return datetime.utcnow()

            except Exception:
                return datetime.utcnow()

        return datetime.utcnow()

    def validate_transformation(self, transformed_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida que la transformación sea correcta"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {
                'total_configs': len(transformed_configs),
                'unique_codes': 0,
                'unique_ids': 0
            }
        }

        try:
            codes = set()
            ids = set()

            for config in transformed_configs:
                # Validar ID único
                config_id = config['id']
                if config_id in ids:
                    validation_results['errors'].append(
                        f"ID duplicado: {config_id}")
                    validation_results['valid'] = False
                ids.add(config_id)

                # Validar código único
                code = config['code']
                if code in codes:
                    validation_results['errors'].append(
                        f"Código duplicado: {code}")
                    validation_results['valid'] = False
                codes.add(code)

                # Validar campos obligatorios
                if not code or not config['name']:
                    validation_results['errors'].append(
                        f"Configuración ID {config_id}: campos obligatorios vacíos"
                    )
                    validation_results['valid'] = False

                # Validar longitudes
                if len(code) > 50:
                    validation_results['errors'].append(
                        f"Código '{code}' excede 50 caracteres"
                    )
                    validation_results['valid'] = False

                if len(config['name']) > 100:
                    validation_results['errors'].append(
                        f"Nombre de configuración ID {config_id} excede 100 caracteres"
                    )
                    validation_results['valid'] = False

                if config.get('description') and len(config['description']) > 500:
                    validation_results['errors'].append(
                        f"Descripción de configuración ID {config_id} excede 500 caracteres"
                    )
                    validation_results['valid'] = False

            validation_results['stats']['unique_codes'] = len(codes)
            validation_results['stats']['unique_ids'] = len(ids)

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
            'configs_transformed': self.stats['configs_transformed'],
            'code_transformations': len(self.stats['code_transformations']),
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings'],
            'transformed_codes': self.stats['code_transformations']
        }

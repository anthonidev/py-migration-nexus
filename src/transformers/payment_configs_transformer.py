from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PaymentConfigsTransformer:

    def __init__(self):
        self.stats = {
            'configs_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform_payment_configs(self, configs_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"Iniciando transformación de {len(configs_data)} configuraciones de pago")

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

        logger.info(f"Transformación completada: {self.stats['configs_transformed']} configuraciones exitosas")
        return transformed_configs

    def _transform_single_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        original_id = config['id']
        
        # Transformar código a mayúsculas y reemplazar espacios
        transformed_code = self._transform_code(config.get('code', ''))
        
        name = self._clean_text_field(config.get('name', ''), 100, 'name')
        description = self._clean_text_field(config.get('description', ''), 500, 'description')
        is_active = bool(config.get('isActive', True))
        created_at = self._process_datetime(config.get('createdAt'))
        updated_at = self._process_datetime(config.get('updatedAt'))

        transformed_config = {
            'id': original_id,  # Conservar ID original
            'code': transformed_code,
            'name': name,
            'description': description if description else None,
            'is_active': is_active,
            'created_at': created_at,
            'updated_at': updated_at
        }

        return transformed_config

    def _transform_code(self, code: str) -> str:
        if not code:
            raise ValueError("Código no puede estar vacío")

        # Convertir a mayúsculas y reemplazar espacios con guiones bajos
        transformed = code.upper().strip()
        import re
        transformed = re.sub(r'\s+', '_', transformed)

        if len(transformed) > 50:
            warning = f"Código '{transformed}' excede 50 caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            transformed = transformed[:50]

        return transformed

    def _clean_text_field(self, text: str, max_length: int, field_name: str) -> str:
        if not text:
            return ''

        cleaned = text.strip()

        if len(cleaned) > max_length:
            warning = f"Campo '{field_name}' excede {max_length} caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            cleaned = cleaned[:max_length]

        return cleaned

    def _process_datetime(self, dt_value: Any) -> datetime:
        if dt_value is None:
            return datetime.utcnow()

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
                ]

                for fmt in datetime_formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue

                return datetime.utcnow()

            except Exception:
                return datetime.utcnow()

        return datetime.utcnow()

    def validate_transformation(self, transformed_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            codes = set()
            ids = set()

            for config in transformed_configs:
                # Validar ID único
                config_id = config['id']
                if config_id in ids:
                    validation_results['errors'].append(f"ID duplicado: {config_id}")
                    validation_results['valid'] = False
                ids.add(config_id)

                # Validar código único
                code = config['code']
                if code in codes:
                    validation_results['errors'].append(f"Código duplicado: {code}")
                    validation_results['valid'] = False
                codes.add(code)

                # Validar campos obligatorios
                if not code or not config['name']:
                    validation_results['errors'].append(
                        f"Configuración ID {config_id}: campos obligatorios vacíos"
                    )
                    validation_results['valid'] = False

            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'configs_transformed': self.stats['configs_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }
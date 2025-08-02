from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MembershipPlansTransformer:

    def __init__(self):
        self.stats = {
            'plans_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform_membership_plans(self, plans_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"Iniciando transformación de {len(plans_data)} planes de membresía")

        transformed_plans = []

        for plan in plans_data:
            try:
                transformed_plan = self._transform_single_plan(plan)
                transformed_plans.append(transformed_plan)
                self.stats['plans_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando plan {plan.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(f"Transformación completada: {self.stats['plans_transformed']} planes exitosos")
        return transformed_plans

    def _transform_single_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        original_id = plan['id']

        name = self._clean_name(plan.get('name', ''))

        price = self._validate_decimal_field(plan.get('price'), 'price', min_value=0.0)
        check_amount = self._validate_decimal_field(plan.get('checkAmount'), 'checkAmount', min_value=0.0)
        binary_points = self._validate_integer_field(plan.get('binaryPoints'), 'binaryPoints', min_value=0)
        commission_percentage = self._validate_decimal_field(
            plan.get('commissionPercentage'), 'commissionPercentage',
            min_value=0.0, max_value=100.0)

        direct_commission_amount = None
        if plan.get('directCommissionAmount') is not None:
            direct_commission_amount = self._validate_decimal_field(
                plan.get('directCommissionAmount'), 'directCommissionAmount',
                allow_none=True)

        products = self._clean_array_field(plan.get('products', []))
        benefits = self._clean_array_field(plan.get('benefits', []))

        is_active = bool(plan.get('isActive', True))
        display_order = int(plan.get('displayOrder', 0))

        created_at = self._process_datetime(plan.get('createdAt'))
        updated_at = self._process_datetime(plan.get('updatedAt'))

        transformed_plan = {
            'id': original_id,  # Conservar ID original
            'name': name,
            'price': price,
            'check_amount': check_amount,
            'binary_points': binary_points,
            'commission_percentage': commission_percentage,
            'direct_commission_amount': direct_commission_amount,
            'products': products,
            'benefits': benefits,
            'is_active': is_active,
            'display_order': display_order,
            'created_at': created_at,
            'updated_at': updated_at
        }

        return transformed_plan

    def _clean_name(self, name: str) -> str:
        if not name:
            raise ValueError("El nombre es requerido y no puede estar vacío")

        cleaned_name = name.strip()

        if not cleaned_name:
            raise ValueError("El nombre no puede estar vacío después del trim")

        if len(cleaned_name) > 100:
            warning = f"Nombre '{cleaned_name}' excede 100 caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            cleaned_name = cleaned_name[:100]

        return cleaned_name

    def _clean_array_field(self, array_field: List[str]) -> List[str]:
        if not array_field:
            return []

        if not isinstance(array_field, list):
            if isinstance(array_field, str):
                if array_field.startswith('{') and array_field.endswith('}'):
                    items = array_field[1:-1].split(',')
                    array_field = [item.strip() for item in items if item.strip()]
                else:
                    array_field = [array_field]
            else:
                return []

        cleaned_array = []
        for item in array_field:
            if item and str(item).strip():
                cleaned_item = str(item).strip()
                if cleaned_item:
                    cleaned_array.append(cleaned_item)

        return cleaned_array

    def _validate_decimal_field(self, value: Any, field_name: str,
                                min_value: float = None, max_value: float = None,
                                allow_none: bool = False) -> float:
        if value is None:
            if allow_none:
                return None
            raise ValueError(f"{field_name} es requerido")

        try:
            decimal_value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número válido: {value}")

        if min_value is not None and decimal_value < min_value:
            if field_name == 'price':
                raise ValueError("El precio no puede ser negativo")
            elif field_name == 'checkAmount':
                raise ValueError("El monto de cheque no puede ser negativo")
            elif field_name == 'commissionPercentage':
                raise ValueError("El porcentaje de comisión debe estar entre 0 y 100")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        if max_value is not None and decimal_value > max_value:
            if field_name == 'commissionPercentage':
                raise ValueError("El porcentaje de comisión debe estar entre 0 y 100")
            else:
                raise ValueError(f"{field_name} no puede ser mayor que {max_value}")

        return decimal_value

    def _validate_integer_field(self, value: Any, field_name: str, min_value: int = None) -> int:
        if value is None:
            raise ValueError(f"{field_name} es requerido")

        try:
            int_value = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número entero válido: {value}")

        if min_value is not None and int_value < min_value:
            if field_name == 'binaryPoints':
                raise ValueError("Los puntos binarios no pueden ser negativos")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return int_value

    def _process_datetime(self, dt_value: Any) -> datetime:
        """Procesa campos de fecha/hora"""
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

    def validate_transformation(self, transformed_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            ids = set()

            for plan in transformed_plans:
                # Validar ID único
                plan_id = plan['id']
                if plan_id in ids:
                    validation_results['errors'].append(f"ID duplicado: {plan_id}")
                    validation_results['valid'] = False
                ids.add(plan_id)

                # Validar campos obligatorios
                if not plan.get('name'):
                    validation_results['errors'].append(f"Plan {plan_id}: nombre requerido")
                    validation_results['valid'] = False

                # Validar rangos numéricos
                if plan['price'] < 0:
                    validation_results['errors'].append(f"Plan {plan_id}: precio negativo")
                    validation_results['valid'] = False

                if plan['check_amount'] < 0:
                    validation_results['errors'].append(f"Plan {plan_id}: checkAmount negativo")
                    validation_results['valid'] = False

                if plan['binary_points'] < 0:
                    validation_results['errors'].append(f"Plan {plan_id}: binaryPoints negativo")
                    validation_results['valid'] = False

                if not (0 <= plan['commission_percentage'] <= 100):
                    validation_results['errors'].append(f"Plan {plan_id}: commissionPercentage fuera de rango")
                    validation_results['valid'] = False

            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'plans_transformed': self.stats['plans_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }
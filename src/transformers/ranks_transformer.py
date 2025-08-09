# src/transformers/ranks_transformer.py
from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RanksTransformer:
    """Transformer para convertir datos JSON de ranks al formato de la entidad PostgreSQL"""

    def __init__(self):
        self.transformation_summary = {
            'total_processed': 0,
            'successful_transformations': 0,
            'errors': [],
            'warnings': []
        }

    def transform_ranks_data(self, ranks_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforma datos de ranks desde formato JSON al formato de entidad PostgreSQL"""
        logger.info(f"Iniciando transformación de {len(ranks_data)} ranks")

        transformed_ranks = []
        self.transformation_summary['total_processed'] = len(ranks_data)

        for i, rank_data in enumerate(ranks_data):
            try:
                transformed_rank = self._transform_single_rank(rank_data, i)
                if transformed_rank:
                    transformed_ranks.append(transformed_rank)
                    self.transformation_summary['successful_transformations'] += 1

            except Exception as e:
                error_msg = f"Error transformando rank en posición {i}: {str(e)}"
                logger.error(error_msg)
                self.transformation_summary['errors'].append(error_msg)

        logger.info(f"Transformación completada. {len(transformed_ranks)} ranks transformados exitosamente")
        return transformed_ranks

    def _transform_single_rank(self, rank_data: Dict[str, Any], index: int) -> Dict[str, Any]:
        """Transforma un solo rank del formato JSON al formato de entidad"""
        
        # Validar que tenemos los campos mínimos requeridos
        required_fields = ['id', 'name', 'code', 'rankOrder']
        for field in required_fields:
            if field not in rank_data:
                raise ValueError(f"Campo requerido faltante: {field}")

        # Mapeo directo de campos
        transformed = {
            'id': rank_data['id'],
            'name': rank_data['name'].strip(),
            'code': rank_data['code'].strip().upper(),
            'rank_order': rank_data['rankOrder'],
            'is_active': rank_data.get('isActive', True)
        }

        # Transformar campos de requisitos básicos
        # Mapear requiredPoints a required_pay_leg_qv si no existe requiredPayLegQv
        if 'requiredPayLegQv' in rank_data:
            transformed['required_pay_leg_qv'] = float(rank_data['requiredPayLegQv'])
        elif 'requiredPoints' in rank_data:
            transformed['required_pay_leg_qv'] = float(rank_data['requiredPoints'])
            self.transformation_summary['warnings'].append(
                f"Rank {rank_data.get('name', 'unknown')}: usando requiredPoints como required_pay_leg_qv"
            )
        else:
            transformed['required_pay_leg_qv'] = 0.0

        # required_total_tree_qv
        if 'requiredTotalTreeQv' in rank_data:
            transformed['required_total_tree_qv'] = float(rank_data['requiredTotalTreeQv'])
        else:
            # Si no existe, usar el doble de required_pay_leg_qv como valor por defecto
            transformed['required_total_tree_qv'] = transformed['required_pay_leg_qv'] * 2
            self.transformation_summary['warnings'].append(
                f"Rank {rank_data.get('name', 'unknown')}: required_total_tree_qv no especificado, usando {transformed['required_total_tree_qv']}"
            )

        # required_directs
        transformed['required_directs'] = int(rank_data.get('requiredDirects', 0))

        # Campos opcionales de restricciones de equipos
        transformed['required_active_teams'] = self._safe_int_or_none(rank_data.get('requiredActiveTeams'))
        transformed['required_qualified_teams'] = self._safe_int_or_none(rank_data.get('requiredQualifiedTeams'))
        transformed['required_qualified_rank_id'] = self._safe_int_or_none(rank_data.get('requiredQualifiedRankId'))

        # Campos opcionales de restricciones de árbol
        transformed['max_sponsorship_branch_qv'] = self._safe_float_or_none(rank_data.get('maxSponsorshipBranchQv'))
        transformed['max_leg_balance_percentage'] = self._safe_float_or_none(rank_data.get('maxLegBalancePercentage'))

        # Campo opcional de profundidad
        transformed['min_depth_levels'] = self._safe_int_or_none(rank_data.get('minDepthLevels'))

        # Beneficios (JSON)
        transformed['benefits'] = rank_data.get('benefits')

        # Descripción
        transformed['description'] = rank_data.get('description')

        # Timestamps - usar los existentes si están disponibles, sino usar el timestamp actual
        current_time = datetime.now()
        
        if 'createdAt' in rank_data and rank_data['createdAt']:
            try:
                # Intentar parsear el timestamp existente
                transformed['created_at'] = self._parse_timestamp(rank_data['createdAt'])
            except:
                transformed['created_at'] = current_time
                self.transformation_summary['warnings'].append(
                    f"Rank {rank_data.get('name', 'unknown')}: No se pudo parsear createdAt, usando timestamp actual"
                )
        else:
            transformed['created_at'] = current_time

        if 'updatedAt' in rank_data and rank_data['updatedAt']:
            try:
                transformed['updated_at'] = self._parse_timestamp(rank_data['updatedAt'])
            except:
                transformed['updated_at'] = current_time
                self.transformation_summary['warnings'].append(
                    f"Rank {rank_data.get('name', 'unknown')}: No se pudo parsear updatedAt, usando timestamp actual"
                )
        else:
            transformed['updated_at'] = current_time

        return transformed

    def _safe_int_or_none(self, value) -> int | None:
        """Convierte valor a int o retorna None si no es válido"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float_or_none(self, value) -> float | None:
        """Convierte valor a float o retorna None si no es válido"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parsea string de timestamp a objeto datetime"""
        # Formato esperado: "2025-04-14 23:48:42.506330"
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Intentar sin microsegundos
            try:
                return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Intentar formato ISO
                return datetime.fromisoformat(timestamp_str.replace('T', ' ').replace('Z', ''))

    def get_transformation_summary(self) -> Dict[str, Any]:
        """Retorna resumen de la transformación"""
        return {
            'total_processed': self.transformation_summary['total_processed'],
            'successful_transformations': self.transformation_summary['successful_transformations'],
            'errors': self.transformation_summary['errors'],
            'warnings': self.transformation_summary['warnings'],
            'total_errors': len(self.transformation_summary['errors'])
        }
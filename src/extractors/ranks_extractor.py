import json
import os
from typing import List, Dict, Any
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RanksExtractor:
    """Extractor para leer datos de ranks desde archivo JSON"""

    def __init__(self):
        self.json_file_path = self._get_json_file_path()

    def _get_json_file_path(self) -> str:
        """Obtiene la ruta completa del archivo ranks.json"""
        # Desde src/extractors/ranks_extractor.py subimos 3 niveles y vamos a src/data/ranks.json
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return os.path.join(base_dir, "src", "data", "ranks.json")

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida que el archivo JSON existe y tiene la estructura correcta"""
        logger.info("Validando archivo JSON de ranks")
        
        errors = []
        warnings = []

        try:
            # Verificar que el archivo existe
            if not os.path.exists(self.json_file_path):
                errors.append(f"Archivo JSON no encontrado: {self.json_file_path}")
                return {
                    'valid': False,
                    'errors': errors,
                    'warnings': warnings
                }

            # Verificar que se puede leer el archivo
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Verificar que es una lista
            if not isinstance(data, list):
                errors.append("El archivo JSON debe contener una lista de ranks")
                return {
                    'valid': False,
                    'errors': errors,
                    'warnings': warnings
                }

            # Verificar que no está vacío
            if len(data) == 0:
                errors.append("El archivo JSON está vacío")
                return {
                    'valid': False,
                    'errors': errors,
                    'warnings': warnings
                }

            # Validar estructura de cada rank
            required_fields = [
                'id', 'name', 'code', 'requiredDirects', 'isActive', 
                'rankOrder', 'requiredPayLegQv', 'requiredTotalTreeQv'
            ]

            for i, rank in enumerate(data):
                if not isinstance(rank, dict):
                    errors.append(f"Rank en posición {i} no es un objeto válido")
                    continue

                # Verificar campos requeridos
                for field in required_fields:
                    if field not in rank:
                        errors.append(f"Rank en posición {i} no tiene el campo requerido: {field}")

                # Validar tipos de datos básicos
                if 'id' in rank and not isinstance(rank['id'], int):
                    errors.append(f"Rank en posición {i}: 'id' debe ser un entero")

                if 'name' in rank and not isinstance(rank['name'], str):
                    errors.append(f"Rank en posición {i}: 'name' debe ser una cadena")

                if 'code' in rank and not isinstance(rank['code'], str):
                    errors.append(f"Rank en posición {i}: 'code' debe ser una cadena")

                if 'isActive' in rank and not isinstance(rank['isActive'], bool):
                    errors.append(f"Rank en posición {i}: 'isActive' debe ser un booleano")

            # Verificar IDs únicos
            ids = [rank.get('id') for rank in data if 'id' in rank]
            if len(ids) != len(set(ids)):
                errors.append("Hay IDs duplicados en los ranks")

            # Verificar códigos únicos
            codes = [rank.get('code') for rank in data if 'code' in rank]
            if len(codes) != len(set(codes)):
                errors.append("Hay códigos duplicados en los ranks")

            # Verificar rankOrder únicos
            rank_orders = [rank.get('rankOrder') for rank in data if 'rankOrder' in rank]
            if len(rank_orders) != len(set(rank_orders)):
                errors.append("Hay rankOrder duplicados en los ranks")

            logger.info(f"Validación completada. Encontrados {len(data)} ranks en el archivo JSON")

            if len(errors) > 0:
                logger.warning(f"Encontrados {len(errors)} errores de validación")

            if len(warnings) > 0:
                logger.warning(f"Encontradas {len(warnings)} advertencias")

            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'total_ranks': len(data)
            }

        except json.JSONDecodeError as e:
            errors.append(f"Error de formato JSON: {str(e)}")
            return {
                'valid': False,
                'errors': errors,
                'warnings': warnings
            }
        except Exception as e:
            errors.append(f"Error inesperado validando archivo JSON: {str(e)}")
            return {
                'valid': False,
                'errors': errors,
                'warnings': warnings
            }

    def extract_ranks_data(self) -> List[Dict[str, Any]]:
        """Extrae los datos de ranks desde el archivo JSON"""
        logger.info(f"Extrayendo datos de ranks desde: {self.json_file_path}")

        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                ranks_data = json.load(file)

            logger.info(f"Extraídos {len(ranks_data)} ranks desde el archivo JSON")
            return ranks_data

        except Exception as e:
            logger.error(f"Error extrayendo datos de ranks: {str(e)}")
            raise

    def close_connection(self):
        """Método para mantener consistencia con otros extractors (no hace nada en este caso)"""
        pass
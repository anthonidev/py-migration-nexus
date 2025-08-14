from typing import List, Dict, Any
import json
import os

from src.utils.logger import get_logger

logger = get_logger(__name__)


class RolesViewsExtractor:

    def __init__(self):
        # Ruta al archivo JSON con roles y vistas
        self.json_file_path = self._get_json_file_path()

    def _get_json_file_path(self) -> str:
        """Obtiene la ruta completa del archivo views_roles.json"""
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return os.path.join(base_dir, "src", "data", "views_roles.json")

    def _read_json(self) -> List[Dict[str, Any]]:
        """Lee y devuelve el contenido del JSON"""
        with open(self.json_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def extract_roles_and_views(self) -> List[Dict[str, Any]]:
        """Extrae roles con sus vistas desde el archivo JSON"""
        logger.info(f"Extrayendo roles y vistas desde JSON: {self.json_file_path}")
        try:
            data = self._read_json()
            logger.info(f"Extraídos {len(data)} roles desde archivo JSON")
            return data
        except Exception as e:
            logger.error(f"Error extrayendo roles y vistas desde JSON: {str(e)}")
            raise

    def extract_all_views(self) -> List[Dict[str, Any]]:
        """Aplana y deduplica todas las vistas presentes en los roles del JSON"""
        logger.info("Extrayendo todas las vistas desde JSON (aplanando por roles)")
        try:
            data = self._read_json()
            views_by_id: Dict[Any, Dict[str, Any]] = {}
            for role in data:
                role_views = role.get('views') or []
                for view in role_views:
                    v_id = view.get('id')
                    if v_id is not None and v_id not in views_by_id:
                        views_by_id[v_id] = view

            views_list = sorted(views_by_id.values(), key=lambda v: (v.get('order', 0), v.get('id', 0)))
            logger.info(f"Extraídas {len(views_list)} vistas únicas desde archivo JSON")
            return views_list
        except Exception as e:
            logger.error(f"Error extrayendo vistas desde JSON: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida que el JSON exista y tenga estructura/campos mínimos"""
        logger.info("Validando datos de origen desde JSON")

        errors: List[str] = []
        warnings: List[str] = []

        # Verificar existencia del archivo
        if not os.path.exists(self.json_file_path):
            errors.append(f"Archivo JSON no encontrado: {self.json_file_path}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}

        try:
            data = self._read_json()

            if not isinstance(data, list):
                errors.append("El archivo JSON debe contener una lista de roles")
                return {'valid': False, 'errors': errors, 'warnings': warnings}

            if len(data) == 0:
                errors.append("El archivo JSON está vacío")
                return {'valid': False, 'errors': errors, 'warnings': warnings}

            # Validar roles
            required_role_fields = ['id', 'code', 'name', 'isActive', 'createdAt', 'updatedAt', 'views']
            role_codes = []
            role_ids = []

            for i, role in enumerate(data):
                if not isinstance(role, dict):
                    errors.append(f"Rol en posición {i} no es un objeto válido")
                    continue

                for field in required_role_fields:
                    if field not in role:
                        errors.append(f"Rol en posición {i} no tiene el campo requerido: {field}")

                if 'id' in role and not isinstance(role['id'], int):
                    errors.append(f"Rol en posición {i}: 'id' debe ser entero")
                if 'code' in role and not isinstance(role['code'], str):
                    errors.append(f"Rol en posición {i}: 'code' debe ser cadena")
                if 'name' in role and not isinstance(role['name'], str):
                    errors.append(f"Rol en posición {i}: 'name' debe ser cadena")
                if 'isActive' in role and not isinstance(role['isActive'], bool):
                    errors.append(f"Rol en posición {i}: 'isActive' debe ser booleano")

                role_ids.append(role.get('id'))
                role_codes.append(role.get('code'))

            # Duplicados en roles
            if len([r for r in role_ids if r is not None]) != len(set([r for r in role_ids if r is not None])):
                errors.append("Hay IDs duplicados en los roles")
            if len([c for c in role_codes if c]) != len(set([c for c in role_codes if c])):
                errors.append("Hay códigos duplicados en los roles")

            # Validar vistas agregadas dentro de roles
            required_view_fields = ['id', 'code', 'name', 'isActive', 'order', 'createdAt', 'updatedAt']
            view_ids = []
            view_codes = []

            for i, role in enumerate(data):
                for j, view in enumerate(role.get('views') or []):
                    if not isinstance(view, dict):
                        errors.append(f"Vista en rol idx {i} posición {j} no es un objeto válido")
                        continue

                    for vf in required_view_fields:
                        if vf not in view:
                            errors.append(f"Vista en rol idx {i} posición {j} sin campo requerido: {vf}")

                    if 'id' in view and not isinstance(view['id'], int):
                        errors.append(f"Vista en rol idx {i} posición {j}: 'id' debe ser entero")
                    if 'code' in view and not isinstance(view['code'], str):
                        errors.append(f"Vista en rol idx {i} posición {j}: 'code' debe ser cadena")
                    if 'name' in view and not isinstance(view['name'], str):
                        errors.append(f"Vista en rol idx {i} posición {j}: 'name' debe ser cadena")
                    if 'order' in view and not isinstance(view['order'], int):
                        warnings.append(f"Vista en rol idx {i} posición {j}: 'order' debería ser entero")

                    view_ids.append(view.get('id'))
                    view_codes.append(view.get('code'))

            if len([v for v in view_ids if v is not None]) != len(set([v for v in view_ids if v is not None])):
                warnings.append("Hay IDs duplicados en vistas entre roles; se deduplicarán al aplanar")
            if len([c for c in view_codes if c]) != len(set([c for c in view_codes if c])):
                warnings.append("Hay códigos duplicados en vistas entre roles; verificar intencionalidad")

            logger.info("Validación del JSON de roles y vistas completada")
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'total_roles': len(data)
            }

        except json.JSONDecodeError as e:
            errors.append(f"Error de formato JSON: {str(e)}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        except Exception as e:
            errors.append(f"Error inesperado validando archivo JSON: {str(e)}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}

    def close_connection(self):
        """Compatibilidad con la interfaz anterior (no se requiere conexión)"""
        pass
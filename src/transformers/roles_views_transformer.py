"""
Transformador de datos de roles y vistas para MongoDB
"""
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
from bson import ObjectId
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RolesViewsTransformer:
    """Transformador de datos de roles y vistas para MongoDB"""

    def __init__(self):
        self.stats = {
            'views_transformed': 0,
            'roles_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform_views_data(self, views_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
        """
        Transforma los datos de vistas para MongoDB

        Returns:
            Tuple con las vistas transformadas y un mapeo de ID original -> ObjectId
        """
        logger.info(f"Iniciando transformación de {len(views_data)} vistas")

        transformed_views = []
        view_id_mapping = {}  # ID original -> ObjectId string

        # Primer paso: crear ObjectIds para todas las vistas
        for view in views_data:
            old_id = view['id']
            new_id = str(ObjectId())
            view_id_mapping[old_id] = new_id

        # Segundo paso: transformar cada vista
        for view in views_data:
            try:
                transformed_view = self._transform_single_view(
                    view, view_id_mapping)
                transformed_views.append(transformed_view)
                self.stats['views_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando vista {view.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(
            f"Transformación de vistas completada: {self.stats['views_transformed']} exitosas")
        return transformed_views, view_id_mapping

    def _transform_single_view(self, view: Dict[str, Any], view_id_mapping: Dict[int, str]) -> Dict[str, Any]:
        """Transforma una vista individual"""

        # Generar nuevo ObjectId para esta vista
        old_id = view['id']
        new_id = view_id_mapping[old_id]

        # Procesar metadata
        metadata = self._process_metadata(view.get('metadata'))

        # Procesar parentId
        parent_id = None
        if view.get('parentId'):
            parent_id = view_id_mapping.get(view['parentId'])
            if not parent_id:
                warning = f"Vista {old_id}: ParentId {view['parentId']} no encontrado en mapeo"
                logger.warning(warning)
                self.stats['warnings'].append(warning)

        # Crear vista transformada
        transformed_view = {
            '_id': ObjectId(new_id),
            'code': view['code'].upper() if view['code'] else '',
            'name': view['name'] or '',
            'icon': view.get('icon') or '',
            'url': view.get('url') or '',
            'isActive': bool(view.get('isActive', True)),
            'order': int(view.get('order', 0)),
            'metadata': metadata,
            'parent': ObjectId(parent_id) if parent_id else None,
            'children': [],  # Se llenará después
            'roles': [],     # Se llenará después
            'createdAt': self._process_datetime(view.get('createdAt')),
            'updatedAt': self._process_datetime(view.get('updatedAt'))
        }

        return transformed_view

    def transform_roles_data(self, roles_data: List[Dict[str, Any]], view_id_mapping: Dict[int, str]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
        """
        Transforma los datos de roles para MongoDB

        Args:
            roles_data: Lista de roles con sus vistas
            view_id_mapping: Mapeo de IDs de vista original -> ObjectId

        Returns:
            Tuple con los roles transformados y un mapeo de ID original -> ObjectId
        """
        logger.info(f"Iniciando transformación de {len(roles_data)} roles")

        transformed_roles = []
        role_id_mapping = {}  # ID original -> ObjectId string

        # Primer paso: crear ObjectIds para todos los roles
        for role in roles_data:
            old_id = role['id']
            new_id = str(ObjectId())
            role_id_mapping[old_id] = new_id

        # Segundo paso: transformar cada rol
        for role in roles_data:
            try:
                transformed_role = self._transform_single_role(
                    role, view_id_mapping, role_id_mapping)
                transformed_roles.append(transformed_role)
                self.stats['roles_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando rol {role.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(
            f"Transformación de roles completada: {self.stats['roles_transformed']} exitosos")
        return transformed_roles, role_id_mapping

    def _transform_single_role(self, role: Dict[str, Any], view_id_mapping: Dict[int, str], role_id_mapping: Dict[int, str]) -> Dict[str, Any]:
        """Transforma un rol individual"""

        old_id = role['id']
        new_id = role_id_mapping[old_id]

        # Procesar vistas asociadas
        view_object_ids = []
        if role.get('views'):
            try:
                # Las vistas vienen como JSON string
                views_list = role['views'] if isinstance(
                    role['views'], list) else json.loads(role['views'])

                for view in views_list:
                    view_old_id = view.get('id')
                    if view_old_id and view_old_id in view_id_mapping:
                        view_object_ids.append(
                            ObjectId(view_id_mapping[view_old_id]))
                    else:
                        warning = f"Rol {old_id}: Vista {view_old_id} no encontrada en mapeo"
                        logger.warning(warning)
                        self.stats['warnings'].append(warning)

            except (json.JSONDecodeError, TypeError) as e:
                error_msg = f"Error procesando vistas del rol {old_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        # Crear rol transformado
        transformed_role = {
            '_id': ObjectId(new_id),
            'code': role['code'].upper() if role['code'] else '',
            'name': role['name'] or '',
            'isActive': bool(role.get('isActive', True)),
            'views': view_object_ids,
            'createdAt': self._process_datetime(role.get('createdAt')),
            'updatedAt': self._process_datetime(role.get('updatedAt'))
        }

        return transformed_role

    def update_views_with_roles(self, views: List[Dict[str, Any]], roles_data: List[Dict[str, Any]],
                                view_id_mapping: Dict[int, str], role_id_mapping: Dict[int, str]):
        """Actualiza las vistas con las referencias a roles y relaciones padre-hijo"""
        logger.info("Actualizando relaciones en vistas")

        # Crear mapeo de ObjectId -> vista para búsqueda rápida
        view_obj_mapping = {str(view['_id']): view for view in views}

        # Actualizar relaciones padre-hijo
        for view in views:
            if view.get('parent'):
                parent_obj_id = str(view['parent'])
                if parent_obj_id in view_obj_mapping:
                    parent_view = view_obj_mapping[parent_obj_id]
                    if view['_id'] not in parent_view['children']:
                        parent_view['children'].append(view['_id'])

        # Actualizar referencias a roles
        for role_data in roles_data:
            if role_data.get('views'):
                try:
                    views_list = role_data['views'] if isinstance(
                        role_data['views'], list) else json.loads(role_data['views'])
                    role_old_id = role_data['id']
                    role_new_id = role_id_mapping.get(role_old_id)

                    if role_new_id:
                        role_object_id = ObjectId(role_new_id)

                        for view_data in views_list:
                            view_old_id = view_data.get('id')
                            view_new_id = view_id_mapping.get(view_old_id)

                            if view_new_id and view_new_id in view_obj_mapping:
                                view = view_obj_mapping[view_new_id]
                                if role_object_id not in view['roles']:
                                    view['roles'].append(role_object_id)

                except Exception as e:
                    error_msg = f"Error actualizando relaciones del rol {role_old_id}: {str(e)}"
                    logger.error(error_msg)
                    self.stats['errors'].append(error_msg)

        logger.info("Actualización de relaciones completada")

    def _process_metadata(self, metadata: Any) -> Dict[str, Any]:
        """Procesa y valida el campo metadata"""
        if not metadata:
            return {}

        try:
            if isinstance(metadata, str):
                return json.loads(metadata)
            elif isinstance(metadata, dict):
                return metadata
            else:
                return {}
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Metadata inválido encontrado: {metadata}")
            return {}

    def _process_datetime(self, dt_value: Any) -> datetime:
        """Procesa campos de fecha/hora"""
        if dt_value is None:
            return datetime.utcnow()

        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            try:
                # Intentar parsear diferentes formatos
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%fZ']:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue

                # Si no funciona ningún formato, usar valor actual
                logger.warning(
                    f"No se pudo parsear fecha: {dt_value}, usando fecha actual")
                return datetime.utcnow()

            except Exception:
                return datetime.utcnow()

        return datetime.utcnow()

    def get_transformation_summary(self) -> Dict[str, Any]:
        """Obtiene un resumen de la transformación"""
        return {
            'views_transformed': self.stats['views_transformed'],
            'roles_transformed': self.stats['roles_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }

    def validate_transformation(self, views: List[Dict[str, Any]], roles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida que la transformación sea correcta"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {
                'views_count': len(views),
                'roles_count': len(roles),
                'views_with_roles': 0,
                'roles_with_views': 0,
                'parent_child_relationships': 0
            }
        }

        # Validar vistas
        view_codes = set()
        for view in views:
            # Validar código único
            if view['code'] in view_codes:
                validation_results['errors'].append(
                    f"Código de vista duplicado: {view['code']}")
                validation_results['valid'] = False
            view_codes.add(view['code'])

            # Contar vistas con roles
            if view['roles']:
                validation_results['stats']['views_with_roles'] += 1

            # Contar relaciones padre-hijo
            if view['children']:
                validation_results['stats']['parent_child_relationships'] += len(
                    view['children'])

        # Validar roles
        role_codes = set()
        for role in roles:
            # Validar código único
            if role['code'] in role_codes:
                validation_results['errors'].append(
                    f"Código de rol duplicado: {role['code']}")
                validation_results['valid'] = False
            role_codes.add(role['code'])

            # Contar roles con vistas
            if role['views']:
                validation_results['stats']['roles_with_views'] += 1

        logger.info(
            f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
        return validation_results

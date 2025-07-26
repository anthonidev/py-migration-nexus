import pymongo
from typing import List, Dict, Any, Tuple
from src.utils.logger import get_logger

logger = get_logger(__name__)

class RolesViewsTransformer:
    """Transformador de datos de roles y vistas para MongoDB"""
    
    def __init__(self):
        self.view_id_mapping = {}  # PostgreSQL ID -> MongoDB ObjectId
        self.role_id_mapping = {}  # PostgreSQL ID -> MongoDB ObjectId
    
    def transform_views_data(self, views_data: List[Dict]) -> Tuple[List[Dict], Dict[int, pymongo.collection.ObjectId]]:
        """
        Transforma datos de vistas para MongoDB
        
        Args:
            views_data: Lista de vistas desde PostgreSQL
            
        Returns:
            Tupla con (vistas transformadas, mapeo de IDs)
        """
        transformed_views = []
        self.view_id_mapping = {}
        
        logger.info(f"Transformando {len(views_data)} vistas")
        
        # Primera pasada: crear ObjectIds y estructura básica
        for view in views_data:
            # Crear nuevo ObjectId para MongoDB
            mongo_view_id = pymongo.collection.ObjectId()
            self.view_id_mapping[view['id']] = mongo_view_id
            
            transformed_view = {
                '_id': mongo_view_id,
                'code': view['code'].upper() if view['code'] else '',
                'name': view['name'],
                'icon': view['icon'],
                'url': view['url'],
                'isActive': view['isActive'],
                'order': view['order'],
                'metadata': view['metadata'] if view['metadata'] else {},
                'parent': None,  # Se actualizará en segunda pasada
                'children': [],
                'roles': [],  # Se actualizará después
                'createdAt': view['createdAt'],
                'updatedAt': view['updatedAt']
            }
            
            transformed_views.append(transformed_view)
        
        # Segunda pasada: mapear referencias parent
        for i, view in enumerate(views_data):
            if view['parentId']:
                parent_mongo_id = self.view_id_mapping.get(view['parentId'])
                if parent_mongo_id:
                    transformed_views[i]['parent'] = parent_mongo_id
                    logger.debug(f"Vista {view['code']} tiene padre {view['parentId']}")
        
        # Tercera pasada: establecer relaciones children
        self._establish_parent_child_relationships(transformed_views, views_data)
        
        logger.info(f"Vistas transformadas exitosamente. Mapeo de {len(self.view_id_mapping)} IDs creado")
        return transformed_views, self.view_id_mapping
    
    def transform_roles_data(self, roles_data: List[Dict], view_id_mapping: Dict) -> Tuple[List[Dict], Dict[int, pymongo.collection.ObjectId]]:
        """
        Transforma datos de roles para MongoDB
        
        Args:
            roles_data: Lista de roles desde PostgreSQL
            view_id_mapping: Mapeo de IDs de vistas PostgreSQL -> MongoDB
            
        Returns:
            Tupla con (roles transformados, mapeo de IDs)
        """
        transformed_roles = []
        self.role_id_mapping = {}
        
        logger.info(f"Transformando {len(roles_data)} roles")
        
        for role in roles_data:
            # Crear nuevo ObjectId para MongoDB
            mongo_role_id = pymongo.collection.ObjectId()
            self.role_id_mapping[role['id']] = mongo_role_id
            
            # Mapear view IDs a MongoDB ObjectIds
            view_object_ids = []
            for view in role['views']:
                if view['id'] in view_id_mapping:
                    view_object_ids.append(view_id_mapping[view['id']])
                else:
                    logger.warning(f"Vista con ID {view['id']} no encontrada en mapeo")
            
            transformed_role = {
                '_id': mongo_role_id,
                'code': role['code'].upper() if role['code'] else '',
                'name': role['name'],
                'isActive': role['isActive'],
                'views': view_object_ids,
                'createdAt': role['createdAt'],
                'updatedAt': role['updatedAt']
            }
            
            transformed_roles.append(transformed_role)
            logger.debug(f"Rol {role['code']} transformado con {len(view_object_ids)} vistas")
        
        logger.info(f"Roles transformados exitosamente. Mapeo de {len(self.role_id_mapping)} IDs creado")
        return transformed_roles, self.role_id_mapping
    
    def update_views_with_roles(self, transformed_views: List[Dict], roles_data: List[Dict], 
                               view_id_mapping: Dict, role_id_mapping: Dict):
        """
        Actualiza vistas con referencias a los roles que las contienen
        
        Args:
            transformed_views: Vistas ya transformadas
            roles_data: Datos originales de roles
            view_id_mapping: Mapeo de IDs de vistas
            role_id_mapping: Mapeo de IDs de roles
        """
        logger.info("Actualizando vistas con referencias a roles")
        
        # Crear mapeo de view_id original a roles que la contienen
        view_to_roles = {}
        
        for role in roles_data:
            role_mongo_id = role_id_mapping[role['id']]
            for view in role['views']:
                view_id = view['id']
                if view_id not in view_to_roles:
                    view_to_roles[view_id] = []
                view_to_roles[view_id].append(role_mongo_id)
        
        # Actualizar transformed_views con referencias a roles
        views_updated = 0
        for view in transformed_views:
            # Encontrar el ID original de PostgreSQL
            original_view_id = None
            for pg_id, mongo_id in view_id_mapping.items():
                if mongo_id == view['_id']:
                    original_view_id = pg_id
                    break
            
            if original_view_id and original_view_id in view_to_roles:
                view['roles'] = view_to_roles[original_view_id]
                views_updated += 1
        
        logger.info(f"Actualizadas {views_updated} vistas con referencias a roles")
    
    def _establish_parent_child_relationships(self, transformed_views: List[Dict], original_views: List[Dict]):
        """
        Establece relaciones padre-hijo en las vistas transformadas
        
        Args:
            transformed_views: Vistas transformadas
            original_views: Vistas originales de PostgreSQL
        """
        # Crear mapeo de MongoDB ObjectId a vista transformada
        mongo_id_to_view = {view['_id']: view for view in transformed_views}
        
        # Establecer relaciones children
        for i, original_view in enumerate(original_views):
            current_view = transformed_views[i]
            
            # Buscar hijos de esta vista
            children_ids = []
            for other_original in original_views:
                if other_original['parentId'] == original_view['id']:
                    child_mongo_id = self.view_id_mapping[other_original['id']]
                    children_ids.append(child_mongo_id)
            
            current_view['children'] = children_ids
            
            if children_ids:
                logger.debug(f"Vista {original_view['code']} tiene {len(children_ids)} hijos")
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """
        Retorna un resumen de la transformación realizada
        
        Returns:
            Diccionario con estadísticas de la transformación
        """
        return {
            'views_mapped': len(self.view_id_mapping),
            'roles_mapped': len(self.role_id_mapping),
            'view_id_mapping_sample': dict(list(self.view_id_mapping.items())[:3]) if self.view_id_mapping else {},
            'role_id_mapping_sample': dict(list(self.role_id_mapping.items())[:3]) if self.role_id_mapping else {}
        }
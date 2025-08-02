from typing import List, Dict, Any
from pymongo.errors import BulkWriteError, DuplicateKeyError
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MongoLoader:
    
    def __init__(self):
        self.mongo_conn = MongoConnection()
        self.database = None
        self.stats = {
            'views_inserted': 0,
            'roles_inserted': 0,
            'views_deleted': 0,
            'roles_deleted': 0,
            'errors': []
        }
    
    def connect(self):
        self.database = self.mongo_conn.connect()
    
    def _check_collections_exist(self):
        if self.database is None:
            self.connect()
        
        existing_collections = self.database.list_collection_names()
        
        if 'views' not in existing_collections:
            raise RuntimeError("Colección 'views' no existe. Debe ser creada por las migraciones del microservicio.")
        
        if 'roles' not in existing_collections:
            raise RuntimeError("Colección 'roles' no existe. Debe ser creada por las migraciones del microservicio.")
    
    def load_views(self, views_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
     
        logger.info(f"Iniciando carga de {len(views_data)} vistas en MongoDB")
        
        try:
            self._check_collections_exist()
            
            views_collection = self.database['views']
            
            if clear_existing:
                delete_result = views_collection.delete_many({})
                self.stats['views_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminadas {self.stats['views_deleted']} vistas existentes")
            
            if views_data:
                insert_result = views_collection.insert_many(views_data, ordered=False)
                self.stats['views_inserted'] = len(insert_result.inserted_ids)
                logger.info(f"Insertadas {self.stats['views_inserted']} vistas exitosamente")
                
                return {
                    'success': True,
                    'inserted_count': self.stats['views_inserted'],
                    'deleted_count': self.stats['views_deleted'],
                    'inserted_ids': [str(id) for id in insert_result.inserted_ids]
                }
            else:
                logger.warning("No hay vistas para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['views_deleted'],
                    'inserted_ids': []
                }
        
        except BulkWriteError as e:
            logger.error(f"Error en inserción masiva de vistas: {str(e)}")
            
            successful_inserts = len(e.details.get('writeErrors', []))
            self.stats['views_inserted'] = len(views_data) - successful_inserts
            
            for error in e.details.get('writeErrors', []):
                error_msg = f"Error insertando vista: {error.get('errmsg', 'Error desconocido')}"
                self.stats['errors'].append(error_msg)
                logger.error(error_msg)
            
            return {
                'success': False,
                'inserted_count': self.stats['views_inserted'],
                'deleted_count': self.stats['views_deleted'],
                'errors': e.details.get('writeErrors', [])
            }
        
        except Exception as e:
            error_msg = f"Error inesperado cargando vistas: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['views_deleted'],
                'error': str(e)
            }
    
    def load_roles(self, roles_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        
        logger.info(f"Iniciando carga de {len(roles_data)} roles en MongoDB")
        
        if self.database is None:
            self.connect()
        
        roles_collection = self.database['roles']
        
        try:
            if clear_existing:
                delete_result = roles_collection.delete_many({})
                self.stats['roles_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminados {self.stats['roles_deleted']} roles existentes")
            
            if roles_data:
                insert_result = roles_collection.insert_many(roles_data, ordered=False)
                self.stats['roles_inserted'] = len(insert_result.inserted_ids)
                logger.info(f"Insertados {self.stats['roles_inserted']} roles exitosamente")
                
                return {
                    'success': True,
                    'inserted_count': self.stats['roles_inserted'],
                    'deleted_count': self.stats['roles_deleted'],
                    'inserted_ids': [str(id) for id in insert_result.inserted_ids]
                }
            else:
                logger.warning("No hay roles para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['roles_deleted'],
                    'inserted_ids': []
                }
        
        except BulkWriteError as e:
            logger.error(f"Error en inserción masiva de roles: {str(e)}")
            
            successful_inserts = len(e.details.get('writeErrors', []))
            self.stats['roles_inserted'] = len(roles_data) - successful_inserts
            
            for error in e.details.get('writeErrors', []):
                error_msg = f"Error insertando rol: {error.get('errmsg', 'Error desconocido')}"
                self.stats['errors'].append(error_msg)
                logger.error(error_msg)
            
            return {
                'success': False,
                'inserted_count': self.stats['roles_inserted'],
                'deleted_count': self.stats['roles_deleted'],
                'errors': e.details.get('writeErrors', [])
            }
        
        except Exception as e:
            error_msg = f"Error inesperado cargando roles: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['roles_deleted'],
                'error': str(e)
            }
    
    def create_indexes(self):
       
        logger.info("Los índices deben ser creados por las migraciones del microservicio ms-users")
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        logger.info("Validando integridad de datos en MongoDB")
        
        if self.database is None:
            self.connect()
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            views_collection = self.database['views']
            roles_collection = self.database['roles']
            
            views_count = views_collection.count_documents({})
            roles_count = roles_collection.count_documents({})
            
            validation_results['stats'] = {
                'views_count': views_count,
                'roles_count': roles_count,
                'active_views': views_collection.count_documents({'isActive': True}),
                'active_roles': roles_collection.count_documents({'isActive': True})
            }
            
            view_codes = list(views_collection.distinct('code'))
            if len(view_codes) != views_count:
                validation_results['errors'].append("Códigos de vista duplicados encontrados")
                validation_results['valid'] = False
            
            role_codes = list(roles_collection.distinct('code'))
            if len(role_codes) != roles_count:
                validation_results['errors'].append("Códigos de rol duplicados encontrados")
                validation_results['valid'] = False
            
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            
            for role in roles_collection.find({'views': {'$ne': []}}):
                for view_id in role.get('views', []):
                    if str(view_id) not in all_view_ids:
                        validation_results['errors'].append(
                            f"Rol {role['code']} referencia vista inexistente: {view_id}"
                        )
                        validation_results['valid'] = False
            
            all_role_ids = set(str(doc['_id']) for doc in roles_collection.find({}, {'_id': 1}))
            
            for view in views_collection.find({'roles': {'$ne': []}}):
                for role_id in view.get('roles', []):
                    if str(role_id) not in all_role_ids:
                        validation_results['errors'].append(
                            f"Vista {view['code']} referencia rol inexistente: {role_id}"
                        )
                        validation_results['valid'] = False
            
            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            
        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False
        
        return validation_results
    
    def get_load_stats(self) -> Dict[str, Any]:
        return {
            'views_inserted': self.stats['views_inserted'],
            'roles_inserted': self.stats['roles_inserted'],
            'views_deleted': self.stats['views_deleted'],
            'roles_deleted': self.stats['roles_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }
    
    def close_connection(self):
        self.mongo_conn.disconnect()
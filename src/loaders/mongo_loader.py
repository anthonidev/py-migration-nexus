"""
Cargador de datos a MongoDB
"""
from typing import List, Dict, Any
from pymongo.errors import BulkWriteError, DuplicateKeyError
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MongoLoader:
    """Cargador de datos transformados a MongoDB"""
    
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
        """Establece conexión a MongoDB"""
        self.database = self.mongo_conn.connect()
    
    def load_views(self, views_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """
        Carga las vistas en MongoDB
        
        Args:
            views_data: Lista de vistas transformadas
            clear_existing: Si True, elimina las vistas existentes antes de cargar
            
        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(f"Iniciando carga de {len(views_data)} vistas en MongoDB")
        
        if self.database is None:
            self.connect()
        
        views_collection = self.database['views']
        
        try:
            # Eliminar datos existentes si se solicita
            if clear_existing:
                delete_result = views_collection.delete_many({})
                self.stats['views_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminadas {self.stats['views_deleted']} vistas existentes")
            
            # Insertar nuevas vistas
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
            # Manejar errores de inserción masiva
            logger.error(f"Error en inserción masiva de vistas: {str(e)}")
            
            # Contar inserciones exitosas
            successful_inserts = len(e.details.get('writeErrors', []))
            self.stats['views_inserted'] = len(views_data) - successful_inserts
            
            # Registrar errores específicos
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
        """
        Carga los roles en MongoDB
        
        Args:
            roles_data: Lista de roles transformados
            clear_existing: Si True, elimina los roles existentes antes de cargar
            
        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(f"Iniciando carga de {len(roles_data)} roles en MongoDB")
        
        if self.database is None:
            self.connect()
        
        roles_collection = self.database['roles']
        
        try:
            # Eliminar datos existentes si se solicita
            if clear_existing:
                delete_result = roles_collection.delete_many({})
                self.stats['roles_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminados {self.stats['roles_deleted']} roles existentes")
            
            # Insertar nuevos roles
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
            # Manejar errores de inserción masiva
            logger.error(f"Error en inserción masiva de roles: {str(e)}")
            
            # Contar inserciones exitosas
            successful_inserts = len(e.details.get('writeErrors', []))
            self.stats['roles_inserted'] = len(roles_data) - successful_inserts
            
            # Registrar errores específicos
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
        """Crea los índices necesarios en las colecciones"""
        logger.info("Creando índices en MongoDB")
        
        if self.database is None:
            self.connect()
        
        try:
            # Índices para la colección views
            views_collection = self.database['views']
            
            # Índice único para code
            views_collection.create_index('code', unique=True)
            
            # Índices simples
            views_collection.create_index('parent')
            views_collection.create_index('isActive')
            views_collection.create_index('order')
            views_collection.create_index('roles')
            
            # Índice compuesto
            views_collection.create_index([('parent', 1), ('order', 1)])
            
            logger.info("Índices de vistas creados exitosamente")
            
            # Índices para la colección roles
            roles_collection = self.database['roles']
            
            # Índice único para code
            roles_collection.create_index('code', unique=True)
            
            # Índices simples
            roles_collection.create_index('isActive')
            roles_collection.create_index('views')
            
            logger.info("Índices de roles creados exitosamente")
            
        except Exception as e:
            error_msg = f"Error creando índices: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
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
            
            # Contar documentos
            views_count = views_collection.count_documents({})
            roles_count = roles_collection.count_documents({})
            
            validation_results['stats'] = {
                'views_count': views_count,
                'roles_count': roles_count,
                'active_views': views_collection.count_documents({'isActive': True}),
                'active_roles': roles_collection.count_documents({'isActive': True})
            }
            
            # Validar códigos únicos en vistas
            view_codes = list(views_collection.distinct('code'))
            if len(view_codes) != views_count:
                validation_results['errors'].append("Códigos de vista duplicados encontrados")
                validation_results['valid'] = False
            
            # Validar códigos únicos en roles
            role_codes = list(roles_collection.distinct('code'))
            if len(role_codes) != roles_count:
                validation_results['errors'].append("Códigos de rol duplicados encontrados")
                validation_results['valid'] = False
            
            # Validar referencias de vistas en roles
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            
            for role in roles_collection.find({'views': {'$ne': []}}):
                for view_id in role.get('views', []):
                    if str(view_id) not in all_view_ids:
                        validation_results['errors'].append(
                            f"Rol {role['code']} referencia vista inexistente: {view_id}"
                        )
                        validation_results['valid'] = False
            
            # Validar referencias padre-hijo en vistas
            for view in views_collection.find({'parent': {'$ne': None}}):
                parent_id = view.get('parent')
                if parent_id and str(parent_id) not in all_view_ids:
                    validation_results['errors'].append(
                        f"Vista {view['code']} referencia padre inexistente: {parent_id}"
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
        """Obtiene estadísticas de la carga"""
        return {
            'views_inserted': self.stats['views_inserted'],
            'roles_inserted': self.stats['roles_inserted'],
            'views_deleted': self.stats['views_deleted'],
            'roles_deleted': self.stats['roles_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }
    
    def close_connection(self):
        """Cierra la conexión a MongoDB"""
        self.mongo_conn.disconnect()
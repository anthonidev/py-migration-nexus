from typing import List, Dict, Any
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MongoLoader:
    """Cargador de datos hacia MongoDB"""
    
    def __init__(self):
        self.mongo_connection = MongoConnection()
    
    def load_views(self, views_data: List[Dict[str, Any]], clear_existing: bool = True) -> Dict[str, Any]:
        """
        Carga vistas en MongoDB
        
        Args:
            views_data: Lista de vistas transformadas
            clear_existing: Si True, limpia la colección antes de insertar
            
        Returns:
            Diccionario con resultado de la operación
        """
        try:
            self.mongo_connection.connect()
            views_collection = self.mongo_connection.get_collection('views')
            
            # Limpiar colección existente si se solicita
            if clear_existing:
                delete_result = views_collection.delete_many({})
                logger.info(f"Eliminados {delete_result.deleted_count} documentos existentes de views")
            
            # Insertar nuevas vistas
            if views_data:
                insert_result = views_collection.insert_many(views_data)
                inserted_count = len(insert_result.inserted_ids)
                
                logger.info(f"Insertadas {inserted_count} vistas en MongoDB")
                
                return {
                    'success': True,
                    'inserted_count': inserted_count,
                    'inserted_ids': insert_result.inserted_ids,
                    'deleted_count': delete_result.deleted_count if clear_existing else 0
                }
            else:
                logger.warning("No hay vistas para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'inserted_ids': [],
                    'deleted_count': delete_result.deleted_count if clear_existing else 0
                }
                
        except Exception as e:
            logger.error(f"Error cargando vistas en MongoDB: {str(e)}")
            raise
        finally:
            self.mongo_connection.disconnect()
    
    def load_roles(self, roles_data: List[Dict[str, Any]], clear_existing: bool = True) -> Dict[str, Any]:
        """
        Carga roles en MongoDB
        
        Args:
            roles_data: Lista de roles transformados
            clear_existing: Si True, limpia la colección antes de insertar
            
        Returns:
            Diccionario con resultado de la operación
        """
        try:
            self.mongo_connection.connect()
            roles_collection = self.mongo_connection.get_collection('roles')
            
            # Limpiar colección existente si se solicita
            if clear_existing:
                delete_result = roles_collection.delete_many({})
                logger.info(f"Eliminados {delete_result.deleted_count} documentos existentes de roles")
            
            # Insertar nuevos roles
            if roles_data:
                insert_result = roles_collection.insert_many(roles_data)
                inserted_count = len(insert_result.inserted_ids)
                
                logger.info(f"Insertados {inserted_count} roles en MongoDB")
                
                return {
                    'success': True,
                    'inserted_count': inserted_count,
                    'inserted_ids': insert_result.inserted_ids,
                    'deleted_count': delete_result.deleted_count if clear_existing else 0
                }
            else:
                logger.warning("No hay roles para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'inserted_ids': [],
                    'deleted_count': delete_result.deleted_count if clear_existing else 0
                }
                
        except Exception as e:
            logger.error(f"Error cargando roles en MongoDB: {str(e)}")
            raise
        finally:
            self.mongo_connection.disconnect()
    
    def get_counts(self) -> Dict[str, int]:
        """
        Obtiene conteos de documentos en MongoDB para validación
        
        Returns:
            Diccionario con conteos de roles y vistas
        """
        try:
            self.mongo_connection.connect()
            
            roles_count = self.mongo_connection.get_collection('roles').count_documents({})
            views_count = self.mongo_connection.get_collection('views').count_documents({})
            
            counts = {
                'roles': roles_count,
                'views': views_count
            }
            
            logger.info(f"Conteos MongoDB - Roles: {roles_count}, Vistas: {views_count}")
            return counts
            
        except Exception as e:
            logger.error(f"Error obteniendo conteos de MongoDB: {str(e)}")
            raise
        finally:
            self.mongo_connection.disconnect()
    
    def create_indexes(self):
        """
        Crea índices en las colecciones según los esquemas definidos
        """
        try:
            self.mongo_connection.connect()
            
            # Índices para roles
            roles_collection = self.mongo_connection.get_collection('roles')
            roles_collection.create_index("code", unique=True)
            roles_collection.create_index("isActive")
            roles_collection.create_index("views")
            
            # Índices para views
            views_collection = self.mongo_connection.get_collection('views')
            views_collection.create_index("code", unique=True)
            views_collection.create_index("parent")
            views_collection.create_index("isActive")
            views_collection.create_index("order")
            views_collection.create_index("roles")
            views_collection.create_index([("parent", 1), ("order", 1)])
            
            logger.info("Índices creados exitosamente")
            
        except Exception as e:
            logger.error(f"Error creando índices: {str(e)}")
            raise
        finally:
            self.mongo_connection.disconnect()
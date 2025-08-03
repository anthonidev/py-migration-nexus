from typing import List, Dict, Any
from pymongo.errors import BulkWriteError
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UsersLoader:
    
    def __init__(self):
        self.mongo_conn = MongoConnection()
        self.database = None
        self.stats = {
            'users_inserted': 0,
            'users_deleted': 0,
            'errors': []
        }
    
    def connect(self):
        self.database = self.mongo_conn.connect()
    
    def _check_collections_exist(self):
        if self.database is None:
            self.connect()
        
        existing_collections = self.database.list_collection_names()
        
        if 'users' not in existing_collections:
            raise RuntimeError("Colección 'users' no existe. Debe ser creada por las migraciones del microservicio.")
        
        if 'roles' not in existing_collections:
            raise RuntimeError("Colección 'roles' no existe. Debe ser creada por las migraciones del microservicio.")
        
        # Verificar que roles tenga datos
        roles_collection = self.database['roles']
        if roles_collection.count_documents({}) == 0:
            raise RuntimeError("La colección 'roles' está vacía. Ejecuta primero la migración de roles y vistas.")
    
    def load_users(self, users_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Iniciando carga de {len(users_data)} usuarios en MongoDB")
        
        try:
            self._check_collections_exist()
            users_collection = self.database['users']
            
            if clear_existing:
                delete_result = users_collection.delete_many({})
                self.stats['users_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminados {self.stats['users_deleted']} usuarios existentes")
            
            if users_data:
                insert_result = users_collection.insert_many(users_data, ordered=False)
                self.stats['users_inserted'] = len(insert_result.inserted_ids)
                logger.info(f"Insertados {self.stats['users_inserted']} usuarios exitosamente")
                
                return {
                    'success': True,
                    'inserted_count': self.stats['users_inserted'],
                    'deleted_count': self.stats['users_deleted']
                }
            else:
                logger.warning("No hay usuarios para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['users_deleted']
                }
        
        except BulkWriteError as e:
            logger.error(f"Error en inserción masiva de usuarios: {str(e)}")
            
            successful_inserts = len(e.details.get('writeErrors', []))
            self.stats['users_inserted'] = len(users_data) - successful_inserts
            
            # Registrar errores específicos
            for error in e.details.get('writeErrors', []):
                error_msg = f"Error insertando usuario: {error.get('errmsg', 'Error desconocido')}"
                self.stats['errors'].append(error_msg)
                logger.error(error_msg)
            
            return {
                'success': False,
                'inserted_count': self.stats['users_inserted'],
                'deleted_count': self.stats['users_deleted'],
                'errors': e.details.get('writeErrors', [])
            }
        
        except Exception as e:
            error_msg = f"Error inesperado cargando usuarios: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['users_deleted'],
                'error': str(e)
            }
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        logger.info("Validando integridad de datos de usuarios en MongoDB")
        
        if self.database is None:
            self.connect()
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            users_collection = self.database['users']
            
            total_users = users_collection.count_documents({})
            validation_results['stats'] = {'total_users': total_users}
            
            # Validar emails únicos
            email_count = len(list(users_collection.distinct('email')))
            if email_count != total_users:
                validation_results['errors'].append("Emails duplicados encontrados")
                validation_results['valid'] = False
            
            # Validar códigos de referencia únicos
            referral_codes = list(users_collection.distinct('referralCode'))
            referral_codes = [code for code in referral_codes if code]
            users_with_referral = users_collection.count_documents({'referralCode': {'$ne': None, '$ne': ''}})
            
            if len(referral_codes) != users_with_referral:
                validation_results['errors'].append("Códigos de referencia duplicados encontrados")
                validation_results['valid'] = False
            
            # Validar referencias de roles
            roles_collection = self.database['roles']
            all_role_ids = set(str(doc['_id']) for doc in roles_collection.find({}, {'_id': 1}))
            
            for user in users_collection.find({'role': {'$ne': None}}):
                role_id = str(user['role'])
                if role_id not in all_role_ids:
                    validation_results['errors'].append(
                        f"Usuario {user['email']} referencia rol inexistente: {role_id}"
                    )
                    validation_results['valid'] = False
            
            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            
        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False
        
        return validation_results
    
    def close_connection(self):
        self.mongo_conn.disconnect()
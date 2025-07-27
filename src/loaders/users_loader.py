"""
Cargador de datos de usuarios a MongoDB
"""
from typing import List, Dict, Any
from pymongo.errors import BulkWriteError, DuplicateKeyError
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UsersLoader:
    """Cargador de datos de usuarios transformados a MongoDB"""
    
    def __init__(self):
        self.mongo_conn = MongoConnection()
        self.database = None
        self.stats = {
            'users_inserted': 0,
            'users_deleted': 0,
            'errors': []
        }
    
    def connect(self):
        """Establece conexión a MongoDB"""
        self.database = self.mongo_conn.connect()
    
    def load_users(self, users_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """
        Carga los usuarios en MongoDB
        
        Args:
            users_data: Lista de usuarios transformados
            clear_existing: Si True, elimina los usuarios existentes antes de cargar
            
        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(f"Iniciando carga de {len(users_data)} usuarios en MongoDB")
        
        if self.database is None:
            self.connect()
        
        users_collection = self.database['users']
        
        try:
            # Eliminar datos existentes si se solicita
            if clear_existing:
                delete_result = users_collection.delete_many({})
                self.stats['users_deleted'] = delete_result.deleted_count
                logger.info(f"Eliminados {self.stats['users_deleted']} usuarios existentes")
            
            # Insertar nuevos usuarios
            if users_data:
                insert_result = users_collection.insert_many(users_data, ordered=False)
                self.stats['users_inserted'] = len(insert_result.inserted_ids)
                logger.info(f"Insertados {self.stats['users_inserted']} usuarios exitosamente")
                
                return {
                    'success': True,
                    'inserted_count': self.stats['users_inserted'],
                    'deleted_count': self.stats['users_deleted'],
                    'inserted_ids': [str(id) for id in insert_result.inserted_ids]
                }
            else:
                logger.warning("No hay usuarios para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['users_deleted'],
                    'inserted_ids': []
                }
        
        except BulkWriteError as e:
            # Manejar errores de inserción masiva
            logger.error(f"Error en inserción masiva de usuarios: {str(e)}")
            
            # Contar inserciones exitosas
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
    
    def create_indexes(self):
        """Crea los índices necesarios en la colección de usuarios"""
        logger.info("Creando índices en colección de usuarios")
        
        if self.database is None:
            self.connect()
        
        try:
            users_collection = self.database['users']
            
            # Índices únicos
            users_collection.create_index('email', unique=True)
            users_collection.create_index('referralCode', unique=True)
            users_collection.create_index(
                [('personalInfo.documentType', 1), ('personalInfo.documentNumber', 1)], 
                unique=True
            )
            
            # Índices simples
            users_collection.create_index('referrerCode')
            users_collection.create_index('parent')
            users_collection.create_index('leftChild')
            users_collection.create_index('rightChild')
            users_collection.create_index('position')
            users_collection.create_index('isActive')
            users_collection.create_index('role')
            users_collection.create_index('contactInfo.phone')
            users_collection.create_index('contactInfo.country')
            users_collection.create_index('billingInfo.ruc')
            
            # Índices compuestos
            users_collection.create_index([('parent', 1), ('position', 1)])
            users_collection.create_index([('isActive', 1), ('role', 1)])
            users_collection.create_index([('referrerCode', 1), ('isActive', 1)])
            
            logger.info("Índices de usuarios creados exitosamente")
            
        except Exception as e:
            error_msg = f"Error creando índices: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
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
            
            # Contar documentos
            total_users = users_collection.count_documents({})
            active_users = users_collection.count_documents({'isActive': True})
            users_with_parent = users_collection.count_documents({'parent': {'$ne': None}})
            users_with_children = users_collection.count_documents({
                '$or': [
                    {'leftChild': {'$ne': None}},
                    {'rightChild': {'$ne': None}}
                ]
            })
            
            validation_results['stats'] = {
                'total_users': total_users,
                'active_users': active_users,
                'inactive_users': total_users - active_users,
                'users_with_parent': users_with_parent,
                'root_users': total_users - users_with_parent,
                'users_with_children': users_with_children
            }
            
            # Validar emails únicos
            email_count = len(list(users_collection.distinct('email')))
            if email_count != total_users:
                validation_results['errors'].append("Emails duplicados encontrados")
                validation_results['valid'] = False
            
            # Validar códigos de referencia únicos
            referral_codes = list(users_collection.distinct('referralCode'))
            referral_codes = [code for code in referral_codes if code]  # Filtrar None/vacíos
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
            
            # Validar relaciones jerárquicas
            self._validate_hierarchy_integrity(users_collection, validation_results)
            
            # Validar documentos únicos
            self._validate_document_uniqueness(users_collection, validation_results)
            
            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            
        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False
        
        return validation_results
    
    def _validate_hierarchy_integrity(self, users_collection, validation_results):
        """Valida la integridad de la jerarquía de usuarios"""
        try:
            all_user_ids = set(str(doc['_id']) for doc in users_collection.find({}, {'_id': 1}))
            
            # Validar referencias padre
            for user in users_collection.find({'parent': {'$ne': None}}):
                parent_id = str(user['parent'])
                if parent_id not in all_user_ids:
                    validation_results['errors'].append(
                        f"Usuario {user['email']} referencia padre inexistente: {parent_id}"
                    )
                    validation_results['valid'] = False
            
            # Validar referencias hijos
            for user in users_collection.find({
                '$or': [
                    {'leftChild': {'$ne': None}},
                    {'rightChild': {'$ne': None}}
                ]
            }):
                if user.get('leftChild'):
                    left_child_id = str(user['leftChild'])
                    if left_child_id not in all_user_ids:
                        validation_results['errors'].append(
                            f"Usuario {user['email']} referencia hijo izquierdo inexistente: {left_child_id}"
                        )
                        validation_results['valid'] = False
                
                if user.get('rightChild'):
                    right_child_id = str(user['rightChild'])
                    if right_child_id not in all_user_ids:
                        validation_results['errors'].append(
                            f"Usuario {user['email']} referencia hijo derecho inexistente: {right_child_id}"
                        )
                        validation_results['valid'] = False
            
            # Validar consistencia bidireccional padre-hijo
            for user in users_collection.find({'parent': {'$ne': None}}):
                parent = users_collection.find_one({'_id': user['parent']})
                if parent:
                    user_id = user['_id']
                    position = user.get('position')
                    
                    if position == 'LEFT' and parent.get('leftChild') != user_id:
                        validation_results['warnings'].append(
                            f"Inconsistencia: Usuario {user['email']} dice tener posición LEFT pero el padre no lo referencia como leftChild"
                        )
                    elif position == 'RIGHT' and parent.get('rightChild') != user_id:
                        validation_results['warnings'].append(
                            f"Inconsistencia: Usuario {user['email']} dice tener posición RIGHT pero el padre no lo referencia como rightChild"
                        )
            
        except Exception as e:
            validation_results['errors'].append(f"Error validando jerarquía: {str(e)}")
            validation_results['valid'] = False
    
    def _validate_document_uniqueness(self, users_collection, validation_results):
        """Valida que los documentos sean únicos"""
        try:
            # Agregar documentos por tipo y número
            pipeline = [
                {
                    '$group': {
                        '_id': {
                            'documentType': '$personalInfo.documentType',
                            'documentNumber': '$personalInfo.documentNumber'
                        },
                        'count': {'$sum': 1}
                    }
                },
                {
                    '$match': {'count': {'$gt': 1}}
                }
            ]
            
            duplicates = list(users_collection.aggregate(pipeline))
            
            if duplicates:
                for dup in duplicates:
                    doc_info = dup['_id']
                    validation_results['errors'].append(
                        f"Documento duplicado: {doc_info['documentType']}-{doc_info['documentNumber']} ({dup['count']} veces)"
                    )
                validation_results['valid'] = False
            
        except Exception as e:
            validation_results['errors'].append(f"Error validando documentos únicos: {str(e)}")
            validation_results['valid'] = False
    
    def get_hierarchy_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la jerarquía de usuarios"""
        if self.database is None:
            self.connect()
        
        try:
            users_collection = self.database['users']
            
            # Estadísticas básicas
            total_users = users_collection.count_documents({})
            root_users = users_collection.count_documents({'parent': None})
            users_with_left_child = users_collection.count_documents({'leftChild': {'$ne': None}})
            users_with_right_child = users_collection.count_documents({'rightChild': {'$ne': None}})
            users_with_both_children = users_collection.count_documents({
                'leftChild': {'$ne': None},
                'rightChild': {'$ne': None}
            })
            
            # Distribución por posición
            left_position_users = users_collection.count_documents({'position': 'LEFT'})
            right_position_users = users_collection.count_documents({'position': 'RIGHT'})
            
            # Profundidad máxima del árbol
            max_depth = self._calculate_max_depth(users_collection)
            
            return {
                'total_users': total_users,
                'root_users': root_users,
                'users_with_left_child': users_with_left_child,
                'users_with_right_child': users_with_right_child,
                'users_with_both_children': users_with_both_children,
                'left_position_users': left_position_users,
                'right_position_users': right_position_users,
                'max_tree_depth': max_depth
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas de jerarquía: {str(e)}")
            return {'error': str(e)}
    
    def _calculate_max_depth(self, users_collection) -> int:
        """Calcula la profundidad máxima del árbol de usuarios"""
        try:
            # Usar agregación para calcular la profundidad
            pipeline = [
                {
                    '$graphLookup': {
                        'from': 'users',
                        'startWith': '$parent',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'as': 'ancestors'
                    }
                },
                {
                    '$project': {
                        'depth': {'$size': '$ancestors'}
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'maxDepth': {'$max': '$depth'}
                    }
                }
            ]
            
            result = list(users_collection.aggregate(pipeline))
            return result[0]['maxDepth'] if result else 0
            
        except Exception as e:
            logger.error(f"Error calculando profundidad máxima: {str(e)}")
            return 0
    
    def get_load_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la carga"""
        return {
            'users_inserted': self.stats['users_inserted'],
            'users_deleted': self.stats['users_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }
    
    def close_connection(self):
        """Cierra la conexión a MongoDB"""
        self.mongo_conn.disconnect()
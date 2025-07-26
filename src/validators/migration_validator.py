from typing import Dict, List, Any
from src.extractors.roles_views_extractor import RolesViewsExtractor
from src.loaders.mongo_loader import MongoLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MigrationValidator:
    """Validador para verificar la integridad de la migración"""
    
    def __init__(self):
        self.extractor = RolesViewsExtractor()
        self.loader = MongoLoader()
    
    def validate_counts(self) -> Dict[str, Any]:
        """
        Valida que los conteos de registros coincidan entre PostgreSQL y MongoDB
        
        Returns:
            Diccionario con resultado de la validación
        """
        try:
            logger.info("Iniciando validación de conteos")
            
            # Obtener conteos de PostgreSQL
            pg_counts = self.extractor.get_counts()
            
            # Obtener conteos de MongoDB
            mongo_counts = self.loader.get_counts()
            
            # Validar coincidencias
            validation_result = {
                'success': True,
                'postgres_counts': pg_counts,
                'mongo_counts': mongo_counts,
                'mismatches': []
            }
            
            # Verificar roles
            if pg_counts['roles'] != mongo_counts['roles']:
                mismatch = {
                    'entity': 'roles',
                    'postgres_count': pg_counts['roles'],
                    'mongo_count': mongo_counts['roles'],
                    'difference': abs(pg_counts['roles'] - mongo_counts['roles'])
                }
                validation_result['mismatches'].append(mismatch)
                validation_result['success'] = False
                logger.error(f"Roles count mismatch: PG={pg_counts['roles']}, Mongo={mongo_counts['roles']}")
            
            # Verificar vistas
            if pg_counts['views'] != mongo_counts['views']:
                mismatch = {
                    'entity': 'views',
                    'postgres_count': pg_counts['views'],
                    'mongo_count': mongo_counts['views'],
                    'difference': abs(pg_counts['views'] - mongo_counts['views'])
                }
                validation_result['mismatches'].append(mismatch)
                validation_result['success'] = False
                logger.error(f"Views count mismatch: PG={pg_counts['views']}, Mongo={mongo_counts['views']}")
            
            if validation_result['success']:
                logger.info(f"Validación exitosa - Roles: {mongo_counts['roles']}, Vistas: {mongo_counts['views']}")
            else:
                logger.error(f"Validación falló con {len(validation_result['mismatches'])} discrepancias")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error durante validación: {str(e)}")
            raise
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Valida la integridad de los datos migrados
        
        Returns:
            Diccionario con resultado de la validación detallada
        """
        try:
            logger.info("Iniciando validación de integridad de datos")
            
            validation_result = {
                'success': True,
                'errors': [],
                'warnings': [],
                'statistics': {}
            }
            
            # Validar estructura de roles
            roles_validation = self._validate_roles_structure()
            validation_result['statistics']['roles'] = roles_validation
            
            if not roles_validation.get('valid', False):
                validation_result['success'] = False
                validation_result['errors'].extend(roles_validation.get('errors', []))
            
            # Validar estructura de vistas
            views_validation = self._validate_views_structure()
            validation_result['statistics']['views'] = views_validation
            
            if not views_validation.get('valid', False):
                validation_result['success'] = False
                validation_result['errors'].extend(views_validation.get('errors', []))
            
            # Validar relaciones
            relations_validation = self._validate_relationships()
            validation_result['statistics']['relationships'] = relations_validation
            
            if not relations_validation.get('valid', False):
                validation_result['success'] = False
                validation_result['errors'].extend(relations_validation.get('errors', []))
            
            if validation_result['success']:
                logger.info("Validación de integridad exitosa")
            else:
                logger.error(f"Validación de integridad falló con {len(validation_result['errors'])} errores")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error durante validación de integridad: {str(e)}")
            raise
    
    def _validate_roles_structure(self) -> Dict[str, Any]:
        """
        Valida la estructura de los roles en MongoDB
        
        Returns:
            Diccionario con resultado de la validación de roles
        """
        try:
            mongo_loader = MongoLoader()
            mongo_loader.mongo_connection.connect()
            roles_collection = mongo_loader.mongo_connection.get_collection('roles')
            
            validation_result = {
                'valid': True,
                'total_roles': 0,
                'roles_with_views': 0,
                'roles_without_views': 0,
                'errors': []
            }
            
            roles = list(roles_collection.find({}))
            validation_result['total_roles'] = len(roles)
            
            for role in roles:
                # Validar campos requeridos
                required_fields = ['code', 'name', 'isActive']
                for field in required_fields:
                    if field not in role:
                        validation_result['errors'].append(f"Rol {role.get('_id')} falta campo: {field}")
                        validation_result['valid'] = False
                
                # Validar que code esté en mayúsculas
                if 'code' in role and role['code'] != role['code'].upper():
                    validation_result['errors'].append(f"Rol {role.get('_id')} código no está en mayúsculas: {role['code']}")
                    validation_result['valid'] = False
                
                # Contar roles con/sin vistas
                if role.get('views'):
                    validation_result['roles_with_views'] += 1
                else:
                    validation_result['roles_without_views'] += 1
            
            logger.info(f"Validación de roles: {validation_result['total_roles']} roles, {len(validation_result['errors'])} errores")
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validando estructura de roles: {str(e)}")
            raise
        finally:
            mongo_loader.mongo_connection.disconnect()
    
    def _validate_views_structure(self) -> Dict[str, Any]:
        """
        Valida la estructura de las vistas en MongoDB
        
        Returns:
            Diccionario con resultado de la validación de vistas
        """
        try:
            mongo_loader = MongoLoader()
            mongo_loader.mongo_connection.connect()
            views_collection = mongo_loader.mongo_connection.get_collection('views')
            
            validation_result = {
                'valid': True,
                'total_views': 0,
                'parent_views': 0,
                'child_views': 0,
                'orphan_views': 0,
                'errors': []
            }
            
            views = list(views_collection.find({}))
            validation_result['total_views'] = len(views)
            
            for view in views:
                # Validar campos requeridos
                required_fields = ['code', 'name', 'order', 'isActive']
                for field in required_fields:
                    if field not in view:
                        validation_result['errors'].append(f"Vista {view.get('_id')} falta campo: {field}")
                        validation_result['valid'] = False
                
                # Validar que code esté en mayúsculas
                if 'code' in view and view['code'] != view['code'].upper():
                    validation_result['errors'].append(f"Vista {view.get('_id')} código no está en mayúsculas: {view['code']}")
                    validation_result['valid'] = False
                
                # Categorizar vistas
                if view.get('parent'):
                    validation_result['child_views'] += 1
                elif view.get('children'):
                    validation_result['parent_views'] += 1
                else:
                    validation_result['orphan_views'] += 1
            
            logger.info(f"Validación de vistas: {validation_result['total_views']} vistas, {len(validation_result['errors'])} errores")
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validando estructura de vistas: {str(e)}")
            raise
        finally:
            mongo_loader.mongo_connection.disconnect()
    
    def _validate_relationships(self) -> Dict[str, Any]:
        """
        Valida las relaciones entre roles y vistas
        
        Returns:
            Diccionario con resultado de la validación de relaciones
        """
        try:
            mongo_loader = MongoLoader()
            mongo_loader.mongo_connection.connect()
            
            roles_collection = mongo_loader.mongo_connection.get_collection('roles')
            views_collection = mongo_loader.mongo_connection.get_collection('views')
            
            validation_result = {
                'valid': True,
                'role_view_references': 0,
                'view_role_references': 0,
                'orphaned_references': 0,
                'errors': []
            }
            
            # Obtener todos los IDs de vistas existentes
            view_ids = set()
            for view in views_collection.find({}, {'_id': 1}):
                view_ids.add(view['_id'])
            
            # Obtener todos los IDs de roles existentes
            role_ids = set()
            for role in roles_collection.find({}, {'_id': 1}):
                role_ids.add(role['_id'])
            
            # Validar referencias de roles a vistas
            for role in roles_collection.find({}):
                if 'views' in role:
                    validation_result['role_view_references'] += len(role['views'])
                    for view_id in role['views']:
                        if view_id not in view_ids:
                            validation_result['errors'].append(f"Rol {role['_id']} referencia vista inexistente: {view_id}")
                            validation_result['orphaned_references'] += 1
                            validation_result['valid'] = False
            
            # Validar referencias de vistas a roles
            for view in views_collection.find({}):
                if 'roles' in view:
                    validation_result['view_role_references'] += len(view['roles'])
                    for role_id in view['roles']:
                        if role_id not in role_ids:
                            validation_result['errors'].append(f"Vista {view['_id']} referencia rol inexistente: {role_id}")
                            validation_result['orphaned_references'] += 1
                            validation_result['valid'] = False
            
            logger.info(f"Validación de relaciones: {validation_result['role_view_references']} refs rol->vista, {validation_result['view_role_references']} refs vista->rol")
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validando relaciones: {str(e)}")
            raise
        finally:
            mongo_loader.mongo_connection.disconnect()
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """
        Genera un reporte completo de la migración
        
        Returns:
            Diccionario con reporte completo de la migración
        """
        try:
            logger.info("Generando reporte de migración")
            
            report = {
                'timestamp': logger.handlers[0].formatter.formatTime(logger.makeRecord('', 0, '', 0, '', (), None)),
                'counts_validation': self.validate_counts(),
                'data_integrity_validation': self.validate_data_integrity()
            }
            
            # Resumen general
            report['summary'] = {
                'overall_success': (
                    report['counts_validation']['success'] and 
                    report['data_integrity_validation']['success']
                ),
                'total_errors': len(report['data_integrity_validation']['errors']),
                'total_warnings': len(report['data_integrity_validation']['warnings'])
            }
            
            logger.info(f"Reporte generado - Éxito: {report['summary']['overall_success']}")
            return report
            
        except Exception as e:
            logger.error(f"Error generando reporte: {str(e)}")
            raise
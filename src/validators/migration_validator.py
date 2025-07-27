"""
Validador de migración de datos
"""
from typing import Dict, Any, List
from src.connections.postgres_connection import PostgresConnection
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MigrationValidator:
    """Validador para verificar la integridad de la migración"""
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
        self.mongo_conn = MongoConnection()
        self.validation_results = {
            'counts_validation': {},
            'data_integrity_validation': {},
            'summary': {
                'overall_success': False,
                'total_errors': 0,
                'total_warnings': 0
            }
        }
    
    def validate_counts(self) -> Dict[str, Any]:
        """Valida que los conteos entre PostgreSQL y MongoDB sean consistentes"""
        logger.info("Validando conteos entre PostgreSQL y MongoDB")
        
        try:
            # Obtener conteos de PostgreSQL
            postgres_counts = self._get_postgres_counts()
            
            # Obtener conteos de MongoDB
            mongo_counts = self._get_mongo_counts()
            
            # Comparar conteos
            counts_validation = {
                'postgres_counts': postgres_counts,
                'mongo_counts': mongo_counts,
                'discrepancies': [],
                'valid': True
            }
            
            # Validar roles
            if postgres_counts['total_roles'] != mongo_counts['roles']:
                discrepancy = {
                    'entity': 'roles',
                    'postgres_count': postgres_counts['total_roles'],
                    'mongo_count': mongo_counts['roles'],
                    'difference': abs(postgres_counts['total_roles'] - mongo_counts['roles'])
                }
                counts_validation['discrepancies'].append(discrepancy)
                counts_validation['valid'] = False
                logger.error(f"Discrepancia en conteo de roles: PG={postgres_counts['total_roles']}, Mongo={mongo_counts['roles']}")
            
            # Validar vistas
            if postgres_counts['total_views'] != mongo_counts['views']:
                discrepancy = {
                    'entity': 'views',
                    'postgres_count': postgres_counts['total_views'],
                    'mongo_count': mongo_counts['views'],
                    'difference': abs(postgres_counts['total_views'] - mongo_counts['views'])
                }
                counts_validation['discrepancies'].append(discrepancy)
                counts_validation['valid'] = False
                logger.error(f"Discrepancia en conteo de vistas: PG={postgres_counts['total_views']}, Mongo={mongo_counts['views']}")
            
            if counts_validation['valid']:
                logger.info("✅ Validación de conteos exitosa")
            else:
                logger.error("❌ Validación de conteos falló")
            
            self.validation_results['counts_validation'] = counts_validation
            return counts_validation
            
        except Exception as e:
            error_msg = f"Error en validación de conteos: {str(e)}"
            logger.error(error_msg)
            self.validation_results['counts_validation'] = {
                'valid': False,
                'error': error_msg
            }
            return self.validation_results['counts_validation']
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos migrados"""
        logger.info("Validando integridad de datos migrados")
        
        try:
            integrity_validation = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'checks_performed': []
            }
            
            # Validar que no haya códigos duplicados
            duplicate_checks = self._validate_unique_codes()
            integrity_validation['checks_performed'].append('unique_codes')
            
            if not duplicate_checks['valid']:
                integrity_validation['valid'] = False
                integrity_validation['errors'].extend(duplicate_checks['errors'])
            
            # Validar referencias entre roles y vistas
            reference_checks = self._validate_references()
            integrity_validation['checks_performed'].append('references')
            
            if not reference_checks['valid']:
                integrity_validation['valid'] = False
                integrity_validation['errors'].extend(reference_checks['errors'])
                integrity_validation['warnings'].extend(reference_checks['warnings'])
            
            # Validar estructura jerárquica de vistas
            hierarchy_checks = self._validate_view_hierarchy()
            integrity_validation['checks_performed'].append('view_hierarchy')
            
            if not hierarchy_checks['valid']:
                integrity_validation['valid'] = False
                integrity_validation['errors'].extend(hierarchy_checks['errors'])
                integrity_validation['warnings'].extend(hierarchy_checks['warnings'])
            
            # Validar datos requeridos
            required_data_checks = self._validate_required_data()
            integrity_validation['checks_performed'].append('required_data')
            
            if not required_data_checks['valid']:
                integrity_validation['valid'] = False
                integrity_validation['errors'].extend(required_data_checks['errors'])
            
            if integrity_validation['valid']:
                logger.info("✅ Validación de integridad exitosa")
            else:
                logger.error("❌ Validación de integridad falló")
            
            self.validation_results['data_integrity_validation'] = integrity_validation
            return integrity_validation
            
        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            self.validation_results['data_integrity_validation'] = {
                'valid': False,
                'error': error_msg,
                'errors': [error_msg]
            }
            return self.validation_results['data_integrity_validation']
    
    def _get_postgres_counts(self) -> Dict[str, int]:
        """Obtiene conteos desde PostgreSQL"""
        try:
            # Contar roles
            roles_query = "SELECT COUNT(*) FROM public.roles"
            roles_result, _ = self.postgres_conn.execute_query(roles_query)
            total_roles = roles_result[0][0]
            
            active_roles_query = "SELECT COUNT(*) FROM public.roles WHERE \"isActive\" = true"
            active_roles_result, _ = self.postgres_conn.execute_query(active_roles_query)
            active_roles = active_roles_result[0][0]
            
            # Contar vistas
            views_query = "SELECT COUNT(*) FROM public.views"
            views_result, _ = self.postgres_conn.execute_query(views_query)
            total_views = views_result[0][0]
            
            active_views_query = "SELECT COUNT(*) FROM public.views WHERE \"isActive\" = true"
            active_views_result, _ = self.postgres_conn.execute_query(active_views_query)
            active_views = active_views_result[0][0]
            
            # Contar relaciones
            relationships_query = "SELECT COUNT(*) FROM public.role_views"
            relationships_result, _ = self.postgres_conn.execute_query(relationships_query)
            total_relationships = relationships_result[0][0]
            
            return {
                'total_roles': total_roles,
                'active_roles': active_roles,
                'total_views': total_views,
                'active_views': active_views,
                'total_relationships': total_relationships
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo conteos de PostgreSQL: {str(e)}")
            raise
    
    def _get_mongo_counts(self) -> Dict[str, int]:
        """Obtiene conteos desde MongoDB"""
        try:
            database = self.mongo_conn.get_database()
            
            roles_collection = database['roles']
            views_collection = database['views']
            
            # Contar documentos
            total_roles = roles_collection.count_documents({})
            active_roles = roles_collection.count_documents({'isActive': True})
            total_views = views_collection.count_documents({})
            active_views = views_collection.count_documents({'isActive': True})
            
            # Contar relaciones (vistas que tienen roles asignados)
            views_with_roles = views_collection.count_documents({'roles': {'$ne': []}})
            
            return {
                'roles': total_roles,
                'active_roles': active_roles,
                'views': total_views,
                'active_views': active_views,
                'views_with_roles': views_with_roles
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo conteos de MongoDB: {str(e)}")
            raise
    
    def _validate_unique_codes(self) -> Dict[str, Any]:
        """Valida que los códigos sean únicos"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': []}
            
            # Validar códigos únicos de roles
            roles_collection = database['roles']
            role_codes = list(roles_collection.distinct('code'))
            total_roles = roles_collection.count_documents({})
            
            if len(role_codes) != total_roles:
                validation_result['valid'] = False
                validation_result['errors'].append("Códigos de rol duplicados encontrados")
            
            # Validar códigos únicos de vistas
            views_collection = database['views']
            view_codes = list(views_collection.distinct('code'))
            total_views = views_collection.count_documents({})
            
            if len(view_codes) != total_views:
                validation_result['valid'] = False
                validation_result['errors'].append("Códigos de vista duplicados encontrados")
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando códigos únicos: {str(e)}"]}
    
    def _validate_references(self) -> Dict[str, Any]:
        """Valida las referencias entre roles y vistas"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': [], 'warnings': []}
            
            roles_collection = database['roles']
            views_collection = database['views']
            
            # Obtener todos los IDs de vistas válidos
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            all_role_ids = set(str(doc['_id']) for doc in roles_collection.find({}, {'_id': 1}))
            
            # Validar referencias de vistas en roles
            for role in roles_collection.find({'views': {'$ne': []}}):
                for view_id in role.get('views', []):
                    if str(view_id) not in all_view_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Rol {role['code']} referencia vista inexistente: {view_id}"
                        )
            
            # Validar referencias de roles en vistas
            for view in views_collection.find({'roles': {'$ne': []}}):
                for role_id in view.get('roles', []):
                    if str(role_id) not in all_role_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Vista {view['code']} referencia rol inexistente: {role_id}"
                        )
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando referencias: {str(e)}"], 'warnings': []}
    
    def _validate_view_hierarchy(self) -> Dict[str, Any]:
        """Valida la jerarquía de vistas (relaciones padre-hijo)"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': [], 'warnings': []}
            
            views_collection = database['views']
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            
            # Validar referencias padre
            for view in views_collection.find({'parent': {'$ne': None}}):
                parent_id = view.get('parent')
                if parent_id and str(parent_id) not in all_view_ids:
                    validation_result['valid'] = False
                    validation_result['errors'].append(
                        f"Vista {view['code']} referencia padre inexistente: {parent_id}"
                    )
            
            # Validar referencias hijos
            for view in views_collection.find({'children': {'$ne': []}}):
                for child_id in view.get('children', []):
                    if str(child_id) not in all_view_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Vista {view['code']} referencia hijo inexistente: {child_id}"
                        )
            
            # Detectar posibles ciclos en la jerarquía
            cycles = self._detect_hierarchy_cycles(views_collection)
            if cycles:
                validation_result['valid'] = False
                for cycle in cycles:
                    validation_result['errors'].append(f"Ciclo detectado en jerarquía: {' -> '.join(cycle)}")
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando jerarquía: {str(e)}"], 'warnings': []}
    
    def _validate_required_data(self) -> Dict[str, Any]:
        """Valida que los campos requeridos estén presentes"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': []}
            role_codes = list(roles_collection.distinct('code'))
            total_roles = roles_collection.count_documents({})
            
            if len(role_codes) != total_roles:
                validation_result['valid'] = False
                validation_result['errors'].append("Códigos de rol duplicados encontrados")
            
            # Validar códigos únicos de vistas
            views_collection = database['views']
            view_codes = list(views_collection.distinct('code'))
            total_views = views_collection.count_documents({})
            
            if len(view_codes) != total_views:
                validation_result['valid'] = False
                validation_result['errors'].append("Códigos de vista duplicados encontrados")
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando códigos únicos: {str(e)}"]}
    
    def _validate_references(self) -> Dict[str, Any]:
        """Valida las referencias entre roles y vistas"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': [], 'warnings': []}
            
            roles_collection = database['roles']
            views_collection = database['views']
            
            # Obtener todos los IDs de vistas válidos
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            all_role_ids = set(str(doc['_id']) for doc in roles_collection.find({}, {'_id': 1}))
            
            # Validar referencias de vistas en roles
            for role in roles_collection.find({'views': {'$ne': []}}):
                for view_id in role.get('views', []):
                    if str(view_id) not in all_view_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Rol {role['code']} referencia vista inexistente: {view_id}"
                        )
            
            # Validar referencias de roles en vistas
            for view in views_collection.find({'roles': {'$ne': []}}):
                for role_id in view.get('roles', []):
                    if str(role_id) not in all_role_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Vista {view['code']} referencia rol inexistente: {role_id}"
                        )
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando referencias: {str(e)}"], 'warnings': []}
    
    def _validate_view_hierarchy(self) -> Dict[str, Any]:
        """Valida la jerarquía de vistas (relaciones padre-hijo)"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': [], 'warnings': []}
            
            views_collection = database['views']
            all_view_ids = set(str(doc['_id']) for doc in views_collection.find({}, {'_id': 1}))
            
            # Validar referencias padre
            for view in views_collection.find({'parent': {'$ne': None}}):
                parent_id = view.get('parent')
                if parent_id and str(parent_id) not in all_view_ids:
                    validation_result['valid'] = False
                    validation_result['errors'].append(
                        f"Vista {view['code']} referencia padre inexistente: {parent_id}"
                    )
            
            # Validar referencias hijos
            for view in views_collection.find({'children': {'$ne': []}}):
                for child_id in view.get('children', []):
                    if str(child_id) not in all_view_ids:
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Vista {view['code']} referencia hijo inexistente: {child_id}"
                        )
            
            # Detectar posibles ciclos en la jerarquía
            cycles = self._detect_hierarchy_cycles(views_collection)
            if cycles:
                validation_result['valid'] = False
                for cycle in cycles:
                    validation_result['errors'].append(f"Ciclo detectado en jerarquía: {' -> '.join(cycle)}")
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando jerarquía: {str(e)}"], 'warnings': []}
    
    def _detect_hierarchy_cycles(self, views_collection) -> List[List[str]]:
        """Detecta ciclos en la jerarquía de vistas"""
        try:
            # Construir grafo de relaciones padre-hijo
            parent_child_map = {}
            for view in views_collection.find({'parent': {'$ne': None}}):
                parent_id = str(view['parent'])
                child_id = str(view['_id'])
                
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = []
                parent_child_map[parent_id].append(child_id)
            
            # Detectar ciclos usando DFS
            cycles = []
            visited = set()
            rec_stack = set()
            
            def dfs(node, path):
                if node in rec_stack:
                    # Encontrado un ciclo
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:] + [node]
                    cycles.append(cycle)
                    return
                
                if node in visited:
                    return
                
                visited.add(node)
                rec_stack.add(node)
                path.append(node)
                
                for child in parent_child_map.get(node, []):
                    dfs(child, path.copy())
                
                rec_stack.remove(node)
            
            # Ejecutar DFS desde cada nodo raíz
            for parent in parent_child_map:
                if parent not in visited:
                    dfs(parent, [])
            
            return cycles
            
        except Exception as e:
            logger.error(f"Error detectando ciclos: {str(e)}")
            return []
    
    def _validate_required_data(self) -> Dict[str, Any]:
        """Valida que los campos requeridos estén presentes"""
        try:
            database = self.mongo_conn.get_database()
            validation_result = {'valid': True, 'errors': []}
            
            # Validar campos requeridos en roles
            roles_collection = database['roles']
            required_role_fields = ['code', 'name']
            
            for role in roles_collection.find():
                for field in required_role_fields:
                    if not role.get(field) or (isinstance(role.get(field), str) and not role.get(field).strip()):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Rol con ID {role['_id']} tiene campo requerido vacío: {field}"
                        )
            
            # Validar campos requeridos en vistas
            views_collection = database['views']
            required_view_fields = ['code', 'name', 'order']
            
            for view in views_collection.find():
                for field in required_view_fields:
                    if field == 'order':
                        if view.get(field) is None or not isinstance(view.get(field), (int, float)):
                            validation_result['valid'] = False
                            validation_result['errors'].append(
                                f"Vista con ID {view['_id']} tiene campo order inválido"
                            )
                    else:
                        if not view.get(field) or (isinstance(view.get(field), str) and not view.get(field).strip()):
                            validation_result['valid'] = False
                            validation_result['errors'].append(
                                f"Vista con ID {view['_id']} tiene campo requerido vacío: {field}"
                            )
            
            return validation_result
            
        except Exception as e:
            return {'valid': False, 'errors': [f"Error validando campos requeridos: {str(e)}"]}
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """Genera un reporte completo de la migración"""
        logger.info("Generando reporte de migración")
        
        try:
            # Ejecutar validaciones si no se han ejecutado
            if not self.validation_results['counts_validation']:
                self.validate_counts()
            
            if not self.validation_results['data_integrity_validation']:
                self.validate_data_integrity()
            
            # Contar errores y advertencias totales
            total_errors = 0
            total_warnings = 0
            
            # Errores de validación de conteos
            counts_val = self.validation_results['counts_validation']
            if not counts_val.get('valid', False):
                total_errors += len(counts_val.get('discrepancies', []))
                if 'error' in counts_val:
                    total_errors += 1
            
            # Errores de validación de integridad
            integrity_val = self.validation_results['data_integrity_validation']
            if not integrity_val.get('valid', False):
                total_errors += len(integrity_val.get('errors', []))
                total_warnings += len(integrity_val.get('warnings', []))
            
            # Determinar éxito general
            overall_success = (
                counts_val.get('valid', False) and 
                integrity_val.get('valid', False) and 
                total_errors == 0
            )
            
            # Actualizar resumen
            self.validation_results['summary'] = {
                'overall_success': overall_success,
                'total_errors': total_errors,
                'total_warnings': total_warnings,
                'validation_timestamp': logger.info.__globals__.get('datetime', __import__('datetime')).datetime.now().isoformat()
            }
            
            # Agregar estadísticas adicionales
            mongo_stats = self._get_final_mongo_stats()
            self.validation_results['final_stats'] = mongo_stats
            
            logger.info(f"Reporte generado: {'EXITOSO' if overall_success else 'CON ERRORES'}")
            return self.validation_results
            
        except Exception as e:
            error_msg = f"Error generando reporte: {str(e)}"
            logger.error(error_msg)
            return {
                'summary': {
                    'overall_success': False,
                    'total_errors': 1,
                    'total_warnings': 0,
                    'error': error_msg
                }
            }
    
    def _get_final_mongo_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas finales de MongoDB"""
        try:
            database = self.mongo_conn.get_database()
            
            roles_collection = database['roles']
            views_collection = database['views']
            
            # Estadísticas de roles
            roles_stats = {
                'total_count': roles_collection.count_documents({}),
                'active_count': roles_collection.count_documents({'isActive': True}),
                'with_views_count': roles_collection.count_documents({'views': {'$ne': []}}),
                'unique_codes': len(list(roles_collection.distinct('code')))
            }
            
            # Estadísticas de vistas
            views_stats = {
                'total_count': views_collection.count_documents({}),
                'active_count': views_collection.count_documents({'isActive': True}),
                'with_roles_count': views_collection.count_documents({'roles': {'$ne': []}}),
                'with_parent_count': views_collection.count_documents({'parent': {'$ne': None}}),
                'with_children_count': views_collection.count_documents({'children': {'$ne': []}}),
                'unique_codes': len(list(views_collection.distinct('code')))
            }
            
            # Estadísticas de relaciones
            total_role_view_refs = 0
            for role in roles_collection.find({'views': {'$ne': []}}):
                total_role_view_refs += len(role.get('views', []))
            
            total_view_role_refs = 0
            for view in views_collection.find({'roles': {'$ne': []}}):
                total_view_role_refs += len(view.get('roles', []))
            
            relationships_stats = {
                'role_to_view_references': total_role_view_refs,
                'view_to_role_references': total_view_role_refs,
                'parent_child_relationships': views_collection.count_documents({'parent': {'$ne': None}})
            }
            
            return {
                'roles': roles_stats,
                'views': views_stats,
                'relationships': relationships_stats,
                'collection_info': {
                    'roles_indexes': list(roles_collection.list_indexes()),
                    'views_indexes': list(views_collection.list_indexes())
                }
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas finales: {str(e)}")
            return {'error': str(e)}
    
    def close_connections(self):
        """Cierra las conexiones a las bases de datos"""
        try:
            self.postgres_conn.disconnect()
            self.mongo_conn.disconnect()
            logger.info("Conexiones cerradas")
        except Exception as e:
            logger.error(f"Error cerrando conexiones: {str(e)}")
    
    def __del__(self):
        """Destructor para asegurar que las conexiones se cierren"""
        try:
            self.close_connections()
        except:
            pass
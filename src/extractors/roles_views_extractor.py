from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class RolesViewsExtractor:
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
    
    def extract_roles_and_views(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando extracción de roles con vistas desde PostgreSQL")
        
        query = """
        SELECT 
            r.id,
            r.code,
            r.name,
            r."isActive",
            r."createdAt",
            r."updatedAt",
            COALESCE(
                views_agg.views, 
                '[]'::json
            ) AS views
        FROM 
            public.roles r
            LEFT JOIN (
                SELECT 
                    rv.role_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', v.id,
                            'code', v.code,
                            'name', v.name,
                            'icon', v.icon,
                            'url', v.url,
                            'isActive', v."isActive",
                            'order', v."order",
                            'metadata', v.metadata,
                            'createdAt', v."createdAt",
                            'updatedAt', v."updatedAt",
                            'parentId', v."parentId"
                        )
                        ORDER BY v."order", v.id
                    ) AS views
                FROM 
                    public.role_views rv
                    INNER JOIN public.views v ON rv.view_id = v.id
                GROUP BY 
                    rv.role_id
            ) views_agg ON r.id = views_agg.role_id
        ORDER BY 
            r.id;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            roles_data = []
            for row in results:
                role_dict = dict(zip(columns, row))
                roles_data.append(role_dict)
            
            logger.info(f"Extraídos {len(roles_data)} roles desde PostgreSQL")
            return roles_data
            
        except Exception as e:
            logger.error(f"Error extrayendo roles y vistas: {str(e)}")
            raise
    
    def extract_all_views(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando extracción de todas las vistas desde PostgreSQL")
        
        query = """
        SELECT 
            v.id,
            v.code,
            v.name,
            v.icon,
            v.url,
            v."isActive",
            v."order",
            v.metadata,
            v."createdAt",
            v."updatedAt",
            v."parentId"
        FROM 
            public.views v
        ORDER BY 
            v."order", v.id;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            views_data = []
            for row in results:
                view_dict = dict(zip(columns, row))
                views_data.append(view_dict)
            
            logger.info(f"Extraídas {len(views_data)} vistas desde PostgreSQL")
            return views_data
            
        except Exception as e:
            logger.error(f"Error extrayendo vistas: {str(e)}")
            raise
    
    def validate_source_data(self) -> Dict[str, Any]:
        """Valida datos básicos de origen"""
        logger.info("Validando datos de origen")
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validar códigos únicos de roles
            duplicate_roles_query = """
            SELECT code, COUNT(*) as count 
            FROM public.roles 
            GROUP BY code 
            HAVING COUNT(*) > 1
            """
            duplicate_roles_results, _ = self.postgres_conn.execute_query(duplicate_roles_query)
            
            if duplicate_roles_results:
                for row in duplicate_roles_results:
                    code, count = row
                    validation_results['errors'].append(f"Código de rol duplicado: '{code}' ({count} veces)")
                validation_results['valid'] = False
            
            # Validar códigos únicos de vistas
            duplicate_views_query = """
            SELECT code, COUNT(*) as count 
            FROM public.views 
            GROUP BY code 
            HAVING COUNT(*) > 1
            """
            duplicate_views_results, _ = self.postgres_conn.execute_query(duplicate_views_query)
            
            if duplicate_views_results:
                for row in duplicate_views_results:
                    code, count = row
                    validation_results['errors'].append(f"Código de vista duplicado: '{code}' ({count} veces)")
                validation_results['valid'] = False
            
            # Validar campos obligatorios en roles
            missing_role_data_query = """
            SELECT id, code, name
            FROM public.roles 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_role_results, _ = self.postgres_conn.execute_query(missing_role_data_query)
            
            if missing_role_results:
                for row in missing_role_results:
                    role_id, code, name = row
                    validation_results['errors'].append(
                        f"Rol ID {role_id}: código='{code}', nombre='{name}' - campos obligatorios vacíos"
                    )
                validation_results['valid'] = False
            
            # Validar campos obligatorios en vistas
            missing_view_data_query = """
            SELECT id, code, name
            FROM public.views 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_view_results, _ = self.postgres_conn.execute_query(missing_view_data_query)
            
            if missing_view_results:
                for row in missing_view_results:
                    view_id, code, name = row
                    validation_results['errors'].append(
                        f"Vista ID {view_id}: código='{code}', nombre='{name}' - campos obligatorios vacíos"
                    )
                validation_results['valid'] = False
            
            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'CON ERRORES'}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results
    
    def close_connection(self):
        self.postgres_conn.disconnect()
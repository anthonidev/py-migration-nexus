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
    
    def extract_role_view_relationships(self) -> List[Dict[str, Any]]:
        logger.info("Extrayendo relaciones role-view desde PostgreSQL")
        
        query = """
        SELECT 
            rv.role_id,
            rv.view_id,
            r.code as role_code,
            v.code as view_code
        FROM 
            public.role_views rv
            INNER JOIN public.roles r ON rv.role_id = r.id
            INNER JOIN public.views v ON rv.view_id = v.id
        ORDER BY 
            rv.role_id, rv.view_id;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            relationships = []
            for row in results:
                rel_dict = dict(zip(columns, row))
                relationships.append(rel_dict)
            
            logger.info(f"Extraídas {len(relationships)} relaciones role-view")
            return relationships
            
        except Exception as e:
            logger.error(f"Error extrayendo relaciones role-view: {str(e)}")
            raise
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        try:
            # Contar roles
            roles_count_query = "SELECT COUNT(*) FROM public.roles WHERE \"isActive\" = true"
            roles_results, _ = self.postgres_conn.execute_query(roles_count_query)
            active_roles_count = roles_results[0][0]
            
            total_roles_query = "SELECT COUNT(*) FROM public.roles"
            total_roles_results, _ = self.postgres_conn.execute_query(total_roles_query)
            total_roles_count = total_roles_results[0][0]
            
            # Contar vistas
            views_count_query = "SELECT COUNT(*) FROM public.views WHERE \"isActive\" = true"
            views_results, _ = self.postgres_conn.execute_query(views_count_query)
            active_views_count = views_results[0][0]
            
            total_views_query = "SELECT COUNT(*) FROM public.views"
            total_views_results, _ = self.postgres_conn.execute_query(total_views_query)
            total_views_count = total_views_results[0][0]
            
            # Contar relaciones
            relationships_query = "SELECT COUNT(*) FROM public.role_views"
            rel_results, _ = self.postgres_conn.execute_query(relationships_query)
            relationships_count = rel_results[0][0]
            
            summary = {
                'roles': {
                    'total': total_roles_count,
                    'active': active_roles_count,
                    'inactive': total_roles_count - active_roles_count
                },
                'views': {
                    'total': total_views_count,
                    'active': active_views_count,
                    'inactive': total_views_count - active_views_count
                },
                'relationships': relationships_count
            }
            
            logger.info(f"Resumen de extracción: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error obteniendo resumen de extracción: {str(e)}")
            raise
    
    def close_connection(self):
        self.postgres_conn.disconnect()
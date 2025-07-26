import json
from typing import List, Dict, Any
from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class RolesViewsExtractor:
    """Extractor de datos de roles y vistas desde PostgreSQL"""
    
    def __init__(self):
        self.pg_connection = PostgresConnection()
    
    def extract_roles_and_views(self) -> List[Dict[str, Any]]:
        """
        Extrae roles con sus vistas asociadas desde PostgreSQL
        
        Returns:
            Lista de diccionarios con datos de roles y vistas
        """
        try:
            self.pg_connection.connect()
            
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
            
            results, columns = self.pg_connection.execute_query(query)
            
            # Convertir resultados a diccionarios
            roles_data = []
            for row in results:
                role_dict = dict(zip(columns, row))
                
                # Parsear JSON de views si es string
                if isinstance(role_dict['views'], str):
                    role_dict['views'] = json.loads(role_dict['views'])
                
                roles_data.append(role_dict)
            
            logger.info(f"Extraídos {len(roles_data)} roles con sus vistas")
            return roles_data
            
        except Exception as e:
            logger.error(f"Error extrayendo roles y vistas: {str(e)}")
            raise
        finally:
            self.pg_connection.disconnect()
    
    def extract_all_views(self) -> List[Dict[str, Any]]:
        """
        Extrae todas las vistas con información de roles asociados
        
        Returns:
            Lista de diccionarios con datos de vistas
        """
        try:
            self.pg_connection.connect()
            
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
                v."parentId",
                v."createdAt",
                v."updatedAt",
                COALESCE(
                    JSON_AGG(
                        DISTINCT r.id
                    ) FILTER (WHERE r.id IS NOT NULL),
                    '[]'::json
                ) as role_ids
            FROM 
                public.views v
                LEFT JOIN public.role_views rv ON v.id = rv.view_id
                LEFT JOIN public.roles r ON rv.role_id = r.id
            GROUP BY 
                v.id, v.code, v.name, v.icon, v.url, v."isActive", 
                v."order", v.metadata, v."parentId", v."createdAt", v."updatedAt"
            ORDER BY 
                v."order", v.id;
            """
            
            results, columns = self.pg_connection.execute_query(query)
            
            # Convertir resultados a diccionarios
            views_data = []
            for row in results:
                view_dict = dict(zip(columns, row))
                
                # Parsear JSON de role_ids si es string
                if isinstance(view_dict['role_ids'], str):
                    view_dict['role_ids'] = json.loads(view_dict['role_ids'])
                
                views_data.append(view_dict)
            
            logger.info(f"Extraídas {len(views_data)} vistas")
            return views_data
            
        except Exception as e:
            logger.error(f"Error extrayendo vistas: {str(e)}")
            raise
        finally:
            self.pg_connection.disconnect()
    
    def get_counts(self) -> Dict[str, int]:
        """
        Obtiene conteos de registros para validación
        
        Returns:
            Diccionario con conteos de roles y vistas
        """
        try:
            self.pg_connection.connect()
            
            # Contar roles
            roles_result, _ = self.pg_connection.execute_query("SELECT COUNT(*) FROM roles")
            roles_count = roles_result[0][0]
            
            # Contar vistas
            views_result, _ = self.pg_connection.execute_query("SELECT COUNT(*) FROM views")
            views_count = views_result[0][0]
            
            counts = {
                'roles': roles_count,
                'views': views_count
            }
            
            logger.info(f"Conteos PostgreSQL - Roles: {roles_count}, Vistas: {views_count}")
            return counts
            
        except Exception as e:
            logger.error(f"Error obteniendo conteos: {str(e)}")
            raise
        finally:
            self.pg_connection.disconnect()
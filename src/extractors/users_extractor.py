"""
Extractor de datos de usuarios desde PostgreSQL
"""
from typing import List, Dict, Any
import sys
import os

# Agregar el directorio raíz al path si es necesario
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UsersExtractor:
    """Extractor de datos de usuarios desde PostgreSQL"""
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
    
    def extract_users_data(self) -> List[Dict[str, Any]]:
        """Extrae todos los usuarios con su información completa"""
        logger.info("Iniciando extracción de usuarios desde PostgreSQL")
        
        query = """
        SELECT 
            u.id as user_id,
            u.email,
            u.password,
            u."referralCode",
            u."referrerCode",
            u.position,
            u."isActive",
            u."createdAt" as user_created_at,
            u."updatedAt" as user_updated_at,
            pi."firstName",
            pi."lastName",
            pi.gender,
            pi."birthDate",
            ci.phone,
            r.code as role_code,
            u.parent_id,
            u.nickname,
            u.photo,
            ci.address as contact_address,
            ci."postalCode",
            bki."bankName",
            bki."accountNumber",
            bki.cci,
            bi.address as billing_address,
            u."lastLoginAt",
            pi."documentNumber"
        FROM users u
        LEFT JOIN personal_info pi ON u.id = pi.user_id
        LEFT JOIN contact_info ci ON u.id = ci.user_id
        LEFT JOIN billing_info bi ON u.id = bi.user_id
        LEFT JOIN bank_info bki ON u.id = bki.user_id
        LEFT JOIN roles r ON u."roleId" = r.id
        ORDER BY u."createdAt" DESC;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            # Convertir resultados a diccionarios
            users_data = []
            for row in results:
                user_dict = dict(zip(columns, row))
                users_data.append(user_dict)
            
            logger.info(f"Extraídos {len(users_data)} usuarios desde PostgreSQL")
            return users_data
            
        except Exception as e:
            logger.error(f"Error extrayendo usuarios: {str(e)}")
            raise
    
    def extract_user_hierarchy_relationships(self) -> List[Dict[str, Any]]:
        """Extrae las relaciones jerárquicas entre usuarios"""
        logger.info("Extrayendo relaciones jerárquicas de usuarios")
        
        query = """
        SELECT 
            u.id as user_id,
            u.parent_id,
            u.position,
            u.email as user_email,
            parent_u.email as parent_email,
            u."referralCode",
            u."referrerCode"
        FROM users u
        LEFT JOIN users parent_u ON u.parent_id = parent_u.id
        WHERE u.parent_id IS NOT NULL
        ORDER BY u.parent_id, u.position;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            relationships = []
            for row in results:
                rel_dict = dict(zip(columns, row))
                relationships.append(rel_dict)
            
            logger.info(f"Extraídas {len(relationships)} relaciones jerárquicas")
            return relationships
            
        except Exception as e:
            logger.error(f"Error extrayendo relaciones jerárquicas: {str(e)}")
            raise
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """Obtiene un resumen de la extracción"""
        try:
            # Contar usuarios totales
            total_users_query = "SELECT COUNT(*) FROM users"
            total_users_results, _ = self.postgres_conn.execute_query(total_users_query)
            total_users = total_users_results[0][0]
            
            # Contar usuarios activos
            active_users_query = "SELECT COUNT(*) FROM users WHERE \"isActive\" = true"
            active_users_results, _ = self.postgres_conn.execute_query(active_users_query)
            active_users = active_users_results[0][0]
            
            # Contar usuarios con información personal
            users_with_personal_info_query = """
            SELECT COUNT(*) FROM users u 
            INNER JOIN personal_info pi ON u.id = pi.user_id
            """
            personal_info_results, _ = self.postgres_conn.execute_query(users_with_personal_info_query)
            users_with_personal_info = personal_info_results[0][0]
            
            # Contar usuarios con información de contacto
            users_with_contact_info_query = """
            SELECT COUNT(*) FROM users u 
            INNER JOIN contact_info ci ON u.id = ci.user_id
            """
            contact_info_results, _ = self.postgres_conn.execute_query(users_with_contact_info_query)
            users_with_contact_info = contact_info_results[0][0]
            
            # Contar usuarios con padre
            users_with_parent_query = "SELECT COUNT(*) FROM users WHERE parent_id IS NOT NULL"
            parent_results, _ = self.postgres_conn.execute_query(users_with_parent_query)
            users_with_parent = parent_results[0][0]
            
            # Contar por rol
            users_by_role_query = """
            SELECT r.code, COUNT(*) as count 
            FROM users u 
            LEFT JOIN roles r ON u."roleId" = r.id 
            GROUP BY r.code
            ORDER BY count DESC
            """
            role_results, role_columns = self.postgres_conn.execute_query(users_by_role_query)
            users_by_role = {}
            for row in role_results:
                role_data = dict(zip(role_columns, row))
                users_by_role[role_data['code'] or 'SIN_ROL'] = role_data['count']
            
            summary = {
                'total_users': total_users,
                'active_users': active_users,
                'inactive_users': total_users - active_users,
                'users_with_personal_info': users_with_personal_info,
                'users_with_contact_info': users_with_contact_info,
                'users_with_parent': users_with_parent,
                'root_users': total_users - users_with_parent,
                'users_by_role': users_by_role
            }
            
            logger.info(f"Resumen de extracción de usuarios: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error obteniendo resumen de extracción: {str(e)}")
            raise
    
    def validate_required_data(self) -> Dict[str, Any]:
        """Valida que los datos requeridos estén presentes"""
        logger.info("Validando datos requeridos para la migración")
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validar usuarios sin email
            no_email_query = "SELECT COUNT(*) FROM users WHERE email IS NULL OR email = ''"
            no_email_results, _ = self.postgres_conn.execute_query(no_email_query)
            users_without_email = no_email_results[0][0]
            
            if users_without_email > 0:
                validation_results['errors'].append(f"{users_without_email} usuarios sin email")
                validation_results['valid'] = False
            
            # Validar usuarios sin información personal
            no_personal_info_query = """
            SELECT COUNT(*) FROM users u 
            LEFT JOIN personal_info pi ON u.id = pi.user_id 
            WHERE pi.user_id IS NULL
            """
            no_personal_results, _ = self.postgres_conn.execute_query(no_personal_info_query)
            users_without_personal_info = no_personal_results[0][0]
            
            if users_without_personal_info > 0:
                validation_results['warnings'].append(f"{users_without_personal_info} usuarios sin información personal")
            
            # Validar usuarios sin rol
            no_role_query = """
            SELECT COUNT(*) FROM users u 
            LEFT JOIN roles r ON u."roleId" = r.id 
            WHERE r.id IS NULL
            """
            no_role_results, _ = self.postgres_conn.execute_query(no_role_query)
            users_without_role = no_role_results[0][0]
            
            if users_without_role > 0:
                validation_results['errors'].append(f"{users_without_role} usuarios sin rol asignado")
                validation_results['valid'] = False
            
            # Validar códigos de referencia duplicados
            duplicate_referral_query = """
            SELECT "referralCode", COUNT(*) as count 
            FROM users 
            WHERE "referralCode" IS NOT NULL 
            GROUP BY "referralCode" 
            HAVING COUNT(*) > 1
            """
            duplicate_results, _ = self.postgres_conn.execute_query(duplicate_referral_query)
            
            if duplicate_results:
                validation_results['errors'].append(f"{len(duplicate_results)} códigos de referencia duplicados")
                validation_results['valid'] = False
            
            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'CON ERRORES'}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results
    
    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()
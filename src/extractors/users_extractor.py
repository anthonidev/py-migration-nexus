from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UsersExtractor:
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
    
    def extract_users_data(self) -> List[Dict[str, Any]]:
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
            
            users_data = []
            for row in results:
                user_dict = dict(zip(columns, row))
                users_data.append(user_dict)
            
            logger.info(f"Extraídos {len(users_data)} usuarios desde PostgreSQL")
            return users_data
            
        except Exception as e:
            logger.error(f"Error extrayendo usuarios: {str(e)}")
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
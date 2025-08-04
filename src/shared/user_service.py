from typing import Optional, Dict, Any, List
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class UserService:

    def __init__(self):
        self.mongo_conn = MongoConnection()
        self.database = None
        self.users_collection = None

    def _ensure_connection(self):
        if self.database is None:
            self.database = self.mongo_conn.get_database()
            self.users_collection = self.database['users']

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:

        try:
            self._ensure_connection()

            email = email.lower().strip()

            user = self.users_collection.find_one(
                {'email': email},
                {
                    '_id': 1,
                    'email': 1,
                    'personalInfo.firstName': 1,
                    'personalInfo.lastName': 1
                }
            )

            if user:
                personal_info = user.get('personalInfo', {})
                first_name = personal_info.get('firstName', '')
                last_name = personal_info.get('lastName', '')
                full_name = f"{first_name} {last_name}".strip()

                result = {
                    'id': str(user['_id']),
                    'email': user['email'],
                    'fullName': full_name
                }

                logger.info(f"Usuario encontrado: {email}")
                return result
            else:
                logger.warning(f"Usuario no encontrado: {email}")
                return None

        except Exception as e:
            logger.error(f"Error buscando usuario por email {email}: {str(e)}")
            return None

    def get_users_batch(self, emails: List[str]) -> Dict[str, Dict[str, Any]]:
        if not emails:
            logger.warning("Lista de emails vacía")
            return {}
        
        try:
            self._ensure_connection()
            
            normalized_emails = list(set(email.lower().strip() for email in emails if email and email.strip()))
            
            if not normalized_emails:
                logger.warning("No hay emails válidos después de la normalización")
                return {}
            
            
            cursor = self.users_collection.find(
                {'email': {'$in': normalized_emails}},
                {
                    '_id': 1,
                    'email': 1,
                    'personalInfo.firstName': 1,
                    'personalInfo.lastName': 1
                }
            )
            
            users_dict = {}
            
            for user in cursor:
                print(f"Procesando usuario: {user.get('email', 'N/A')}")
                
                personal_info = user.get('personalInfo', {})
                first_name = personal_info.get('firstName', '')
                last_name = personal_info.get('lastName', '')
                full_name = f"{first_name} {last_name}".strip()
                
                email = user['email']
                users_dict[email] = {
                    'id': str(user['_id']),
                    'email': email,
                    'fullName': full_name
                }
            
            found_count = len(users_dict)
            total_requested = len(normalized_emails)
            
            if found_count < total_requested:
                missing_emails = set(normalized_emails) - set(users_dict.keys())
                logger.warning(f"No se encontraron {total_requested - found_count} usuarios: {missing_emails}")
            
            logger.info(f"Usuarios obtenidos en lote: {found_count}/{total_requested}")
            return users_dict
            
        except Exception as e:
            logger.error(f"Error obteniendo usuarios en lote {emails}: {str(e)}")
            return {}

    def close_connection(self):
        if self.mongo_conn:
            self.mongo_conn.disconnect()
            self.database = None
            self.users_collection = None
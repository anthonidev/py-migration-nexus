
from typing import Optional, Dict, Any
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
    
    def close_connection(self):
        if self.mongo_conn:
            self.mongo_conn.disconnect()
            self.database = None
            self.users_collection = None
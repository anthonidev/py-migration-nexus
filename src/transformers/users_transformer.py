import random
import string
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from bson import ObjectId
from src.connections.mongo_connection import MongoConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UsersTransformer:
    
    def __init__(self):
        self.mongo_conn = MongoConnection()
        self.stats = {
            'users_transformed': 0,
            'errors': [],
            'warnings': []
        }
        self.role_code_to_id_mapping = {}
        self.user_id_mapping = {}  # ID original -> ObjectId string
        
    def initialize_role_mapping(self):
        logger.info("Inicializando mapeo de roles desde MongoDB")
        
        try:
            database = self.mongo_conn.get_database()
            roles_collection = database['roles']
            
            roles = roles_collection.find({}, {'_id': 1, 'code': 1})
            for role in roles:
                self.role_code_to_id_mapping[role['code']] = role['_id']
            
            logger.info(f"Mapeo de roles inicializado: {len(self.role_code_to_id_mapping)} roles encontrados")
            
        except Exception as e:
            logger.error(f"Error inicializando mapeo de roles: {str(e)}")
            raise
    
    def transform_users_data(self, users_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
        logger.info(f"Iniciando transformación de {len(users_data)} usuarios")
        
        self.initialize_role_mapping()
        users_data = self._resolve_document_duplicates(users_data)
        
        transformed_users = []
        
        for user in users_data:
            old_id = user['user_id']
            new_id = str(ObjectId())
            self.user_id_mapping[old_id] = new_id
        
        for user in users_data:
            try:
                transformed_user = self._transform_single_user(user)
                transformed_users.append(transformed_user)
                self.stats['users_transformed'] += 1
                
            except Exception as e:
                error_msg = f"Error transformando usuario {user.get('user_id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        self._establish_hierarchy_relationships(transformed_users, users_data)
        
        logger.info(f"Transformación completada: {self.stats['users_transformed']} usuarios exitosos")
        return transformed_users, self.user_id_mapping
    
    def _resolve_document_duplicates(self, users_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info("Resolviendo duplicados de números de documento")
        
        sorted_users = sorted(users_data, key=lambda x: x.get('user_created_at') or '1900-01-01')
        used_documents = set()
        
        for user in sorted_users:
            document_number = user.get('documentNumber')
            
            if not document_number or document_number.strip() == '':
                new_document = self._generate_unique_document_number(used_documents)
                user['documentNumber'] = new_document
                used_documents.add(new_document)
                continue
            
            document_number = document_number.strip()
            
            if document_number in used_documents:
                new_document = self._generate_unique_document_number(used_documents)
                user['documentNumber'] = new_document
                used_documents.add(new_document)
                logger.warning(f"Documento duplicado resuelto - Usuario {user['user_id']}")
            else:
                used_documents.add(document_number)
        
        return sorted_users
    
    def _generate_unique_document_number(self, used_documents: set) -> str:
        max_attempts = 1000
        attempts = 0
        
        while attempts < max_attempts:
            document = ''.join(random.choices(string.digits, k=8))
            if document not in used_documents:
                return document
            attempts += 1
        
        import time
        fallback_document = str(int(time.time()))[-8:]
        return fallback_document
    
    def _transform_single_user(self, user: Dict[str, Any]) -> Dict[str, Any]:
        old_id = user['user_id']
        new_id = self.user_id_mapping[old_id]
        
        role_object_id = self._get_role_object_id(user.get('role_code'))
        personal_info = self._transform_personal_info(user)
        contact_info = self._transform_contact_info(user)
        bank_info = self._transform_bank_info(user)
        billing_info = self._transform_billing_info(user)
        
        parent_id = None
        if user.get('parent_id'):
            parent_new_id = self.user_id_mapping.get(user['parent_id'])
            if parent_new_id:
                parent_id = ObjectId(parent_new_id)
        
        transformed_user = {
            '_id': ObjectId(new_id),
            'email': user['email'].lower().strip() if user['email'] else '',
            'password': user['password'] or '',
            'referralCode': user['referralCode'].upper().strip() if user['referralCode'] else '',
            'referrerCode': user['referrerCode'].upper().strip() if user['referrerCode'] else None,
            'parent': parent_id,
            'leftChild': None,
            'rightChild': None,
            'position': user.get('position'),
            'isActive': bool(user.get('isActive', True)),
            'lastLoginAt': self._process_datetime(user.get('lastLoginAt')),
            'role': role_object_id,
            'personalInfo': personal_info,
            'contactInfo': contact_info,
            'billingInfo': billing_info,
            'bankInfo': bank_info,
            'nickname': user.get('nickname', '').strip() if user.get('nickname') else None,
            'photo': user.get('photo', '').strip() if user.get('photo') else None,
            'photoKey': None,
            'createdAt': self._process_datetime(user.get('user_created_at')),
            'updatedAt': self._process_datetime(user.get('user_updated_at'))
        }
        
        return transformed_user
    
    def _get_role_object_id(self, role_code: str) -> ObjectId:
        if not role_code:
            raise ValueError("Usuario sin código de rol válido")
        
        role_object_id = self.role_code_to_id_mapping.get(role_code)
        if not role_object_id:
            error_msg = f"Rol con código '{role_code}' no encontrado en MongoDB"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return role_object_id
    
    def _transform_personal_info(self, user: Dict[str, Any]) -> Dict[str, Any]:
        document_number = user.get('documentNumber', '').strip()
        
        if not document_number:
            document_number = self._generate_random_document_number()
            
        gender = self._map_gender(user.get('gender'))
        
        personal_info = {
            'firstName': user.get('firstName', '').strip() if user.get('firstName') else 'Sin Nombre',
            'lastName': user.get('lastName', '').strip() if user.get('lastName') else 'Sin Apellido',
            'documentType': 'DNI',
            'documentNumber': document_number,
            'gender': gender,
            'birthdate': self._process_birthdate(user.get('birthDate'))
        }
        
        return personal_info
    
    def _transform_contact_info(self, user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        phone = user.get('phone')
        if not phone:
            return None
        
        contact_info = {
            'phone': phone.strip(),
            'address': user.get('contact_address', '').strip() if user.get('contact_address') else None,
            'postalCode': user.get('postalCode', '').strip() if user.get('postalCode') else None,
            'country': 'Peru',
            'address_city': None,
            'country_code': 'PE'
        }
        
        return contact_info
    
    def _transform_bank_info(self, user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        bank_name = user.get('bankName')
        account_number = user.get('accountNumber')
        cci = user.get('cci')
        
        if not any([bank_name, account_number, cci]):
            return None
        
        bank_info = {
            'bankName': bank_name.strip() if bank_name else None,
            'accountNumber': account_number.strip() if account_number else None,
            'cci': cci.strip() if cci else None
        }
        
        return bank_info
    
    def _transform_billing_info(self, user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        billing_address = user.get('billing_address')
        
        if not billing_address:
            return None
        
        billing_info = {
            'ruc': None,
            'razonSocial': None,
            'address': billing_address.strip() if billing_address else None
        }
        
        return billing_info
    
    def _establish_hierarchy_relationships(self, transformed_users: List[Dict[str, Any]], original_users: List[Dict[str, Any]]):
        logger.info("Estableciendo relaciones jerárquicas")
        
        user_mapping = {str(user['_id']): user for user in transformed_users}
        hierarchy_count = 0
        
        for original_user in original_users:
            if not original_user.get('parent_id'):
                continue
                
            user_id = self.user_id_mapping.get(original_user['user_id'])
            parent_id = self.user_id_mapping.get(original_user['parent_id'])
            position = original_user.get('position')
            
            if user_id and parent_id and position and user_id in user_mapping and parent_id in user_mapping:
                parent_user = user_mapping[parent_id]
                
                if position.upper() == 'LEFT':
                    parent_user['leftChild'] = ObjectId(user_id)
                elif position.upper() == 'RIGHT':
                    parent_user['rightChild'] = ObjectId(user_id)
                
                hierarchy_count += 1
        
        logger.info(f"Relaciones jerárquicas establecidas: {hierarchy_count}")
    
    def _generate_random_document_number(self) -> str:
        return ''.join(random.choices(string.digits, k=8))
    
    def _map_gender(self, gender: str) -> str:
        if not gender:
            return 'Otro'
        
        gender_lower = gender.lower().strip()
        
        if gender_lower in ['masculino', 'male', 'm', 'hombre']:
            return 'Masculino'
        elif gender_lower in ['femenino', 'female', 'f', 'mujer']:
            return 'Femenino'
        else:
            return 'Otro'
    
    def _process_datetime(self, dt_value: Any) -> Optional[datetime]:
        if dt_value is None:
            return None
        
        if isinstance(dt_value, datetime):
            return dt_value
        
        if isinstance(dt_value, str):
            try:
                datetime_formats = [
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%fZ',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                ]
                
                for fmt in datetime_formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue
                
                return None
                
            except Exception:
                return None
        
        return None
    
    def _process_birthdate(self, birthdate: Any) -> datetime:
        if birthdate is None:
            default_date = datetime.now().replace(year=datetime.now().year - 30, hour=0, minute=0, second=0, microsecond=0)
            return default_date
        
        if hasattr(birthdate, 'year') and hasattr(birthdate, 'month') and hasattr(birthdate, 'day'):
            try:
                if isinstance(birthdate, datetime):
                    return birthdate.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    return datetime(birthdate.year, birthdate.month, birthdate.day, 0, 0, 0, 0)
            except Exception:
                pass
        
        if isinstance(birthdate, str):
            try:
                birthdate_formats = [
                    '%Y-%m-%d',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y/%m/%d',
                    '%d/%m/%Y',
                ]
                
                for fmt in birthdate_formats:
                    try:
                        parsed_date = datetime.strptime(birthdate, fmt)
                        return parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    except ValueError:
                        continue
                
            except Exception:
                pass
        
        default_date = datetime.now().replace(year=datetime.now().year - 30, hour=0, minute=0, second=0, microsecond=0)
        return default_date
    
    def validate_transformation(self, transformed_users: List[Dict[str, Any]]) -> Dict[str, Any]:
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            emails = set()
            referral_codes = set()
            documents = set()
            
            for user in transformed_users:
                email = user['email']
                if email in emails:
                    validation_results['errors'].append(f"Email duplicado: {email}")
                    validation_results['valid'] = False
                emails.add(email)
                
                referral_code = user['referralCode']
                if referral_code and referral_code in referral_codes:
                    validation_results['errors'].append(f"Código de referencia duplicado: {referral_code}")
                    validation_results['valid'] = False
                if referral_code:
                    referral_codes.add(referral_code)
                
                personal_info = user['personalInfo']
                document_key = f"{personal_info['documentType']}-{personal_info['documentNumber']}"
                if document_key in documents:
                    validation_results['errors'].append(f"Documento duplicado: {document_key}")
                    validation_results['valid'] = False
                documents.add(document_key)
            
            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results
            
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'users_transformed': self.stats['users_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings']
        }
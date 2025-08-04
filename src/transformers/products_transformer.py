import json
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from src.shared.user_service import UserService
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ProductsTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.stats = {
            'categories_transformed': 0,
            'products_transformed': 0,
            'images_transformed': 0,
            'stock_history_transformed': 0,
            'errors': [],
            'warnings': []
        }
        self.users_cache = {}  # Cache para usuarios obtenidos

    def transform_products_data(self, categories_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma los datos de categorías y productos"""
        logger.info(f"Iniciando transformación de {len(categories_data)} categorías de productos")

        # 1. Obtener todos los emails únicos para consulta masiva de usuarios
        unique_emails = set()
        for category in categories_data:
            products = category.get('products', [])
            if isinstance(products, str):
                try:
                    products = json.loads(products)
                except json.JSONDecodeError:
                    continue
            
            for product in products:
                stock_history = product.get('stockHistory', [])
                if isinstance(stock_history, str):
                    try:
                        stock_history = json.loads(stock_history)
                    except json.JSONDecodeError:
                        continue
                
                for history_item in stock_history:
                    updated_by = history_item.get('updatedBy')
                    if updated_by and updated_by.get('userEmail'):
                        unique_emails.add(updated_by['userEmail'])

        unique_emails = list(unique_emails)
        logger.info(f"Obteniendo información de {len(unique_emails)} usuarios únicos")
        
        # 2. Realizar consulta masiva de usuarios
        if unique_emails:
            self.users_cache = self.user_service.get_users_batch(unique_emails)
            logger.info(f"Cache de usuarios cargado con {len(self.users_cache)} usuarios")

        transformed_categories = []
        transformed_products = []
        transformed_images = []
        transformed_stock_history = []

        for category_data in categories_data:
            try:
                category, products, images, stock_history = self._transform_single_category(category_data)
                transformed_categories.append(category)
                transformed_products.extend(products)
                transformed_images.extend(images)
                transformed_stock_history.extend(stock_history)
                self.stats['categories_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando categoría {category_data.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        logger.info(f"Transformación completada: {self.stats['categories_transformed']} categorías, {self.stats['products_transformed']} productos, {self.stats['images_transformed']} imágenes, {self.stats['stock_history_transformed']} registros de historial")
        return transformed_categories, transformed_products, transformed_images, transformed_stock_history

    def _transform_single_category(self, category_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma una categoría individual con sus productos"""
        category_id = category_data['id']

        # Transformar categoría
        transformed_category = {
            'id': category_id,  # Conservar ID original
            'name': self._clean_text_field(category_data.get('name', ''), 255, 'name'),
            'description': self._clean_text_field(category_data.get('description'), 500),
            'code': self._transform_code(category_data.get('code', '')),
            'order': int(category_data.get('order', 0)),
            'is_active': bool(category_data.get('isActive', True)),
            'created_at': self._process_datetime(category_data.get('createdAt')),
            'updated_at': self._process_datetime(category_data.get('updatedAt'))
        }

        # Transformar productos de esta categoría
        products_data = category_data.get('products', [])
        if isinstance(products_data, str):
            try:
                products_data = json.loads(products_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear products JSON para categoría {category_id}")
                products_data = []

        transformed_products = []
        transformed_images = []
        transformed_stock_history = []

        for product_data in products_data:
            try:
                product, images, stock_history = self._transform_single_product(product_data, category_id)
                transformed_products.append(product)
                transformed_images.extend(images)
                transformed_stock_history.extend(stock_history)
                self.stats['products_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando producto {product_data.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_category, transformed_products, transformed_images, transformed_stock_history

    def _transform_single_product(self, product_data: Dict[str, Any], category_id: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transforma un producto individual con sus imágenes e historial"""
        product_id = product_data['id']

        # Validar y transformar precios
        member_price = self._validate_decimal_field(
            product_data.get('memberPrice'), 'memberPrice', min_value=0.0
        )
        public_price = self._validate_decimal_field(
            product_data.get('publicPrice'), 'publicPrice', min_value=0.0
        )

        # Validar stock
        stock = self._validate_integer_field(
            product_data.get('stock', 0), 'stock', min_value=0
        )

        # Procesar estado del producto
        status = self._map_product_status(product_data.get('status', ''), stock)

        # Procesar beneficios
        benefits = self._clean_array_field(product_data.get('benefits', []))

        # Crear producto transformado
        transformed_product = {
            'id': product_id,  # Conservar ID original
            'name': self._clean_text_field(product_data.get('name', ''), 255, 'name'),
            'description': self._clean_text_field(product_data.get('description', ''), None, 'description'),
            'composition': self._clean_text_field(product_data.get('composition'), None),
            'member_price': member_price,
            'public_price': public_price,
            'stock': stock,
            'status': status,
            'benefits': benefits,
            'sku': self._transform_sku(product_data.get('sku', '')),
            'category_id': category_id,
            'is_active': bool(product_data.get('isActive', True)),
            'created_at': self._process_datetime(product_data.get('createdAt')),
            'updated_at': self._process_datetime(product_data.get('updatedAt'))
        }

        # Transformar imágenes
        images = self._transform_product_images(product_id, product_data.get('images', []))

        # Transformar historial de stock
        stock_history = self._transform_stock_history(product_id, product_data.get('stockHistory', []))

        return transformed_product, images, stock_history

    def _transform_product_images(self, product_id: int, images_data: Any) -> List[Dict[str, Any]]:
        """Transforma las imágenes de un producto"""
        if not images_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(images_data, str):
            try:
                images_data = json.loads(images_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear images JSON para producto {product_id}")
                return []

        if not isinstance(images_data, list):
            return []

        transformed_images = []

        for image_data in images_data:
            try:
                original_id = image_data.get('id')
                if not original_id:
                    logger.warning(f"Imagen de producto {product_id} sin ID original, se omitirá")
                    continue

                url = self._clean_text_field(image_data.get('url'), 500, 'url')
                if not url:
                    logger.warning(f"Imagen {original_id} sin URL, se omitirá")
                    continue

                transformed_image = {
                    'id': original_id,  # Conservar ID original
                    'product_id': product_id,
                    'url': url,
                    'url_key': None,  # Siempre null según especificaciones
                    'is_main': bool(image_data.get('isMain', False)),
                    'order': int(image_data.get('order', 0)),
                    'is_active': bool(image_data.get('isActive', True)),
                    'created_at': self._process_datetime(image_data.get('createdAt')),
                    'updated_at': self._process_datetime(image_data.get('updatedAt'))
                }

                transformed_images.append(transformed_image)
                self.stats['images_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando imagen de producto {product_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_images

    def _transform_stock_history(self, product_id: int, stock_history_data: Any) -> List[Dict[str, Any]]:
        """Transforma el historial de stock de un producto"""
        if not stock_history_data:
            return []

        # Si es string JSON, parsearlo
        if isinstance(stock_history_data, str):
            try:
                stock_history_data = json.loads(stock_history_data)
            except json.JSONDecodeError:
                logger.warning(f"No se pudo parsear stock history JSON para producto {product_id}")
                return []

        if not isinstance(stock_history_data, list):
            return []

        transformed_history = []

        for history_item in stock_history_data:
            try:
                original_id = history_item.get('id')
                if not original_id:
                    logger.warning(f"Historial de stock de producto {product_id} sin ID original, se omitirá")
                    continue

                # Obtener información del usuario
                user_info = self._get_user_info_from_history(history_item)

                # Mapear tipo de acción
                action_type = self._map_stock_action_type(history_item.get('actionType', ''))

                # Validar cantidades
                previous_quantity = self._validate_integer_field(
                    history_item.get('previousQuantity', 0), 'previousQuantity', min_value=0
                )
                new_quantity = self._validate_integer_field(
                    history_item.get('newQuantity', 0), 'newQuantity', min_value=0
                )
                quantity_changed = int(history_item.get('quantityChanged', 0))

                transformed_history_item = {
                    'id': original_id,  # Conservar ID original
                    'product_id': product_id,
                    'action_type': action_type,
                    'previous_quantity': previous_quantity,
                    'new_quantity': new_quantity,
                    'quantity_changed': quantity_changed,
                    'notes': self._clean_text_field(history_item.get('notes'), 500),
                    'user_id': user_info['id'] if user_info else None,
                    'user_email': user_info['email'] if user_info else None,
                    'user_name': user_info['fullName'] if user_info else None,
                    'created_at': self._process_datetime(history_item.get('createdAt'))
                }

                transformed_history.append(transformed_history_item)
                self.stats['stock_history_transformed'] += 1

            except Exception as e:
                error_msg = f"Error transformando historial de stock de producto {product_id}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        return transformed_history

    def _get_user_info_from_history(self, history_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Obtiene información del usuario desde el historial y cache"""
        updated_by = history_item.get('updatedBy')
        if not updated_by or not updated_by.get('userEmail'):
            return None

        email = updated_by['userEmail'].lower().strip()
        return self.users_cache.get(email)

    def _transform_code(self, code: str) -> str:
        """Transforma código de categoría"""
        if not code:
            raise ValueError("Código de categoría no puede estar vacío")

        # Convertir a mayúsculas y limpiar espacios
        transformed = code.upper().strip()
        import re
        transformed = re.sub(r'\s+', '_', transformed)

        if len(transformed) > 50:
            warning = f"Código '{transformed}' excede 50 caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            transformed = transformed[:50]

        return transformed

    def _transform_sku(self, sku: str) -> str:
        """Transforma SKU del producto"""
        if not sku:
            raise ValueError("SKU no puede estar vacío")

        # Convertir a mayúsculas y limpiar espacios
        transformed = sku.upper().strip()
        import re
        transformed = re.sub(r'\s+', '_', transformed)

        if len(transformed) > 100:
            warning = f"SKU '{transformed}' excede 100 caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            transformed = transformed[:100]

        return transformed

    def _map_product_status(self, status: str, stock: int) -> str:
        """Mapea estados de producto"""
        if not status:
            return 'OUT_OF_STOCK' if stock == 0 else 'ACTIVE'

        status_upper = status.upper().strip()

        status_mapping = {
            'ACTIVE': 'ACTIVE',
            'ACTIVO': 'ACTIVE',
            'INACTIVE': 'INACTIVE',
            'INACTIVO': 'INACTIVE',
            'OUT_OF_STOCK': 'OUT_OF_STOCK',
            'SIN_STOCK': 'OUT_OF_STOCK',
            'AGOTADO': 'OUT_OF_STOCK'
        }

        mapped_status = status_mapping.get(status_upper, 'ACTIVE')
        
        # Si el stock es 0 y el status es ACTIVE, cambiar a OUT_OF_STOCK
        if stock == 0 and mapped_status == 'ACTIVE':
            mapped_status = 'OUT_OF_STOCK'

        return mapped_status

    def _map_stock_action_type(self, action_type: str) -> str:
        """Mapea tipos de acción de stock"""
        if not action_type:
            return 'INCREASE'  # Default

        action_upper = action_type.upper().strip()

        action_mapping = {
            'INCREASE': 'INCREASE',
            'INCREMENTAR': 'INCREASE',
            'AUMENTAR': 'INCREASE',
            'DECREASE': 'DECREASE',
            'DECREMENTAR': 'DECREASE',
            'DISMINUIR': 'DECREASE',
            'UPDATE': 'UPDATE',
            'ACTUALIZAR': 'UPDATE',
            'MODIFICAR': 'UPDATE'
        }

        return action_mapping.get(action_upper, 'INCREASE')

    def _clean_array_field(self, array_field: Any) -> List[str]:
        """Limpia campos de array"""
        if not array_field:
            return []

        if isinstance(array_field, str):
            # Si es un string JSON array
            if array_field.startswith('[') and array_field.endswith(']'):
                try:
                    array_field = json.loads(array_field)
                except json.JSONDecodeError:
                    return []
            # Si es un string con formato PostgreSQL array
            elif array_field.startswith('{') and array_field.endswith('}'):
                items = array_field[1:-1].split(',')
                array_field = [item.strip().strip('"') for item in items if item.strip()]
            else:
                array_field = [array_field]

        if not isinstance(array_field, list):
            return []

        cleaned_array = []
        for item in array_field:
            if item and str(item).strip():
                cleaned_item = str(item).strip()
                if cleaned_item:
                    cleaned_array.append(cleaned_item)

        return cleaned_array

    def _validate_decimal_field(self, value: Any, field_name: str, min_value: float = None) -> float:
        """Valida campos decimales"""
        if value is None:
            raise ValueError(f"{field_name} es requerido")

        try:
            decimal_value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número válido: {value}")

        if min_value is not None and decimal_value < min_value:
            if field_name in ['memberPrice', 'publicPrice']:
                raise ValueError(f"El precio no puede ser negativo: {field_name}")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return decimal_value

    def _validate_integer_field(self, value: Any, field_name: str, min_value: int = None) -> int:
        """Valida campos enteros"""
        if value is None:
            if field_name == 'stock':
                return 0  # Stock por defecto es 0
            raise ValueError(f"{field_name} es requerido")

        try:
            int_value = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} debe ser un número entero válido: {value}")

        if min_value is not None and int_value < min_value:
            if field_name == 'stock':
                raise ValueError("El stock no puede ser negativo")
            else:
                raise ValueError(f"{field_name} no puede ser menor que {min_value}")

        return int_value

    def _clean_text_field(self, text: str, max_length: int = None, field_name: str = None) -> Optional[str]:
        """Limpia campos de texto"""
        if not text or not str(text).strip():
            if field_name in ['name', 'description']:
                if field_name == 'name':
                    raise ValueError("El nombre es requerido y no puede estar vacío")
                elif field_name == 'description':
                    raise ValueError("La descripción es requerida y no puede estar vacía")
            return None

        cleaned = str(text).strip()

        if max_length and len(cleaned) > max_length:
            warning = f"Campo '{field_name or 'texto'}' excede {max_length} caracteres, será truncado"
            logger.warning(warning)
            self.stats['warnings'].append(warning)
            cleaned = cleaned[:max_length]

        return cleaned if cleaned else None

    def _process_datetime(self, dt_value: Any) -> Optional[datetime]:
        """Procesa campos de fecha/hora"""
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
                    '%Y-%m-%d'
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

    def validate_transformation(self, categories: List[Dict[str, Any]], 
                              products: List[Dict[str, Any]], 
                              images: List[Dict[str, Any]], 
                              stock_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida la transformación de datos"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            category_ids = set()
            product_ids = set()
            category_codes = set()
            product_skus = set()

            # Validar categorías
            for category in categories:
                # Validar ID único
                category_id = category['id']
                if category_id in category_ids:
                    validation_results['errors'].append(f"ID de categoría duplicado: {category_id}")
                    validation_results['valid'] = False
                category_ids.add(category_id)

                # Validar código único
                code = category['code']
                if code in category_codes:
                    validation_results['errors'].append(f"Código de categoría duplicado: {code}")
                    validation_results['valid'] = False
                category_codes.add(code)

                # Validar campos obligatorios
                if not category.get('name'):
                    validation_results['errors'].append(f"Categoría {category_id}: nombre requerido")
                    validation_results['valid'] = False

            # Validar productos
            for product in products:
                # Validar ID único
                product_id = product['id']
                if product_id in product_ids:
                    validation_results['errors'].append(f"ID de producto duplicado: {product_id}")
                    validation_results['valid'] = False
                product_ids.add(product_id)

                # Validar SKU único
                sku = product['sku']
                if sku in product_skus:
                    validation_results['errors'].append(f"SKU duplicado: {sku}")
                    validation_results['valid'] = False
                product_skus.add(sku)

                # Validar campos obligatorios
                if not product.get('name'):
                    validation_results['errors'].append(f"Producto {product_id}: nombre requerido")
                    validation_results['valid'] = False

                if not product.get('description'):
                    validation_results['errors'].append(f"Producto {product_id}: descripción requerida")
                    validation_results['valid'] = False

                # Validar que la categoría exista
                if product['category_id'] not in category_ids:
                    validation_results['errors'].append(f"Producto {product_id}: categoría inexistente {product['category_id']}")
                    validation_results['valid'] = False

                # Validar precios
                if product['member_price'] < 0:
                    validation_results['errors'].append(f"Producto {product_id}: precio de miembro negativo")
                    validation_results['valid'] = False

                if product['public_price'] < 0:
                    validation_results['errors'].append(f"Producto {product_id}: precio público negativo")
                    validation_results['valid'] = False

                # Validar stock
                if product['stock'] < 0:
                    validation_results['errors'].append(f"Producto {product_id}: stock negativo")
                    validation_results['valid'] = False

            # Validar imágenes
            for image in images:
                if image['product_id'] not in product_ids:
                    validation_results['errors'].append(f"Imagen referencia producto inexistente: {image['product_id']}")
                    validation_results['valid'] = False

                if not image.get('url'):
                    validation_results['errors'].append(f"Imagen {image['id']}: URL requerida")
                    validation_results['valid'] = False

            # Validar historial de stock
            for history_item in stock_history:
                if history_item['product_id'] not in product_ids:
                    validation_results['errors'].append(f"Historial referencia producto inexistente: {history_item['product_id']}")
                    validation_results['valid'] = False

                if history_item['previous_quantity'] < 0:
                    validation_results['errors'].append(f"Historial {history_item['id']}: cantidad previa negativa")
                    validation_results['valid'] = False

                if history_item['new_quantity'] < 0:
                    validation_results['errors'].append(f"Historial {history_item['id']}: nueva cantidad negativa")
                    validation_results['valid'] = False

            logger.info(f"Validación de transformación: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def get_transformation_summary(self) -> Dict[str, Any]:
        """Retorna resumen de la transformación"""
        return {
            'categories_transformed': self.stats['categories_transformed'],
            'products_transformed': self.stats['products_transformed'],
            'images_transformed': self.stats['images_transformed'],
            'stock_history_transformed': self.stats['stock_history_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings'],
            'users_cached': len(self.users_cache)
        }

    def close_connections(self):
        """Cierra las conexiones"""
        try:
            self.user_service.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexión del servicio de usuarios: {str(e)}")
# src/loaders/products_loader.py
from typing import List, Dict, Any
from src.connections.orders_postgres_connection import OrdersPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ProductsLoader:

    def __init__(self):
        self.postgres_conn = OrdersPostgresConnection()

    def _check_tables_exist(self):
        """Verifica que las tablas necesarias existan"""
        tables_to_check = ['product_categories', 'products', 'product_images', 'product_stock_history']
        
        for table in tables_to_check:
            check_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table}'
            );
            """
            result, _ = self.postgres_conn.execute_query(check_query)
            if not result[0][0]:
                raise RuntimeError(f"Tabla '{table}' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> Dict[str, int]:
        """Elimina todos los datos existentes en las tablas de productos"""
        logger.info("Eliminando datos de productos existentes")

        try:
            # Eliminar en orden por dependencias FK
            delete_stock_history_query = "DELETE FROM product_stock_history"
            stock_history_deleted, _ = self.postgres_conn.execute_query(delete_stock_history_query)

            delete_images_query = "DELETE FROM product_images"
            images_deleted, _ = self.postgres_conn.execute_query(delete_images_query)

            delete_products_query = "DELETE FROM products"
            products_deleted, _ = self.postgres_conn.execute_query(delete_products_query)

            delete_categories_query = "DELETE FROM product_categories"
            categories_deleted, _ = self.postgres_conn.execute_query(delete_categories_query)

            # Resetear secuencias
            self.postgres_conn.execute_query("SELECT setval('product_categories_id_seq', COALESCE((SELECT MAX(id) FROM product_categories), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('products_id_seq', COALESCE((SELECT MAX(id) FROM products), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('product_images_id_seq', COALESCE((SELECT MAX(id) FROM product_images), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('product_stock_history_id_seq', COALESCE((SELECT MAX(id) FROM product_stock_history), 0) + 1, false);")

            logger.info(f"Eliminados {categories_deleted} categorías, {products_deleted} productos, {images_deleted} imágenes, {stock_history_deleted} registros de historial")

            return {
                'categories_deleted': categories_deleted,
                'products_deleted': products_deleted,
                'images_deleted': images_deleted,
                'stock_history_deleted': stock_history_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            raise

    def load_categories(self, categories_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """Carga las categorías de productos en la base de datos"""
        logger.info(f"Iniciando carga de {len(categories_data)} categorías de productos en PostgreSQL")

        try:
            self._check_tables_exist()
            
            deleted_count = 0
            if clear_existing:
                clear_result = self.clear_existing_data()
                deleted_count = clear_result['categories_deleted']

            if not categories_data:
                logger.warning("No hay categorías para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': deleted_count
                }

            inserted_count = self._insert_categories_with_original_ids(categories_data)
            logger.info(f"Insertadas {inserted_count} categorías exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': deleted_count
            }

        except Exception as e:
            error_msg = f"Error cargando categorías: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': deleted_count if 'deleted_count' in locals() else 0,
                'error': str(e)
            }

    def load_products(self, products_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga los productos en la base de datos"""
        logger.info(f"Iniciando carga de {len(products_data)} productos en PostgreSQL")

        try:
            if not products_data:
                logger.warning("No hay productos para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_products_with_original_ids(products_data)
            logger.info(f"Insertados {inserted_count} productos exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando productos: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def load_images(self, images_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga las imágenes de productos en la base de datos"""
        logger.info(f"Iniciando carga de {len(images_data)} imágenes en PostgreSQL")

        try:
            if not images_data:
                logger.warning("No hay imágenes para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_images_with_original_ids(images_data)
            logger.info(f"Insertadas {inserted_count} imágenes exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando imágenes: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def load_stock_history(self, stock_history_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga el historial de stock en la base de datos"""
        logger.info(f"Iniciando carga de {len(stock_history_data)} registros de historial de stock en PostgreSQL")

        try:
            if not stock_history_data:
                logger.warning("No hay registros de historial para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_stock_history_with_original_ids(stock_history_data)
            logger.info(f"Insertados {inserted_count} registros de historial exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando historial de stock: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def _insert_categories_with_original_ids(self, categories_data: List[Dict[str, Any]]) -> int:
        """Inserta categorías conservando los IDs originales"""
        insert_query = """
        INSERT INTO product_categories (
            id, name, description, code, "order", is_active, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for category in categories_data:
            params = (
                category['id'],
                category['name'],
                category['description'],
                category['code'],
                category['order'],
                category['is_active'],
                category['created_at'],
                category['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia para continuar desde el ID más alto
            max_id = max(category['id'] for category in categories_data)
            self.postgres_conn.execute_query("SELECT setval('product_categories_id_seq', %s, true);", (max_id,))
            logger.info(f"Secuencia de categorías actualizada para continuar desde ID {max_id}")
            
            return len(categories_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de categorías: {str(e)}")
            raise

    def _insert_products_with_original_ids(self, products_data: List[Dict[str, Any]]) -> int:
        """Inserta productos conservando los IDs originales"""
        insert_query = """
        INSERT INTO products (
            id, name, description, composition, member_price, public_price, 
            stock, status, benefits, sku, category_id, is_active, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for product in products_data:
            params = (
                product['id'],
                product['name'],
                product['description'],
                product['composition'],
                product['member_price'],
                product['public_price'],
                product['stock'],
                product['status'],
                product['benefits'],  # PostgreSQL maneja arrays nativamente
                product['sku'],
                product['category_id'],
                product['is_active'],
                product['created_at'],
                product['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if products_data:
                max_id = max(product['id'] for product in products_data)
                self.postgres_conn.execute_query("SELECT setval('products_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de productos actualizada para continuar desde ID {max_id}")

            return len(products_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de productos: {str(e)}")
            raise

    def _insert_images_with_original_ids(self, images_data: List[Dict[str, Any]]) -> int:
        """Inserta imágenes conservando los IDs originales"""
        insert_query = """
        INSERT INTO product_images (
            id, product_id, url, url_key, is_main, "order", is_active, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for image in images_data:
            params = (
                image['id'],
                image['product_id'],
                image['url'],
                image['url_key'],
                image['is_main'],
                image['order'],
                image['is_active'],
                image['created_at'],
                image['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if images_data:
                max_id = max(image['id'] for image in images_data)
                self.postgres_conn.execute_query("SELECT setval('product_images_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de imágenes actualizada para continuar desde ID {max_id}")

            return len(images_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de imágenes: {str(e)}")
            raise

    def _insert_stock_history_with_original_ids(self, stock_history_data: List[Dict[str, Any]]) -> int:
        """Inserta historial de stock conservando los IDs originales"""
        insert_query = """
        INSERT INTO product_stock_history (
            id, product_id, action_type, previous_quantity, new_quantity, 
            quantity_changed, notes, user_id, user_email, user_name, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for history_item in stock_history_data:
            params = (
                history_item['id'],
                history_item['product_id'],
                history_item['action_type'],
                history_item['previous_quantity'],
                history_item['new_quantity'],
                history_item['quantity_changed'],
                history_item['notes'],
                history_item['user_id'],
                history_item['user_email'],
                history_item['user_name'],
                history_item['created_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if stock_history_data:
                max_id = max(history_item['id'] for history_item in stock_history_data)
                self.postgres_conn.execute_query("SELECT setval('product_stock_history_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de historial actualizada para continuar desde ID {max_id}")

            return len(stock_history_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de historial de stock: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos de productos en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            categories_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM product_categories")
            products_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM products")
            images_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM product_images")
            stock_history_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM product_stock_history")

            total_categories = categories_count[0][0]
            total_products = products_count[0][0]
            total_images = images_count[0][0]
            total_stock_history = stock_history_count[0][0]

            validation_results['stats'] = {
                'total_categories': total_categories,
                'total_products': total_products,
                'total_images': total_images,
                'total_stock_history': total_stock_history
            }

            # Validar códigos únicos de categorías
            unique_category_codes, _ = self.postgres_conn.execute_query("SELECT COUNT(DISTINCT code) FROM product_categories")
            unique_codes = unique_category_codes[0][0]

            if unique_codes != total_categories:
                validation_results['errors'].append("Códigos de categoría duplicados encontrados")
                validation_results['valid'] = False

            # Validar SKUs únicos de productos
            unique_product_skus, _ = self.postgres_conn.execute_query("SELECT COUNT(DISTINCT sku) FROM products")
            unique_skus = unique_product_skus[0][0]

            if unique_skus != total_products:
                validation_results['errors'].append("SKUs de producto duplicados encontrados")
                validation_results['valid'] = False

            # Validar campos obligatorios en categorías
            missing_category_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM product_categories 
                WHERE name IS NULL OR name = '' OR code IS NULL OR code = ''
            """)
            
            if missing_category_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_category_data[0][0]} categorías con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar campos obligatorios en productos
            missing_product_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM products 
                WHERE name IS NULL OR name = '' 
                   OR description IS NULL OR description = ''
                   OR sku IS NULL OR sku = ''
                   OR member_price < 0 
                   OR public_price < 0
                   OR stock < 0
            """)
            
            if missing_product_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_product_data[0][0]} productos con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar productos huérfanos (sin categoría)
            orphan_products, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM products p 
                LEFT JOIN product_categories pc ON p.category_id = pc.id 
                WHERE pc.id IS NULL
            """)
            
            if orphan_products[0][0] > 0:
                validation_results['errors'].append(f"{orphan_products[0][0]} productos huérfanos sin categoría asociada")
                validation_results['valid'] = False

            # Validar imágenes huérfanas (sin producto)
            orphan_images, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM product_images pi 
                LEFT JOIN products p ON pi.product_id = p.id 
                WHERE p.id IS NULL
            """)
            
            if orphan_images[0][0] > 0:
                validation_results['errors'].append(f"{orphan_images[0][0]} imágenes huérfanas sin producto asociado")
                validation_results['valid'] = False

            # Validar historial huérfano (sin producto)
            orphan_history, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM product_stock_history psh 
                LEFT JOIN products p ON psh.product_id = p.id 
                WHERE p.id IS NULL
            """)
            
            if orphan_history[0][0] > 0:
                validation_results['errors'].append(f"{orphan_history[0][0]} registros de historial huérfanos sin producto asociado")
                validation_results['valid'] = False

            # Validar cantidades negativas en historial
            negative_quantities, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM product_stock_history 
                WHERE previous_quantity < 0 OR new_quantity < 0
            """)
            
            if negative_quantities[0][0] > 0:
                validation_results['errors'].append(f"{negative_quantities[0][0]} registros de historial con cantidades negativas")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        self.postgres_conn.disconnect()
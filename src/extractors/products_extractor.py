from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ProductsExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_products_data(self) -> List[Dict[str, Any]]:
        """Extrae todas las categorías con sus productos, imágenes e historial de stock desde PostgreSQL"""
        logger.info("Iniciando extracción de productos desde PostgreSQL")

        query = """
        SELECT
            pc.id,
            pc.name,
            pc.description,
            pc.code,
            pc."order",
            pc."isActive",
            pc."createdAt",
            pc."updatedAt",
            COALESCE(
                JSON_AGG(
                    CASE
                        WHEN p.id IS NOT NULL THEN
                            JSON_BUILD_OBJECT(
                                'id', p.id,
                                'name', p.name,
                                'description', p.description,
                                'composition', p.composition,
                                'memberPrice', p."memberPrice",
                                'publicPrice', p."publicPrice",
                                'stock', p.stock,
                                'status', p.status,
                                'benefits', p.benefits,
                                'sku', p.sku,
                                'isActive', p."isActive",
                                'createdAt', p."createdAt",
                                'updatedAt', p."updatedAt",
                                'images', COALESCE(images_agg.images, '[]'::json),
                                'stockHistory', COALESCE(stock_history_agg.stock_history, '[]'::json)
                            )
                        ELSE NULL
                    END
                ) FILTER (WHERE p.id IS NOT NULL),
                '[]'::json
            ) AS products
        FROM
            public.product_categories pc
            LEFT JOIN public.products p ON pc.id = p."categoryId"
            LEFT JOIN (
                SELECT
                    pi."productId",
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', pi.id,
                            'url', pi.url,
                            'isMain', pi."isMain",
                            'order', pi."order",
                            'isActive', pi."isActive",
                            'createdAt', pi."createdAt",
                            'updatedAt', pi."updatedAt"
                        )
                        ORDER BY pi."order", pi.id
                    ) AS images
                FROM
                    public.product_images pi
                GROUP BY
                    pi."productId"
            ) images_agg ON p.id = images_agg."productId"
            LEFT JOIN (
                SELECT
                    psh.product_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', psh.id,
                            'actionType', psh."actionType",
                            'previousQuantity', psh."previousQuantity",
                            'newQuantity', psh."newQuantity",
                            'quantityChanged', psh."quantityChanged",
                            'notes', psh.notes,
                            'createdAt', psh."createdAt",
                            'updatedBy', CASE
                                WHEN psh.updated_by_id IS NOT NULL THEN
                                    JSON_BUILD_OBJECT(
                                        'userEmail', u.email
                                    )
                                ELSE NULL
                            END
                        )
                        ORDER BY psh."createdAt" DESC
                    ) AS stock_history
                FROM
                    public.product_stock_history psh
                    LEFT JOIN public.users u ON psh.updated_by_id = u.id
                GROUP BY
                    psh.product_id
            ) stock_history_agg ON p.id = stock_history_agg.product_id
        GROUP BY
            pc.id,
            pc.name,
            pc.description,
            pc.code,
            pc."order",
            pc."isActive",
            pc."createdAt",
            pc."updatedAt"
        ORDER BY
            pc."order", pc.id;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            products_data = []
            for row in results:
                product_dict = dict(zip(columns, row))
                products_data.append(product_dict)

            logger.info(f"Extraídas {len(products_data)} categorías de productos desde PostgreSQL")
            return products_data

        except Exception as e:
            logger.error(f"Error extrayendo productos: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen para productos")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar categorías sin código
            missing_category_codes_query = """
            SELECT COUNT(*) 
            FROM product_categories 
            WHERE code IS NULL OR code = ''
            """
            missing_codes_results, _ = self.postgres_conn.execute_query(missing_category_codes_query)
            missing_codes = missing_codes_results[0][0]

            if missing_codes > 0:
                validation_results['errors'].append(f"{missing_codes} categorías sin código")
                validation_results['valid'] = False

            # Validar códigos de categoría duplicados
            duplicate_category_codes_query = """
            SELECT code, COUNT(*) as count 
            FROM product_categories 
            WHERE code IS NOT NULL AND code != ''
            GROUP BY code 
            HAVING COUNT(*) > 1
            """
            duplicate_codes_results, _ = self.postgres_conn.execute_query(duplicate_category_codes_query)

            if duplicate_codes_results:
                for row in duplicate_codes_results:
                    code, count = row
                    validation_results['errors'].append(f"Código de categoría duplicado: '{code}' ({count} veces)")
                validation_results['valid'] = False

            # Validar productos sin SKU
            missing_sku_query = """
            SELECT COUNT(*) 
            FROM products 
            WHERE sku IS NULL OR sku = ''
            """
            missing_sku_results, _ = self.postgres_conn.execute_query(missing_sku_query)
            missing_skus = missing_sku_results[0][0]

            if missing_skus > 0:
                validation_results['errors'].append(f"{missing_skus} productos sin SKU")
                validation_results['valid'] = False

            # Validar SKUs duplicados
            duplicate_sku_query = """
            SELECT sku, COUNT(*) as count 
            FROM products 
            WHERE sku IS NOT NULL AND sku != ''
            GROUP BY sku 
            HAVING COUNT(*) > 1
            """
            duplicate_sku_results, _ = self.postgres_conn.execute_query(duplicate_sku_query)

            if duplicate_sku_results:
                for row in duplicate_sku_results:
                    sku, count = row
                    validation_results['errors'].append(f"SKU duplicado: '{sku}' ({count} veces)")
                validation_results['valid'] = False

            # Validar productos con precios negativos
            negative_prices_query = """
            SELECT COUNT(*) 
            FROM products 
            WHERE "memberPrice" < 0 OR "publicPrice" < 0
            """
            negative_prices_results, _ = self.postgres_conn.execute_query(negative_prices_query)
            negative_prices = negative_prices_results[0][0]

            if negative_prices > 0:
                validation_results['errors'].append(f"{negative_prices} productos con precios negativos")
                validation_results['valid'] = False

            # Validar productos con stock negativo
            negative_stock_query = """
            SELECT COUNT(*) 
            FROM products 
            WHERE stock < 0
            """
            negative_stock_results, _ = self.postgres_conn.execute_query(negative_stock_query)
            negative_stock = negative_stock_results[0][0]

            if negative_stock > 0:
                validation_results['errors'].append(f"{negative_stock} productos con stock negativo")
                validation_results['valid'] = False

            # Validar productos huérfanos (sin categoría)
            orphan_products_query = """
            SELECT COUNT(*) 
            FROM products p 
            LEFT JOIN product_categories pc ON p."categoryId" = pc.id 
            WHERE pc.id IS NULL
            """
            orphan_products_results, _ = self.postgres_conn.execute_query(orphan_products_query)
            orphan_products = orphan_products_results[0][0]

            if orphan_products > 0:
                validation_results['errors'].append(f"{orphan_products} productos huérfanos sin categoría")
                validation_results['valid'] = False

            # Validar imágenes huérfanas (sin producto)
            orphan_images_query = """
            SELECT COUNT(*) 
            FROM product_images pi 
            LEFT JOIN products p ON pi."productId" = p.id 
            WHERE p.id IS NULL
            """
            orphan_images_results, _ = self.postgres_conn.execute_query(orphan_images_query)
            orphan_images = orphan_images_results[0][0]

            if orphan_images > 0:
                validation_results['warnings'].append(f"{orphan_images} imágenes huérfanas (se omitirán)")

            # Validar historial de stock huérfano (sin producto)
            orphan_stock_history_query = """
            SELECT COUNT(*) 
            FROM product_stock_history psh 
            LEFT JOIN products p ON psh.product_id = p.id 
            WHERE p.id IS NULL
            """
            orphan_stock_results, _ = self.postgres_conn.execute_query(orphan_stock_history_query)
            orphan_stock = orphan_stock_results[0][0]

            if orphan_stock > 0:
                validation_results['warnings'].append(f"{orphan_stock} registros de historial de stock huérfanos (se omitirán)")

            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()
# src/core/products_migration.py
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.products_extractor import ProductsExtractor
from src.transformers.products_transformer import ProductsTransformer
from src.loaders.products_loader import ProductsLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("products")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE PRODUCTOS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = ProductsExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            failure("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                failure(f"   - {error}")
            
            errors, warnings = extract_validation_issues(validation_result)
            report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        subtitle("📤 PASO 2: Extrayendo productos de PostgreSQL (monolito)")
        categories_data = extractor.extract_products_data()
        info(f"✅ Extraídas {len(categories_data)} categorías de productos")

        report_builder.extraction_completed("product_categories", len(categories_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-orders PostgreSQL")
        transformer = ProductsTransformer()

        transformed_categories, transformed_products, transformed_images, transformed_stock_history = transformer.transform_products_data(categories_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("product_categories", transform_summary['categories_transformed'], total_errors)
        report_builder.transformation_completed("products", transform_summary['products_transformed'], total_errors)
        report_builder.transformation_completed("product_images", transform_summary['images_transformed'], total_errors)
        report_builder.transformation_completed("product_stock_history", transform_summary['stock_history_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(
            transformed_categories, transformed_products, transformed_images, transformed_stock_history)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['categories_transformed']} categorías, {transform_summary['products_transformed']} productos, {transform_summary['images_transformed']} imágenes, {transform_summary['stock_history_transformed']} historial")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-orders)")
        loader = ProductsLoader()

        # Cargar categorías primero
        categories_result = loader.load_categories(transformed_categories, clear_existing=True)

        if not categories_result['success']:
            failure("❌ Error en la carga de categorías")
            if 'error' in categories_result:
                failure(f"Error: {categories_result['error']}")
            
            report_builder.loading_completed("product_categories", 0, 0, 1)
            report_builder.add_validation_errors([categories_result.get('error', 'Error en carga de categorías')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        categories_inserted = categories_result.get('inserted_count', 0)
        categories_deleted = categories_result.get('deleted_count', 0)
        report_builder.loading_completed("product_categories", categories_inserted, categories_deleted)

        info(f"✅ Categorías cargadas: {categories_inserted} insertadas")

        # Cargar productos
        products_result = loader.load_products(transformed_products)

        if not products_result['success']:
            failure("❌ Error en la carga de productos")
            if 'error' in products_result:
                failure(f"Error: {products_result['error']}")
            
            report_builder.loading_completed("products", 0, 0, 1)
            report_builder.add_validation_errors([products_result.get('error', 'Error en carga de productos')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        products_inserted = products_result.get('inserted_count', 0)
        report_builder.loading_completed("products", products_inserted, 0)

        info(f"✅ Productos cargados: {products_inserted} insertados")

        # Cargar imágenes
        images_result = loader.load_images(transformed_images)

        if not images_result['success']:
            failure("❌ Error en la carga de imágenes")
            if 'error' in images_result:
                failure(f"Error: {images_result['error']}")
            
            report_builder.loading_completed("product_images", 0, 0, 1)
            report_builder.add_validation_errors([images_result.get('error', 'Error en carga de imágenes')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        images_inserted = images_result.get('inserted_count', 0)
        report_builder.loading_completed("product_images", images_inserted, 0)

        info(f"✅ Imágenes cargadas: {images_inserted} insertadas")

        # Cargar historial de stock
        stock_history_result = loader.load_stock_history(transformed_stock_history)

        if not stock_history_result['success']:
            failure("❌ Error en la carga de historial de stock")
            if 'error' in stock_history_result:
                failure(f"Error: {stock_history_result['error']}")
            
            report_builder.loading_completed("product_stock_history", 0, 0, 1)
            report_builder.add_validation_errors([stock_history_result.get('error', 'Error en carga de historial')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        stock_history_inserted = stock_history_result.get('inserted_count', 0)
        report_builder.loading_completed("product_stock_history", stock_history_inserted, 0)

        info(f"✅ Historial de stock cargado: {stock_history_inserted} registros insertados")

        subtitle("✅ PASO 5: Validando integridad de datos")
        integrity_validation = loader.validate_data_integrity()

        if not integrity_validation['valid']:
            failure("❌ Validación de integridad fallida")
            for error in integrity_validation['errors']:
                failure(f"   - {error}")
            
            int_errors, int_warnings = extract_validation_issues(integrity_validation)
            report_builder.add_validation_errors(int_errors).add_validation_warnings(int_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        if integrity_validation.get('warnings'):
            report_builder.add_validation_warnings(integrity_validation['warnings'])

        success("🎉 === MIGRACIÓN DE PRODUCTOS COMPLETADA EXITOSAMENTE ===")
        info(f"📂 Categorías migradas: {integrity_validation['stats']['total_categories']}")
        info(f"📦 Productos migrados: {integrity_validation['stats']['total_products']}")
        info(f"🖼️ Imágenes migradas: {integrity_validation['stats']['total_images']}")
        info(f"📋 Registros de historial migrados: {integrity_validation['stats']['total_stock_history']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de productos: {str(e)}")
        report_builder.add_validation_errors([f"Error crítico: {str(e)}"])
        report_builder.mark_failure()
        report_builder.build().save_to_file()
        
        return False

    finally:
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'transformer' in locals():
                transformer.close_connections()
            if 'loader' in locals():
                loader.close_connection()
        except Exception as e:
            failure(f"Error cerrando conexiones: {str(e)}")


def check_dependencies():
    info("🔍 Verificando dependencias...")

    try:
        from src.shared.user_service import UserService

        user_service = UserService()
        test_result = user_service.get_user_by_email("test@test.com")
        user_service.close_connection()

        info("✅ Servicio de usuarios disponible")
        return True

    except Exception as e:
        failure(f"❌ Error verificando dependencias: {str(e)}")
        return False


def test_connections():
    info("🔍 Probando conexiones a bases de datos...")

    try:
        from src.connections.postgres_connection import PostgresConnection
        monolito_conn = PostgresConnection()
        monolito_conn.connect()
        info("✅ Conexión al monolito (PostgreSQL) exitosa")
        monolito_conn.disconnect()

        from src.connections.orders_postgres_connection import OrdersPostgresConnection
        orders_conn = OrdersPostgresConnection()
        orders_conn.connect()
        info("✅ Conexión a ms-orders (PostgreSQL) exitosa")
        orders_conn.disconnect()

        from src.connections.mongo_connection import MongoConnection
        mongo_conn = MongoConnection()
        mongo_conn.connect()
        info("✅ Conexión a ms-users (MongoDB) exitosa")
        mongo_conn.disconnect()

        return True

    except Exception as e:
        failure(f"❌ Error en conexiones: {str(e)}")
        return False


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if not test_connections():
        sys.exit(1)

    if not check_dependencies():
        sys.exit(1)

    success = main()

    if success:
        print("\n🎉 ¡Migración de productos completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de productos falló. Revisa los logs.")
        sys.exit(1)
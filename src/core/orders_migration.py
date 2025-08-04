# src/core/orders_migration.py
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.orders_extractor import OrdersExtractor
from src.transformers.orders_transformer import OrdersTransformer
from src.loaders.orders_loader import OrdersLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("orders")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE ÓRDENES ===")
        
        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = OrdersExtractor()
        
        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            failure("❌ Validación de datos fallida")
            for error in validation_result['errors']:
                failure(f"   - {error}")
            
            errors, warnings = extract_validation_issues(validation_result)
            report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        if validation_result.get('warnings'):
            for warning in validation_result['warnings']:
                info(f"⚠️ {warning}")

        subtitle("📤 PASO 2: Extrayendo datos de PostgreSQL")
        orders_data = extractor.extract_orders_data()
        info(f"✅ Extraídas {len(orders_data)} órdenes")
        
        report_builder.extraction_completed("orders", len(orders_data))
        
        subtitle("🔄 PASO 3: Transformando datos para ms-orders")
        transformer = OrdersTransformer()
        
        transformation_result = transformer.transform_orders_data(orders_data)
        
        # Procesar el resumen de transformación para los reportes
        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("orders", transform_summary['orders_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
        
        if not transformation_result['success']:
            failure("❌ Transformación de datos fallida")
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        info(f"✅ Transformadas {len(transformation_result['orders'])} órdenes")
        info(f"✅ Transformadas {len(transformation_result['orders_details'])} detalles de órdenes")
        info(f"✅ Transformadas {len(transformation_result['orders_history'])} historiales de órdenes")
        
        subtitle("📥 PASO 4: Cargando datos en ms-orders")
        loader = OrdersLoader()
        
        # Verificar que las tablas existan
        loader._check_tables_exist()
        
        loading_result = loader.load_orders_data(transformation_result)
        
        if not loading_result['success']:
            failure("❌ Carga de datos fallida")
            for error in loading_result.get('errors', []):
                failure(f"   - {error}")
            
            report_builder.add_validation_errors(loading_result.get('errors', []))
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        info(f"✅ Cargadas {loading_result['orders_inserted']} órdenes")
        info(f"✅ Cargados {loading_result['orders_details_inserted']} detalles de órdenes")
        info(f"✅ Cargados {loading_result['orders_history_inserted']} historiales de órdenes")
        
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
        
        # FINALIZACIÓN EXITOSA
        success("🎉 === MIGRACIÓN DE ÓRDENES COMPLETADA EXITOSAMENTE ===")
        info(f"📋 Órdenes migradas: {integrity_validation['stats']['total_orders']}")
        info(f"📄 Detalles migrados: {integrity_validation['stats']['total_order_details']}")
        info(f"📚 Historiales migrados: {integrity_validation['stats']['total_order_history']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de órdenes: {str(e)}")
        
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
        # Verificar que existan productos en la base de datos de destino
        from src.connections.orders_postgres_connection import OrdersPostgresConnection
        orders_conn = OrdersPostgresConnection()
        orders_conn.connect()
        
        check_products_query = "SELECT COUNT(*) FROM products"
        products_count, _ = orders_conn.execute_query(check_products_query)
        
        if products_count[0][0] == 0:
            failure("❌ No hay productos en ms-orders")
            failure("💡 Ejecuta primero la migración de productos")
            return False

        info(f"✅ Encontrados {products_count[0][0]} productos en ms-orders")
        orders_conn.disconnect()

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
        print("\n🎉 ¡Migración de órdenes completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de órdenes falló. Revisa los logs.")
        sys.exit(1)
from src.loaders.payments_loader import PaymentsLoader
from src.transformers.payments_transformer import PaymentsTransformer
from src.extractors.payments_extractor import PaymentsExtractor
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)
import os
import sys
from src.utils.logger import title, subtitle, success, failure,  info

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


def main():
    report_builder = MigrationReportBuilder("payments")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE PAGOS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = PaymentsExtractor()

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

        subtitle("📤 PASO 2: Extrayendo pagos de PostgreSQL (monolito)")
        payments_data = extractor.extract_payments_data()
        info(f"✅ Extraídos {len(payments_data)} pagos")

        report_builder.extraction_completed("payments", len(payments_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-payments PostgreSQL")
        transformer = PaymentsTransformer()

        transformed_payments, transformed_payment_items = transformer.transform_payments_data(payments_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("payments", transform_summary['payments_transformed'], total_errors)
        report_builder.transformation_completed("payment_items", transform_summary['payment_items_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(
            transformed_payments, transformed_payment_items)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['payments_transformed']} pagos, {transform_summary['payment_items_transformed']} items")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-payments)")
        loader = PaymentsLoader()

        payments_result = loader.load_payments(transformed_payments, clear_existing=True)

        if not payments_result['success']:
            failure("❌ Error en la carga de pagos")
            if 'error' in payments_result:
                failure(f"Error: {payments_result['error']}")
            
            report_builder.loading_completed("payments", 0, 0, 1)
            report_builder.add_validation_errors([payments_result.get('error', 'Error en carga de pagos')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        payments_inserted = payments_result.get('inserted_count', 0)
        payments_deleted = payments_result.get('deleted_count', 0)
        report_builder.loading_completed("payments", payments_inserted, payments_deleted)

        info(f"✅ Pagos cargados: {payments_inserted} insertados")

        items_result = loader.load_payment_items(transformed_payment_items)

        if not items_result['success']:
            failure("❌ Error en la carga de items de pago")
            if 'error' in items_result:
                failure(f"Error: {items_result['error']}")
            
            report_builder.loading_completed("payment_items", 0, 0, 1)
            report_builder.add_validation_errors([items_result.get('error', 'Error en carga de items')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        items_inserted = items_result.get('inserted_count', 0)
        report_builder.loading_completed("payment_items", items_inserted, 0)

        info(f"✅ Items de pago cargados: {items_inserted} insertados")

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

        success("🎉 === MIGRACIÓN DE PAGOS COMPLETADA EXITOSAMENTE ===")
        info(f"💳 Pagos migrados: {integrity_validation['stats']['total_payments']}")
        info(f"📋 Items migrados: {integrity_validation['stats']['total_payment_items']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de pagos: {str(e)}")
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
        from src.connections.payments_postgres_connection import PaymentsPostgresConnection

        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()

        check_configs_query = "SELECT COUNT(*) FROM payment_configs"
        configs_count, _ = payments_conn.execute_query(check_configs_query)

        if configs_count[0][0] == 0:
            failure("❌ No hay configuraciones de pago en ms-payments")
            failure("💡 Ejecuta primero la migración de configuraciones de pago")
            return False

        info(f"✅ Encontradas {configs_count[0][0]} configuraciones de pago en ms-payments")
        payments_conn.disconnect()

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

    try:
        from src.connections.postgres_connection import PostgresConnection
        monolito_conn = PostgresConnection()
        monolito_conn.connect()
        info("✅ Conexión al monolito (PostgreSQL) exitosa")
        monolito_conn.disconnect()

        from src.connections.payments_postgres_connection import PaymentsPostgresConnection
        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()
        info("✅ Conexión a ms-payments (PostgreSQL) exitosa")
        payments_conn.disconnect()

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
        print("\n🎉 ¡Migración de pagos completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de pagos falló. Revisa los logs.")
        sys.exit(1)
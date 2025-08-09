import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.withdrawals_extractor import WithdrawalsExtractor
from src.transformers.withdrawals_transformer import WithdrawalsTransformer
from src.loaders.withdrawals_loader import WithdrawalsLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("withdrawals")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE WITHDRAWALS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = WithdrawalsExtractor()

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

        if validation_result.get('warnings'):
            for warning in validation_result['warnings']:
                info(f"⚠️ {warning}")

        subtitle("📤 PASO 2: Extrayendo withdrawals de PostgreSQL (monolito)")
        withdrawals_data = extractor.extract_withdrawals_data()
        info(f"✅ Extraídos {len(withdrawals_data)} withdrawals")

        report_builder.extraction_completed("withdrawals", len(withdrawals_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-payments PostgreSQL")
        transformer = WithdrawalsTransformer()

        transformed_withdrawals, transformed_withdrawal_points = transformer.transform_withdrawals_data(withdrawals_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("withdrawals", transform_summary['withdrawals_transformed'], total_errors)
        
        if errors:
            for error in errors:
                failure(f"   - {error}")
        
        if warnings:
            for warning in warnings:
                info(f"⚠️ {warning}")

        if total_errors > 0:
            failure(f"❌ Se encontraron {total_errors} errores durante la transformación")
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        subtitle("🏁 PASO 4: Cargando datos en ms-payments PostgreSQL")
        loader = WithdrawalsLoader()

        # Limpiar tablas antes de cargar datos
        cleanup_result = loader.cleanup_existing_data()
        if not cleanup_result:
            failure("❌ Error en la limpieza de datos existentes")
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        # Cargar withdrawals primero
        withdrawals_loaded = loader.load_withdrawals(transformed_withdrawals)
        info(f"✅ Cargados {withdrawals_loaded} withdrawals")

        # Luego cargar withdrawal_points
        withdrawal_points_loaded = loader.load_withdrawal_points(transformed_withdrawal_points)
        info(f"✅ Cargados {withdrawal_points_loaded} withdrawal_points")

        report_builder.loading_completed("withdrawals", withdrawals_loaded)
        report_builder.loading_completed("withdrawal_points", withdrawal_points_loaded)

        subtitle("✅ PASO 5: Validación de integridad")
        integrity_validation = extractor.validate_migration_integrity(loader)
        
        if not integrity_validation['valid']:
            failure("❌ Falló la validación de integridad")
            for error in integrity_validation['errors']:
                failure(f"   - {error}")
            
            report_builder.add_integrity_validation_errors(integrity_validation['errors'])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        # FINALIZACIÓN EXITOSA
        success("🎉 === MIGRACIÓN DE WITHDRAWALS COMPLETADA EXITOSAMENTE ===")
        info(f"📋 Withdrawals migrados: {integrity_validation['stats']['total_withdrawals']}")
        info(f"📋 Withdrawal points migrados: {integrity_validation['stats']['total_withdrawal_points']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de withdrawals: {str(e)}")
        
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
        # Verificar que existan configuraciones de pagos en ms-payments
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
    info("🔍 Probando conexiones a bases de datos...")

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
        print("\n🎉 ¡Migración de withdrawals completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de withdrawals falló. Revisa los logs.")
        sys.exit(1)
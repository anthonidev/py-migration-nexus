import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.user_points_extractor import UserPointsExtractor
from src.transformers.user_points_transformer import UserPointsTransformer
from src.loaders.user_points_loader import UserPointsLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("user_points")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE PUNTOS DE USUARIOS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = UserPointsExtractor()

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

        subtitle("📤 PASO 2: Extrayendo puntos de usuarios de PostgreSQL (monolito)")
        user_points_data = extractor.extract_user_points_data()
        info(f"✅ Extraídos {len(user_points_data)} registros de puntos de usuarios")

        report_builder.extraction_completed("user_points", len(user_points_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-points PostgreSQL")
        transformer = UserPointsTransformer()

        transformed_user_points, transformed_transactions, transformed_transaction_payments = transformer.transform_user_points_data(user_points_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("user_points", transform_summary['user_points_transformed'], total_errors)
        report_builder.transformation_completed("transactions", transform_summary['transactions_transformed'], total_errors)
        report_builder.transformation_completed("transaction_payments", transform_summary['transaction_payments_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(
            transformed_user_points, transformed_transactions, transformed_transaction_payments)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['user_points_transformed']} user_points, {transform_summary['transactions_transformed']} transactions, {transform_summary['transaction_payments_transformed']} transaction_payments")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-points)")
        loader = UserPointsLoader()

        user_points_result = loader.load_user_points(transformed_user_points, clear_existing=True)

        if not user_points_result['success']:
            failure("❌ Error en la carga de user_points")
            if 'error' in user_points_result:
                failure(f"Error: {user_points_result['error']}")
            
            report_builder.loading_completed("user_points", 0, 0, 1)
            report_builder.add_validation_errors([user_points_result.get('error', 'Error en carga de user_points')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        user_points_inserted = user_points_result.get('inserted_count', 0)
        user_points_deleted = user_points_result.get('deleted_count', 0)
        report_builder.loading_completed("user_points", user_points_inserted, user_points_deleted)

        info(f"✅ User_points cargados: {user_points_inserted} insertados")

        transactions_result = loader.load_transactions(transformed_transactions)

        if not transactions_result['success']:
            failure("❌ Error en la carga de transactions")
            if 'error' in transactions_result:
                failure(f"Error: {transactions_result['error']}")
            
            report_builder.loading_completed("transactions", 0, 0, 1)
            report_builder.add_validation_errors([transactions_result.get('error', 'Error en carga de transactions')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        transactions_inserted = transactions_result.get('inserted_count', 0)
        report_builder.loading_completed("transactions", transactions_inserted, 0)

        info(f"✅ Transactions cargadas: {transactions_inserted} insertadas")

        transaction_payments_result = loader.load_transaction_payments(transformed_transaction_payments)

        if not transaction_payments_result['success']:
            failure("❌ Error en la carga de transaction_payments")
            if 'error' in transaction_payments_result:
                failure(f"Error: {transaction_payments_result['error']}")
            
            report_builder.loading_completed("transaction_payments", 0, 0, 1)
            report_builder.add_validation_errors([transaction_payments_result.get('error', 'Error en carga de transaction_payments')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        transaction_payments_inserted = transaction_payments_result.get('inserted_count', 0)
        report_builder.loading_completed("transaction_payments", transaction_payments_inserted, 0)

        info(f"✅ Transaction_payments cargados: {transaction_payments_inserted} insertados")

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

        success("🎉 === MIGRACIÓN DE PUNTOS DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        info(f"👥 User_points migrados: {integrity_validation['stats']['total_user_points']}")
        info(f"📊 Transactions migradas: {integrity_validation['stats']['total_transactions']}")
        info(f"💳 Transaction_payments migrados: {integrity_validation['stats']['total_transaction_payments']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de puntos de usuarios: {str(e)}")
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

        from src.shared.payment_service import PaymentService

        payment_service = PaymentService()
        test_payment = payment_service.get_payment_by_id(1)
        payment_service.close_connection()

        info("✅ Servicio de pagos disponible")
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

        from src.connections.points_postgres_connection import PointsPostgresConnection
        points_conn = PointsPostgresConnection()
        points_conn.connect()
        info("✅ Conexión a ms-points (PostgreSQL) exitosa")
        points_conn.disconnect()

        from src.connections.mongo_connection import MongoConnection
        mongo_conn = MongoConnection()
        mongo_conn.connect()
        info("✅ Conexión a ms-users (MongoDB) exitosa")
        mongo_conn.disconnect()

        from src.connections.payments_postgres_connection import PaymentsPostgresConnection
        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()
        info("✅ Conexión a ms-payments (PostgreSQL) exitosa")
        payments_conn.disconnect()

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
        print("\n🎉 ¡Migración de puntos de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de puntos de usuarios falló. Revisa los logs.")
        sys.exit(1)
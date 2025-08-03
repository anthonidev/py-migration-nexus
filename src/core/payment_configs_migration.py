from src.utils.logger import title, subtitle, success, failure,  info
from src.loaders.payment_configs_loader import PaymentConfigsLoader
from src.transformers.payment_configs_transformer import PaymentConfigsTransformer
from src.extractors.payment_configs_extractor import PaymentConfigsExtractor
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


def main():
    report_builder = MigrationReportBuilder("payment_configs")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE CONFIGURACIONES DE PAGO ===")
        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = PaymentConfigsExtractor()

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

        subtitle("📤 PASO 2: Extrayendo configuraciones de pago de PostgreSQL (monolito)")
        configs_data = extractor.extract_payment_configs()
        info(f"✅ Extraídas {len(configs_data)} configuraciones de pago")

        report_builder.extraction_completed("payment_configs", len(configs_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-payments PostgreSQL")
        transformer = PaymentConfigsTransformer()

        transformed_configs = transformer.transform_payment_configs(configs_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("payment_configs", transform_summary['configs_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(transformed_configs)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['configs_transformed']} configuraciones")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-payments)")
        loader = PaymentConfigsLoader()

        load_result = loader.load_payment_configs(transformed_configs, clear_existing=True)

        if not load_result['success']:
            failure("❌ Error en la carga de configuraciones")
            if 'error' in load_result:
                failure(f"Error: {load_result['error']}")
            
            report_builder.loading_completed("payment_configs", 0, 0, 1)
            report_builder.add_validation_errors([load_result.get('error', 'Error en carga')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        inserted_count = load_result.get('inserted_count', 0)
        deleted_count = load_result.get('deleted_count', 0)
        report_builder.loading_completed("payment_configs", inserted_count, deleted_count)

        info(f"✅ Configuraciones cargadas: {inserted_count} insertadas")

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

        success("🎉 === MIGRACIÓN DE CONFIGURACIONES DE PAGO COMPLETADA EXITOSAMENTE ===")
        info(f"📊 Configuraciones migradas: {integrity_validation['stats']['total_configs']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de configuraciones de pago: {str(e)}")
        report_builder.add_validation_errors([f"Error crítico: {str(e)}"])
        report_builder.mark_failure()
        report_builder.build().save_to_file()
        
        return False

    finally:
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'loader' in locals():
                loader.close_connection()
        except Exception as e:
            failure(f"Error cerrando conexiones: {str(e)}")

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

    success = main()

    if success:
        print("\n🎉 ¡Migración de configuraciones de pago completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de configuraciones de pago falló. Revisa los logs.")
        sys.exit(1)
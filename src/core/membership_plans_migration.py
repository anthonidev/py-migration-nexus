from src.loaders.membership_plans_loader import MembershipPlansLoader
from src.transformers.membership_plans_transformer import MembershipPlansTransformer
from src.extractors.membership_plans_extractor import MembershipPlansExtractor
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
    report_builder = MigrationReportBuilder("membership_plans")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE PLANES DE MEMBRESÍA ===")
        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = MembershipPlansExtractor()

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

        subtitle("📤 PASO 2: Extrayendo planes de membresía de PostgreSQL (monolito)")
        plans_data = extractor.extract_membership_plans()
        info(f"✅ Extraídos {len(plans_data)} planes de membresía")

        report_builder.extraction_completed("membership_plans", len(plans_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-membership PostgreSQL")
        transformer = MembershipPlansTransformer()

        transformed_plans = transformer.transform_membership_plans(plans_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("membership_plans", transform_summary['plans_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(transformed_plans)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['plans_transformed']} planes")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-membership)")
        loader = MembershipPlansLoader()

        load_result = loader.load_membership_plans(transformed_plans, clear_existing=True)

        if not load_result['success']:
            failure("❌ Error en la carga de planes")
            if 'error' in load_result:
                failure(f"Error: {load_result['error']}")
            
            report_builder.loading_completed("membership_plans", 0, 0, 1)
            report_builder.add_validation_errors([load_result.get('error', 'Error en carga')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        inserted_count = load_result.get('inserted_count', 0)
        deleted_count = load_result.get('deleted_count', 0)
        report_builder.loading_completed("membership_plans", inserted_count, deleted_count)

        info(f"✅ Planes cargados: {inserted_count} insertados")

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

        # 6. FINALIZACIÓN EXITOSA
        success("🎉 === MIGRACIÓN DE PLANES DE MEMBRESÍA COMPLETADA EXITOSAMENTE ===")
        info(f"📋 Planes migrados: {integrity_validation['stats']['total_plans']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de planes de membresía: {str(e)}")
        
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

        from src.connections.membership_postgres_connection import MembershipPostgresConnection
        membership_conn = MembershipPostgresConnection()
        membership_conn.connect()
        info("✅ Conexión a ms-membership (PostgreSQL) exitosa")
        membership_conn.disconnect()

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
        print("\n🎉 ¡Migración de planes de membresía completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de planes de membresía falló. Revisa los logs.")
        sys.exit(1)
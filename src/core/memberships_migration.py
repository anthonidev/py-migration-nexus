import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.memberships_extractor import MembershipsExtractor
from src.transformers.memberships_transformer import MembershipsTransformer
from src.loaders.memberships_loader import MembershipsLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("memberships")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE MEMBRESÍAS DE USUARIOS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = MembershipsExtractor()

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

        subtitle("📤 PASO 2: Extrayendo membresías de PostgreSQL (monolito)")
        memberships_data = extractor.extract_memberships_data()
        info(f"✅ Extraídas {len(memberships_data)} membresías")

        report_builder.extraction_completed("memberships", len(memberships_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-membership PostgreSQL")
        transformer = MembershipsTransformer()

        transformed_memberships, transformed_reconsumptions, transformed_history = transformer.transform_memberships_data(memberships_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("memberships", transform_summary['memberships_transformed'], total_errors)
        report_builder.transformation_completed("reconsumptions", transform_summary['reconsumptions_transformed'], total_errors)
        report_builder.transformation_completed("membership_history", transform_summary['history_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(
            transformed_memberships, transformed_reconsumptions, transformed_history)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['memberships_transformed']} membresías, {transform_summary['reconsumptions_transformed']} reconsumptions, {transform_summary['history_transformed']} history")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-membership)")
        loader = MembershipsLoader()

        memberships_result = loader.load_memberships(transformed_memberships, clear_existing=True)

        if not memberships_result['success']:
            failure("❌ Error en la carga de membresías")
            if 'error' in memberships_result:
                failure(f"Error: {memberships_result['error']}")
            
            report_builder.loading_completed("memberships", 0, 0, 1)
            report_builder.add_validation_errors([memberships_result.get('error', 'Error en carga de membresías')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        memberships_inserted = memberships_result.get('inserted_count', 0)
        memberships_deleted = memberships_result.get('deleted_count', 0)
        report_builder.loading_completed("memberships", memberships_inserted, memberships_deleted)

        info(f"✅ Membresías cargadas: {memberships_inserted} insertadas")

        reconsumptions_result = loader.load_reconsumptions(transformed_reconsumptions)

        if not reconsumptions_result['success']:
            failure("❌ Error en la carga de reconsumptions")
            if 'error' in reconsumptions_result:
                failure(f"Error: {reconsumptions_result['error']}")
            
            report_builder.loading_completed("reconsumptions", 0, 0, 1)
            report_builder.add_validation_errors([reconsumptions_result.get('error', 'Error en carga de reconsumptions')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        reconsumptions_inserted = reconsumptions_result.get('inserted_count', 0)
        report_builder.loading_completed("reconsumptions", reconsumptions_inserted, 0)

        info(f"✅ Reconsumptions cargados: {reconsumptions_inserted} insertados")

        history_result = loader.load_history(transformed_history)

        if not history_result['success']:
            failure("❌ Error en la carga de historial")
            if 'error' in history_result:
                failure(f"Error: {history_result['error']}")
            
            report_builder.loading_completed("membership_history", 0, 0, 1)
            report_builder.add_validation_errors([history_result.get('error', 'Error en carga de historial')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        history_inserted = history_result.get('inserted_count', 0)
        report_builder.loading_completed("membership_history", history_inserted, 0)

        info(f"✅ Historial cargado: {history_inserted} registros insertados")

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

        success("🎉 === MIGRACIÓN DE MEMBRESÍAS DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        info(f"👥 Membresías migradas: {integrity_validation['stats']['total_memberships']}")
        info(f"🔄 Reconsumptions migrados: {integrity_validation['stats']['total_reconsumptions']}")
        info(f"📋 Registros de historial migrados: {integrity_validation['stats']['total_history']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de membresías: {str(e)}")
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
        from src.connections.membership_postgres_connection import MembershipPostgresConnection

        membership_conn = MembershipPostgresConnection()
        membership_conn.connect()

        check_plans_query = "SELECT COUNT(*) FROM membership_plans"
        plans_count, _ = membership_conn.execute_query(check_plans_query)

        if plans_count[0][0] == 0:
            failure("❌ No hay planes de membresía en ms-membership")
            failure("💡 Ejecuta primero la migración de planes de membresía")
            return False

        info(f"✅ Encontrados {plans_count[0][0]} planes de membresía en ms-membership")
        membership_conn.disconnect()

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

        from src.connections.membership_postgres_connection import MembershipPostgresConnection
        membership_conn = MembershipPostgresConnection()
        membership_conn.connect()
        info("✅ Conexión a ms-membership (PostgreSQL) exitosa")
        membership_conn.disconnect()

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
        print("\n🎉 ¡Migración de membresías de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de membresías de usuarios falló. Revisa los logs.")
        sys.exit(1)
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.weekly_volumes_extractor import WeeklyVolumesExtractor
from src.transformers.weekly_volumes_transformer import WeeklyVolumesTransformer
from src.loaders.weekly_volumes_loader import WeeklyVolumesLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("weekly_volumes")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE VOLÚMENES SEMANALES ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = WeeklyVolumesExtractor()

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

        subtitle("📤 PASO 2: Extrayendo volúmenes semanales de PostgreSQL (monolito)")
        weekly_volumes_data = extractor.extract_weekly_volumes_data()
        info(f"✅ Extraídos {len(weekly_volumes_data)} volúmenes semanales")

        report_builder.extraction_completed("weekly_volumes", len(weekly_volumes_data))

        subtitle("🔄 PASO 3: Transformando datos para ms-points PostgreSQL")
        transformer = WeeklyVolumesTransformer()

        transformed_volumes, transformed_history = transformer.transform_weekly_volumes_data(weekly_volumes_data)

        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("weekly_volumes", transform_summary['weekly_volumes_transformed'], total_errors)
        report_builder.transformation_completed("volume_history", transform_summary['history_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        transformation_validation = transformer.validate_transformation(
            transformed_volumes, transformed_history)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        info(f"✅ Transformación completada: {transform_summary['weekly_volumes_transformed']} volúmenes, {transform_summary['history_transformed']} history")

        subtitle("📥 PASO 4: Cargando datos en PostgreSQL (ms-points)")
        loader = WeeklyVolumesLoader()

        volumes_result = loader.load_weekly_volumes(transformed_volumes, clear_existing=True)

        if not volumes_result['success']:
            failure("❌ Error en la carga de volúmenes semanales")
            if 'error' in volumes_result:
                failure(f"Error: {volumes_result['error']}")
            
            report_builder.loading_completed("weekly_volumes", 0, 0, 1)
            report_builder.add_validation_errors([volumes_result.get('error', 'Error en carga de volúmenes')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        volumes_inserted = volumes_result.get('inserted_count', 0)
        volumes_deleted = volumes_result.get('deleted_count', 0)
        report_builder.loading_completed("weekly_volumes", volumes_inserted, volumes_deleted)

        info(f"✅ Volúmenes semanales cargados: {volumes_inserted} insertados")

        history_result = loader.load_volume_history(transformed_history)

        if not history_result['success']:
            failure("❌ Error en la carga de historial de volúmenes")
            if 'error' in history_result:
                failure(f"Error: {history_result['error']}")
            
            report_builder.loading_completed("volume_history", 0, 0, 1)
            report_builder.add_validation_errors([history_result.get('error', 'Error en carga de historial')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        history_inserted = history_result.get('inserted_count', 0)
        report_builder.loading_completed("volume_history", history_inserted, 0)

        info(f"✅ Historial de volúmenes cargado: {history_inserted} insertados")

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

        success("🎉 === MIGRACIÓN DE VOLÚMENES SEMANALES COMPLETADA EXITOSAMENTE ===")
        info(f"📊 Volúmenes semanales migrados: {integrity_validation['stats']['total_weekly_volumes']}")
        info(f"📋 Registros de historial migrados: {integrity_validation['stats']['total_volume_history']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de volúmenes semanales: {str(e)}")
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
        print("\n🎉 ¡Migración de volúmenes semanales completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de volúmenes semanales falló. Revisa los logs.")
        sys.exit(1)
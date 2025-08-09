# src/core/ranks_migration.py
import os
import sys

# Agregamos el directorio padre al path para poder importar los módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.ranks_extractor import RanksExtractor
from src.transformers.ranks_transformer import RanksTransformer
from src.loaders.ranks_loader import RanksLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    """Función principal de migración de ranks desde JSON a PostgreSQL"""
    report_builder = MigrationReportBuilder("ranks")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE RANKS ===")
        
        # PASO 1: Validación de archivo JSON
        subtitle("🔍 PASO 1: Validando archivo JSON de origen")
        extractor = RanksExtractor()
        
        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            failure("❌ Validación de archivo JSON fallida")
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

        # PASO 2: Extracción de datos desde JSON
        subtitle("📤 PASO 2: Extrayendo datos desde archivo JSON")
        ranks_data = extractor.extract_ranks_data()
        info(f"✅ Extraídos {len(ranks_data)} ranks desde src/data/ranks.json")
        
        report_builder.extraction_completed("ranks", len(ranks_data))
        
        # PASO 3: Transformación de datos
        subtitle("🔄 PASO 3: Transformando datos para ms-points")
        transformer = RanksTransformer()
        
        transformation_result = transformer.transform_ranks_data(ranks_data)
        
        # Procesar el resumen de transformación para los reportes
        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        if total_errors > 0:
            failure(f"❌ Error durante la transformación: {total_errors} errores encontrados")
            for error in errors:
                failure(f"   - {error}")
            
            report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
            
        if warnings:
            for warning in warnings:
                info(f"⚠️ {warning}")
        
        transformed_ranks = transformation_result
        info(f"✅ Transformados {len(transformed_ranks)} ranks exitosamente")
        
        report_builder.transformation_completed("ranks", len(transformed_ranks))
        report_builder.add_validation_warnings(warnings)

        # PASO 4: Carga de datos en PostgreSQL
        subtitle("📥 PASO 4: Cargando datos en ms-points (PostgreSQL)")
        loader = RanksLoader()
        
        loading_result = loader.load_ranks(transformed_ranks, clear_existing=True)
        
        if not loading_result['success']:
            failure("❌ Error durante la carga de datos")
            for error in loading_result.get('errors', []):
                failure(f"   - {error}")
            
            report_builder.add_validation_errors(loading_result.get('errors', []))
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        inserted_count = loading_result['inserted_count']
        deleted_count = loading_result.get('deleted_count', 0)
        
        info(f"✅ Cargados {inserted_count} ranks en PostgreSQL")
        if deleted_count > 0:
            info(f"🗑️ Eliminados {deleted_count} ranks existentes")
        
        report_builder.loading_completed("ranks", inserted_count, deleted_count)

        # PASO 5: Validación de integridad de datos
        subtitle("🔍 PASO 5: Validando integridad de datos migrados")
        integrity_validation = loader.validate_data_integrity(ranks_data)
        
        if not integrity_validation['valid']:
            failure("❌ Validación de integridad fallida")
            for error in integrity_validation['errors']:
                failure(f"   - {error}")
            
            report_builder.add_validation_errors(integrity_validation['errors'])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        # FINALIZACIÓN EXITOSA
        success("🎉 === MIGRACIÓN DE RANKS COMPLETADA EXITOSAMENTE ===")
        info(f"📋 Ranks migrados: {integrity_validation['stats']['total_ranks']}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()

        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de ranks: {str(e)}")
        
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
    """Prueba las conexiones a las bases de datos requeridas"""
    info("🔍 Probando conexiones a bases de datos...")

    try:
        from src.connections.points_postgres_connection import PointsPostgresConnection
        points_conn = PointsPostgresConnection()
        points_conn.connect()
        info("✅ Conexión a ms-points (PostgreSQL) exitosa")
        points_conn.disconnect()

        return True

    except Exception as e:
        failure(f"❌ Error en conexiones: {str(e)}")
        return False


def check_json_file():
    """Verifica que el archivo JSON de ranks existe"""
    info("🔍 Verificando archivo JSON de ranks...")
    
    try:
        json_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "src", "data", "ranks.json"
        )
        
        if not os.path.exists(json_file_path):
            failure(f"❌ Archivo JSON no encontrado: {json_file_path}")
            return False
        
        info(f"✅ Archivo JSON encontrado: {json_file_path}")
        return True
        
    except Exception as e:
        failure(f"❌ Error verificando archivo JSON: {str(e)}")
        return False


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if not check_json_file():
        sys.exit(1)

    if not test_connections():
        sys.exit(1)

    success = main()

    if success:
        print("\n🎉 ¡Migración de ranks completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de ranks falló. Revisa los logs.")
        sys.exit(1)
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.weekly_volumes_extractor import WeeklyVolumesExtractor
from src.transformers.weekly_volumes_transformer import WeeklyVolumesTransformer
from src.loaders.weekly_volumes_loader import WeeklyVolumesLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE VOLÚMENES SEMANALES ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = WeeklyVolumesExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo volúmenes semanales de PostgreSQL (monolito)")
        weekly_volumes_data = extractor.extract_weekly_volumes_data()
        logger.info(f"✅ Extraídos {len(weekly_volumes_data)} volúmenes semanales")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-points PostgreSQL")
        transformer = WeeklyVolumesTransformer()

        transformed_volumes, transformed_history = transformer.transform_weekly_volumes_data(weekly_volumes_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(
            transformed_volumes, transformed_history)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['weekly_volumes_transformed']} volúmenes, {transform_summary['history_transformed']} history")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-points)")
        loader = WeeklyVolumesLoader()

        # Cargar volúmenes semanales
        volumes_result = loader.load_weekly_volumes(transformed_volumes, clear_existing=True)

        if not volumes_result['success']:
            logger.error("❌ Error en la carga de volúmenes semanales")
            if 'error' in volumes_result:
                logger.error(f"Error: {volumes_result['error']}")
            return False

        logger.info(f"✅ Volúmenes semanales cargados: {volumes_result['inserted_count']} insertados")

        # Cargar historial de volúmenes
        history_result = loader.load_volume_history(transformed_history)

        if not history_result['success']:
            logger.error("❌ Error en la carga de historial de volúmenes")
            if 'error' in history_result:
                logger.error(f"Error: {history_result['error']}")
            return False

        logger.info(f"✅ Historial de volúmenes cargado: {history_result['inserted_count']} insertados")

        # 5. VALIDACIÓN POST-CARGA
        logger.info("✅ PASO 5: Validando integridad de datos")
        integrity_validation = loader.validate_data_integrity()

        if not integrity_validation['valid']:
            logger.error("❌ Validación de integridad fallida")
            for error in integrity_validation['errors']:
                logger.error(f"   - {error}")
            return False

        # 6. RESULTADOS
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("🎉 === MIGRACIÓN DE VOLÚMENES SEMANALES COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📊 Volúmenes semanales migrados: {integrity_validation['stats']['total_weekly_volumes']}")
        logger.info(f"📋 Registros de historial migrados: {integrity_validation['stats']['total_volume_history']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'weekly_volumes_migrated': integrity_validation['stats']['total_weekly_volumes'],
                'volume_history_migrated': integrity_validation['stats']['total_volume_history']
            },
            'extraction': {
                'total_extracted': len(weekly_volumes_data)
            },
            'transformation': {
                'weekly_volumes_transformed': transform_summary['weekly_volumes_transformed'],
                'history_transformed': transform_summary['history_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'volumes_result': {
                    'success': volumes_result['success'],
                    'inserted_count': volumes_result['inserted_count'],
                    'deleted_count': volumes_result['deleted_count']
                },
                'history_result': {
                    'success': history_result['success'],
                    'inserted_count': history_result['inserted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })

        return True

    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de volúmenes semanales: {str(e)}")
        logger.exception("Detalles del error:")
        return False

    finally:
        # Cerrar conexiones
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'transformer' in locals():
                transformer.close_connections()
            if 'loader' in locals():
                loader.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexiones: {str(e)}")

def save_migration_report(report_data, filename_prefix="weekly_volumes_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_POINTS', 'MS_NEXUS_USER']
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        logger.error("❌ Variables de entorno faltantes:")
        for var in missing_vars:
            logger.error(f"   - {var}")
        return False

    return True

def check_dependencies():
    """Verifica que las dependencias estén disponibles"""
    logger.info("🔍 Verificando dependencias...")

    try:
        # Verificar que existan usuarios en MongoDB
        from src.shared.user_service import UserService

        user_service = UserService()
        # Hacer una búsqueda de prueba
        test_result = user_service.get_user_by_email("test@test.com")
        user_service.close_connection()

        logger.info("✅ Servicio de usuarios disponible")
        return True

    except Exception as e:
        logger.error(f"❌ Error verificando dependencias: {str(e)}")
        return False

def test_connections():
    """Prueba las conexiones a las bases de datos"""
    logger.info("🔍 Probando conexiones a bases de datos...")

    try:
        # Probar conexión al monolito
        from src.connections.postgres_connection import PostgresConnection
        monolito_conn = PostgresConnection()
        monolito_conn.connect()
        logger.info("✅ Conexión al monolito (PostgreSQL) exitosa")
        monolito_conn.disconnect()

        # Probar conexión a ms-points
        from src.connections.points_postgres_connection import PointsPostgresConnection
        points_conn = PointsPostgresConnection()
        points_conn.connect()
        logger.info("✅ Conexión a ms-points (PostgreSQL) exitosa")
        points_conn.disconnect()

        # Probar conexión a ms-users (MongoDB)
        from src.connections.mongo_connection import MongoConnection
        mongo_conn = MongoConnection()
        mongo_conn.connect()
        logger.info("✅ Conexión a ms-users (MongoDB) exitosa")
        mongo_conn.disconnect()

        return True

    except Exception as e:
        logger.error(f"❌ Error en conexiones: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if not validate_environment():
        sys.exit(1)

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
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.memberships_extractor import MembershipsExtractor
from src.transformers.memberships_transformer import MembershipsTransformer
from src.loaders.memberships_loader import MembershipsLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE MEMBRESÍAS DE USUARIOS ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = MembershipsExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo membresías de PostgreSQL (monolito)")
        memberships_data = extractor.extract_memberships_data()
        logger.info(f"✅ Extraídas {len(memberships_data)} membresías")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-membership PostgreSQL")
        transformer = MembershipsTransformer()

        transformed_memberships, transformed_reconsumptions, transformed_history = transformer.transform_memberships_data(memberships_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(
            transformed_memberships, transformed_reconsumptions, transformed_history)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['memberships_transformed']} membresías, {transform_summary['reconsumptions_transformed']} reconsumptions, {transform_summary['history_transformed']} history")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-membership)")
        loader = MembershipsLoader()

        # Cargar membresías
        memberships_result = loader.load_memberships(transformed_memberships, clear_existing=True)

        if not memberships_result['success']:
            logger.error("❌ Error en la carga de membresías")
            if 'error' in memberships_result:
                logger.error(f"Error: {memberships_result['error']}")
            return False

        logger.info(f"✅ Membresías cargadas: {memberships_result['inserted_count']} insertadas")

        # Cargar reconsumptions
        reconsumptions_result = loader.load_reconsumptions(transformed_reconsumptions)

        if not reconsumptions_result['success']:
            logger.error("❌ Error en la carga de reconsumptions")
            if 'error' in reconsumptions_result:
                logger.error(f"Error: {reconsumptions_result['error']}")
            return False

        logger.info(f"✅ Reconsumptions cargados: {reconsumptions_result['inserted_count']} insertados")

        # Cargar historial
        history_result = loader.load_history(transformed_history)

        if not history_result['success']:
            logger.error("❌ Error en la carga de historial")
            if 'error' in history_result:
                logger.error(f"Error: {history_result['error']}")
            return False

        logger.info(f"✅ Historial cargado: {history_result['inserted_count']} registros insertados")

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

        logger.info("🎉 === MIGRACIÓN DE MEMBRESÍAS DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"👥 Membresías migradas: {integrity_validation['stats']['total_memberships']}")
        logger.info(f"🔄 Reconsumptions migrados: {integrity_validation['stats']['total_reconsumptions']}")
        logger.info(f"📋 Registros de historial migrados: {integrity_validation['stats']['total_history']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'memberships_migrated': integrity_validation['stats']['total_memberships'],
                'reconsumptions_migrated': integrity_validation['stats']['total_reconsumptions'],
                'history_migrated': integrity_validation['stats']['total_history']
            },
            'extraction': {
                'total_extracted': len(memberships_data)
            },
            'transformation': {
                'memberships_transformed': transform_summary['memberships_transformed'],
                'reconsumptions_transformed': transform_summary['reconsumptions_transformed'],
                'history_transformed': transform_summary['history_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'memberships_result': {
                    'success': memberships_result['success'],
                    'inserted_count': memberships_result['inserted_count'],
                    'deleted_count': memberships_result['deleted_count']
                },
                'reconsumptions_result': {
                    'success': reconsumptions_result['success'],
                    'inserted_count': reconsumptions_result['inserted_count']
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
        logger.error(f"💥 Error crítico durante la migración de membresías: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="memberships_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_MEMBERSHIP', 'MS_NEXUS_USER']
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
        # Verificar que existan planes de membresía
        from src.connections.membership_postgres_connection import MembershipPostgresConnection

        membership_conn = MembershipPostgresConnection()
        membership_conn.connect()

        check_plans_query = "SELECT COUNT(*) FROM membership_plans"
        plans_count, _ = membership_conn.execute_query(check_plans_query)

        if plans_count[0][0] == 0:
            logger.error("❌ No hay planes de membresía en ms-membership")
            logger.error("💡 Ejecuta primero la migración de planes de membresía")
            return False

        logger.info(f"✅ Encontrados {plans_count[0][0]} planes de membresía en ms-membership")
        membership_conn.disconnect()

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

        # Probar conexión a ms-membership
        from src.connections.membership_postgres_connection import MembershipPostgresConnection
        membership_conn = MembershipPostgresConnection()
        membership_conn.connect()
        logger.info("✅ Conexión a ms-membership (PostgreSQL) exitosa")
        membership_conn.disconnect()

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
        print("\n🎉 ¡Migración de membresías de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de membresías de usuarios falló. Revisa los logs.")
        sys.exit(1)
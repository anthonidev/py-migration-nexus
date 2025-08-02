from src.utils.logger import get_logger
from src.loaders.membership_plans_loader import MembershipPlansLoader
from src.transformers.membership_plans_transformer import MembershipPlansTransformer
from src.extractors.membership_plans_extractor import MembershipPlansExtractor
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE PLANES DE MEMBRESÍA ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = MembershipPlansExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo planes de membresía de PostgreSQL (monolito)")
        plans_data = extractor.extract_membership_plans()
        logger.info(f"✅ Extraídos {len(plans_data)} planes de membresía")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-membership PostgreSQL")
        transformer = MembershipPlansTransformer()

        transformed_plans = transformer.transform_membership_plans(plans_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(transformed_plans)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['plans_transformed']} planes")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-membership)")
        loader = MembershipPlansLoader()

        load_result = loader.load_membership_plans(transformed_plans, clear_existing=True)

        if not load_result['success']:
            logger.error("❌ Error en la carga de planes")
            if 'error' in load_result:
                logger.error(f"Error: {load_result['error']}")
            return False

        logger.info(f"✅ Planes cargados: {load_result['inserted_count']} insertados")

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

        logger.info("🎉 === MIGRACIÓN DE PLANES DE MEMBRESÍA COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📋 Planes migrados: {integrity_validation['stats']['total_plans']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'plans_migrated': integrity_validation['stats']['total_plans']
            },
            'extraction': {
                'total_extracted': len(plans_data)
            },
            'transformation': {
                'plans_transformed': transform_summary['plans_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'load_result': {
                    'success': load_result['success'],
                    'inserted_count': load_result['inserted_count'],
                    'deleted_count': load_result['deleted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })

        return True

    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de planes de membresía: {str(e)}")
        logger.exception("Detalles del error:")
        return False

    finally:
        # Cerrar conexiones
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'loader' in locals():
                loader.close_connection()
        except Exception as e:
            logger.error(f"Error cerrando conexiones: {str(e)}")

def save_migration_report(report_data, filename_prefix="membership_plans_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_MEMBERSHIP']
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

    success = main()

    if success:
        print("\n🎉 ¡Migración de planes de membresía completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de planes de membresía falló. Revisa los logs.")
        sys.exit(1)
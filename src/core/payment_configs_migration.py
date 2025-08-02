from src.utils.logger import get_logger
from src.loaders.payment_configs_loader import PaymentConfigsLoader
from src.transformers.payment_configs_transformer import PaymentConfigsTransformer
from src.extractors.payment_configs_extractor import PaymentConfigsExtractor
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE CONFIGURACIONES DE PAGO ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = PaymentConfigsExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo configuraciones de pago de PostgreSQL (monolito)")
        configs_data = extractor.extract_payment_configs()
        logger.info(f"✅ Extraídas {len(configs_data)} configuraciones de pago")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-payments PostgreSQL")
        transformer = PaymentConfigsTransformer()

        transformed_configs = transformer.transform_payment_configs(configs_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(transformed_configs)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['configs_transformed']} configuraciones")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-payments)")
        loader = PaymentConfigsLoader()

        load_result = loader.load_payment_configs(transformed_configs, clear_existing=True)

        if not load_result['success']:
            logger.error("❌ Error en la carga de configuraciones")
            if 'error' in load_result:
                logger.error(f"Error: {load_result['error']}")
            return False

        logger.info(f"✅ Configuraciones cargadas: {load_result['inserted_count']} insertadas")

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

        logger.info("🎉 === MIGRACIÓN DE CONFIGURACIONES DE PAGO COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📊 Configuraciones migradas: {integrity_validation['stats']['total_configs']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'configs_migrated': integrity_validation['stats']['total_configs']
            },
            'extraction': {
                'total_extracted': len(configs_data)
            },
            'transformation': {
                'configs_transformed': transform_summary['configs_transformed'],
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
        logger.error(f"💥 Error crítico durante la migración de configuraciones de pago: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="payment_configs_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS']
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

        # Probar conexión a ms-payments
        from src.connections.payments_postgres_connection import PaymentsPostgresConnection
        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()
        logger.info("✅ Conexión a ms-payments (PostgreSQL) exitosa")
        payments_conn.disconnect()

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
        print("\n🎉 ¡Migración de configuraciones de pago completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de configuraciones de pago falló. Revisa los logs.")
        sys.exit(1)
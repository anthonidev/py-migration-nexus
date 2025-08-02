from src.utils.logger import get_logger
from src.loaders.payments_loader import PaymentsLoader
from src.transformers.payments_transformer import PaymentsTransformer
from src.extractors.payments_extractor import PaymentsExtractor
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE PAGOS ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = PaymentsExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo pagos de PostgreSQL (monolito)")
        payments_data = extractor.extract_payments_data()
        logger.info(f"✅ Extraídos {len(payments_data)} pagos")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-payments PostgreSQL")
        transformer = PaymentsTransformer()

        transformed_payments, transformed_payment_items = transformer.transform_payments_data(payments_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(
            transformed_payments, transformed_payment_items)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['payments_transformed']} pagos, {transform_summary['payment_items_transformed']} items")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-payments)")
        loader = PaymentsLoader()

        # Cargar pagos
        payments_result = loader.load_payments(transformed_payments, clear_existing=True)

        if not payments_result['success']:
            logger.error("❌ Error en la carga de pagos")
            if 'error' in payments_result:
                logger.error(f"Error: {payments_result['error']}")
            return False

        logger.info(f"✅ Pagos cargados: {payments_result['inserted_count']} insertados")

        # Cargar items de pago
        items_result = loader.load_payment_items(transformed_payment_items)

        if not items_result['success']:
            logger.error("❌ Error en la carga de items de pago")
            if 'error' in items_result:
                logger.error(f"Error: {items_result['error']}")
            return False

        logger.info(f"✅ Items de pago cargados: {items_result['inserted_count']} insertados")

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

        logger.info("🎉 === MIGRACIÓN DE PAGOS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"💳 Pagos migrados: {integrity_validation['stats']['total_payments']}")
        logger.info(f"📋 Items migrados: {integrity_validation['stats']['total_payment_items']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'payments_migrated': integrity_validation['stats']['total_payments'],
                'payment_items_migrated': integrity_validation['stats']['total_payment_items']
            },
            'extraction': {
                'total_extracted': len(payments_data)
            },
            'transformation': {
                'payments_transformed': transform_summary['payments_transformed'],
                'payment_items_transformed': transform_summary['payment_items_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'payments_result': {
                    'success': payments_result['success'],
                    'inserted_count': payments_result['inserted_count'],
                    'deleted_count': payments_result['deleted_count']
                },
                'items_result': {
                    'success': items_result['success'],
                    'inserted_count': items_result['inserted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })

        return True

    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de pagos: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="payments_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS', 'MS_NEXUS_USER']
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
        # Verificar que existan configuraciones de pago
        from src.connections.payments_postgres_connection import PaymentsPostgresConnection

        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()

        check_configs_query = "SELECT COUNT(*) FROM payment_configs"
        configs_count, _ = payments_conn.execute_query(check_configs_query)

        if configs_count[0][0] == 0:
            logger.error("❌ No hay configuraciones de pago en ms-payments")
            logger.error("💡 Ejecuta primero la migración de configuraciones de pago")
            return False

        logger.info(f"✅ Encontradas {configs_count[0][0]} configuraciones de pago en ms-payments")
        payments_conn.disconnect()

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

        # Probar conexión a ms-payments
        from src.connections.payments_postgres_connection import PaymentsPostgresConnection
        payments_conn = PaymentsPostgresConnection()
        payments_conn.connect()
        logger.info("✅ Conexión a ms-payments (PostgreSQL) exitosa")
        payments_conn.disconnect()

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
        print("\n🎉 ¡Migración de pagos completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de pagos falló. Revisa los logs.")
        sys.exit(1)
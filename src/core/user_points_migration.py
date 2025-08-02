import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.user_points_extractor import UserPointsExtractor
from src.transformers.user_points_transformer import UserPointsTransformer
from src.loaders.user_points_loader import UserPointsLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE PUNTOS DE USUARIOS ===")
        start_time = datetime.now()

        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = UserPointsExtractor()

        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False

        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo puntos de usuarios de PostgreSQL (monolito)")
        user_points_data = extractor.extract_user_points_data()
        logger.info(f"✅ Extraídos {len(user_points_data)} registros de puntos de usuarios")

        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para ms-points PostgreSQL")
        transformer = UserPointsTransformer()

        transformed_user_points, transformed_transactions, transformed_transaction_payments = transformer.transform_user_points_data(user_points_data)

        # Validar transformación
        transformation_validation = transformer.validate_transformation(
            transformed_user_points, transformed_transactions, transformed_transaction_payments)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False

        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['user_points_transformed']} user_points, {transform_summary['transactions_transformed']} transactions, {transform_summary['transaction_payments_transformed']} transaction_payments")

        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en PostgreSQL (ms-points)")
        loader = UserPointsLoader()

        # Cargar user_points
        user_points_result = loader.load_user_points(transformed_user_points, clear_existing=True)

        if not user_points_result['success']:
            logger.error("❌ Error en la carga de user_points")
            if 'error' in user_points_result:
                logger.error(f"Error: {user_points_result['error']}")
            return False

        logger.info(f"✅ User_points cargados: {user_points_result['inserted_count']} insertados")

        # Cargar transactions
        transactions_result = loader.load_transactions(transformed_transactions)

        if not transactions_result['success']:
            logger.error("❌ Error en la carga de transactions")
            if 'error' in transactions_result:
                logger.error(f"Error: {transactions_result['error']}")
            return False

        logger.info(f"✅ Transactions cargadas: {transactions_result['inserted_count']} insertadas")

        # Cargar transaction_payments
        transaction_payments_result = loader.load_transaction_payments(transformed_transaction_payments)

        if not transaction_payments_result['success']:
            logger.error("❌ Error en la carga de transaction_payments")
            if 'error' in transaction_payments_result:
                logger.error(f"Error: {transaction_payments_result['error']}")
            return False

        logger.info(f"✅ Transaction_payments cargados: {transaction_payments_result['inserted_count']} insertados")

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

        logger.info("🎉 === MIGRACIÓN DE PUNTOS DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"👥 User_points migrados: {integrity_validation['stats']['total_user_points']}")
        logger.info(f"📊 Transactions migradas: {integrity_validation['stats']['total_transactions']}")
        logger.info(f"💳 Transaction_payments migrados: {integrity_validation['stats']['total_transaction_payments']}")

        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'user_points_migrated': integrity_validation['stats']['total_user_points'],
                'transactions_migrated': integrity_validation['stats']['total_transactions'],
                'transaction_payments_migrated': integrity_validation['stats']['total_transaction_payments']
            },
            'extraction': {
                'total_extracted': len(user_points_data)
            },
            'transformation': {
                'user_points_transformed': transform_summary['user_points_transformed'],
                'transactions_transformed': transform_summary['transactions_transformed'],
                'transaction_payments_transformed': transform_summary['transaction_payments_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'user_points_result': {
                    'success': user_points_result['success'],
                    'inserted_count': user_points_result['inserted_count'],
                    'deleted_count': user_points_result['deleted_count']
                },
                'transactions_result': {
                    'success': transactions_result['success'],
                    'inserted_count': transactions_result['inserted_count']
                },
                'transaction_payments_result': {
                    'success': transaction_payments_result['success'],
                    'inserted_count': transaction_payments_result['inserted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })

        return True

    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de puntos de usuarios: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="user_points_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"

    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)

    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_POINTS', 'MS_NEXUS_USER', 'MS_NEXUS_PAYMENTS']
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

        # Verificar que existan pagos en ms-payments
        from src.shared.payment_service import PaymentService

        payment_service = PaymentService()
        # Hacer una búsqueda de prueba
        test_payment = payment_service.get_payment_by_id(1)
        payment_service.close_connection()

        logger.info("✅ Servicio de pagos disponible")
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

        # Probar conexión a ms-payments (PostgreSQL)
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

    if not check_dependencies():
        sys.exit(1)

    success = main()

    if success:
        print("\n🎉 ¡Migración de puntos de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de puntos de usuarios falló. Revisa los logs.")
        sys.exit(1)
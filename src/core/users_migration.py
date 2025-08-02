import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.users_extractor import UsersExtractor
from src.transformers.users_transformer import UsersTransformer
from src.loaders.users_loader import UsersLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE USUARIOS ===")
        start_time = datetime.now()
        
        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = UsersExtractor()
        
        validation_result = extractor.validate_required_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False
        
        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo datos de PostgreSQL")
        users_data = extractor.extract_users_data()
        logger.info(f"✅ Extraídos {len(users_data)} usuarios")
        
        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para MongoDB")
        transformer = UsersTransformer()
        
        transformed_users, user_id_mapping = transformer.transform_users_data(users_data)
        
        # Validar transformación
        transformation_validation = transformer.validate_transformation(transformed_users)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False
        
        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['users_transformed']} usuarios")
        
        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en MongoDB")
        loader = UsersLoader()
        
        users_result = loader.load_users(transformed_users, clear_existing=True)
        
        if not users_result['success']:
            logger.error("❌ Error en la carga de usuarios")
            if 'error' in users_result:
                logger.error(f"Error: {users_result['error']}")
            return False
        
        logger.info(f"✅ Usuarios cargados: {users_result['inserted_count']} insertados")
        
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
        
        logger.info("🎉 === MIGRACIÓN DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📊 Usuarios migrados: {integrity_validation['stats']['total_users']}")
        
        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'users_migrated': integrity_validation['stats']['total_users']
            },
            'extraction': {
                'total_extracted': len(users_data)
            },
            'transformation': {
                'users_transformed': transform_summary['users_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'load_result': {
                    'success': users_result['success'],
                    'inserted_count': users_result['inserted_count'],
                    'deleted_count': users_result['deleted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de usuarios: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="users_migration_report"):
    """Guarda el reporte de migración simplificado en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"
    
    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    logger.info(f"📄 Reporte de migración guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER']
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

def check_roles_collection():
    """Verifica que la colección de roles esté disponible"""
    logger.info("🔍 Verificando colección de roles...")
    
    try:
        from src.connections.mongo_connection import MongoConnection
        
        mongo_conn = MongoConnection()
        database = mongo_conn.get_database()
        roles_collection = database['roles']
        
        roles_count = roles_collection.count_documents({})
        
        if roles_count == 0:
            logger.error("❌ No hay roles en la base de datos MongoDB")
            logger.error("💡 Ejecuta primero la migración de roles y vistas")
            return False
        
        logger.info(f"✅ Encontrados {roles_count} roles en MongoDB")
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error verificando roles: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    if not validate_environment():
        sys.exit(1)
    
    if not check_roles_collection():
        sys.exit(1)
    
    success = main()
    
    if success:
        print("\n🎉 ¡Migración de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de usuarios falló. Revisa los logs.")
        sys.exit(1)
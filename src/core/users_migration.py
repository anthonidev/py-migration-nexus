"""
Script principal de migración de usuarios desde PostgreSQL a MongoDB
"""
import os
import sys
from datetime import datetime

# Agregar el directorio raíz al path si es necesario
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.users_extractor import UsersExtractor
from src.transformers.users_transformer import UsersTransformer
from src.loaders.users_loader import UsersLoader
from src.validators.migration_validator import MigrationValidator
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    """Función principal de migración de usuarios"""
    
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE USUARIOS ===")
        start_time = datetime.now()
        
        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = UsersExtractor()
        
        # Validar datos requeridos
        validation_result = extractor.validate_required_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False
        
        if validation_result['warnings']:
            logger.warning("⚠️ Advertencias encontradas:")
            for warning in validation_result['warnings']:
                logger.warning(f"   - {warning}")
        
        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo datos de PostgreSQL")
        users_data = extractor.extract_users_data()
        hierarchy_data = extractor.extract_user_hierarchy_relationships()
        summary = extractor.get_extraction_summary()
        
        logger.info(f"✅ Extraídos {len(users_data)} usuarios")
        logger.info(f"📊 Resumen: {summary['total_users']} total, {summary['active_users']} activos")
        logger.info(f"👥 Jerarquía: {summary['users_with_parent']} con padre, {summary['root_users']} raíz")
        
        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para MongoDB")
        transformer = UsersTransformer()
        
        # Transformar usuarios
        transformed_users, user_id_mapping = transformer.transform_users_data(users_data)
        
        # Validar transformación
        transformation_validation = transformer.validate_transformation(transformed_users)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False
        
        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary}")
        
        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en MongoDB")
        loader = UsersLoader()
        
        # Cargar usuarios
        users_result = loader.load_users(transformed_users, clear_existing=True)
        
        if not users_result['success']:
            logger.error("❌ Error en la carga de usuarios")
            if 'error' in users_result:
                logger.error(f"Error: {users_result['error']}")
            if 'errors' in users_result:
                for error in users_result['errors']:
                    logger.error(f"   - {error.get('errmsg', 'Error desconocido')}")
            return False
        
        logger.info(f"✅ Usuarios cargados: {users_result['inserted_count']} insertados")
        
        # 5. CREAR ÍNDICES
        logger.info("🔍 PASO 5: Creando índices")
        loader.create_indexes()
        logger.info("✅ Índices creados")
        
        # 6. VALIDACIÓN POST-CARGA
        logger.info("✅ PASO 6: Validando integridad de datos")
        integrity_validation = loader.validate_data_integrity()
        
        if not integrity_validation['valid']:
            logger.error("❌ Validación de integridad fallida")
            for error in integrity_validation['errors']:
                logger.error(f"   - {error}")
            
            # Mostrar advertencias si las hay
            if integrity_validation['warnings']:
                logger.warning("⚠️ Advertencias:")
                for warning in integrity_validation['warnings']:
                    logger.warning(f"   - {warning}")
            
            return False
        
        # 7. ESTADÍSTICAS FINALES
        logger.info("📊 PASO 7: Generando estadísticas finales")
        hierarchy_stats = loader.get_hierarchy_statistics()
        load_stats = loader.get_load_stats()
        
        # 8. RESULTADOS
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("🎉 === MIGRACIÓN DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📊 Usuarios migrados: {integrity_validation['stats']['total_users']}")
        logger.info(f"👤 Usuarios activos: {integrity_validation['stats']['active_users']}")
        logger.info(f"👥 Usuarios con padre: {integrity_validation['stats']['users_with_parent']}")
        logger.info(f"🌳 Usuarios raíz: {integrity_validation['stats']['root_users']}")
        logger.info(f"📝 Documentos generados: {transform_summary['generated_documents']}")
        logger.info(f"🔗 Relaciones jerárquicas: {transform_summary['hierarchy_relationships']}")
        
        # Guardar reporte detallado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': duration,
                'users_migrated': integrity_validation['stats']['total_users']
            },
            'extraction': {
                'total_extracted': len(users_data),
                'extraction_summary': summary
            },
            'transformation': {
                'transform_summary': transform_summary,
                'validation': transformation_validation
            },
            'loading': {
                'load_result': users_result,
                'load_stats': load_stats,
                'integrity_validation': integrity_validation
            },
            'hierarchy_statistics': hierarchy_stats
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
    """Guarda el reporte de migración en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{filename_prefix}_{timestamp}.json"
    
    # Añadir información adicional al reporte
    report_data['execution_info'] = {
        'timestamp': timestamp,
        'platform': sys.platform,
        'python_version': sys.version
    }
    
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
        logger.info("\n💡 Configura las variables en tu .env o sistema:")
        logger.info("   NEXUS_POSTGRES_URL=postgresql://user:pass@host:port/db")
        logger.info("   MS_NEXUS_USER=mongodb://user:pass@host:port/db")
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
    # Cargar variables de entorno desde .env si existe
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    # Validar entorno
    if not validate_environment():
        sys.exit(1)
    
    # Verificar que existan roles
    if not check_roles_collection():
        sys.exit(1)
    
    # Ejecutar migración
    success = main()
    
    if success:
        print("\n🎉 ¡Migración de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de usuarios falló. Revisa los logs.")
        sys.exit(1)
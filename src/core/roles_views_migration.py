import os
import sys
from datetime import datetime

from src.extractors.roles_views_extractor import RolesViewsExtractor
from src.transformers.roles_views_transformer import RolesViewsTransformer
from src.loaders.mongo_loader import MongoLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE ROLES Y VISTAS ===")
        start_time = datetime.now()
        
        # 1. VALIDACIÓN PREVIA
        logger.info("🔍 PASO 1: Validando datos de origen")
        extractor = RolesViewsExtractor()
        
        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            logger.error("❌ Validación de datos fallida")
            for error in validation_result['errors']:
                logger.error(f"   - {error}")
            return False
        
        # 2. EXTRACCIÓN
        logger.info("📤 PASO 2: Extrayendo datos de PostgreSQL")
        roles_data = extractor.extract_roles_and_views()
        views_data = extractor.extract_all_views()
        logger.info(f"✅ Extraídos {len(roles_data)} roles y {len(views_data)} vistas")
        
        # 3. TRANSFORMACIÓN
        logger.info("🔄 PASO 3: Transformando datos para MongoDB")
        transformer = RolesViewsTransformer()
        
        # Transformar vistas
        transformed_views, view_id_mapping = transformer.transform_views_data(views_data)
        
        # Transformar roles
        transformed_roles, role_id_mapping = transformer.transform_roles_data(roles_data, view_id_mapping)
        
        # Actualizar relaciones
        transformer.update_views_with_roles(transformed_views, roles_data, view_id_mapping, role_id_mapping)
        
        # Validar transformación
        transformation_validation = transformer.validate_transformation(transformed_views, transformed_roles)
        if not transformation_validation['valid']:
            logger.error("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                logger.error(f"   - {error}")
            return False
        
        transform_summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {transform_summary['views_transformed']} vistas, {transform_summary['roles_transformed']} roles")
        
        # 4. CARGA
        logger.info("📥 PASO 4: Cargando datos en MongoDB")
        loader = MongoLoader()
        
        # Cargar vistas
        views_result = loader.load_views(transformed_views, clear_existing=True)
        if not views_result['success']:
            logger.error("❌ Error en la carga de vistas")
            if 'error' in views_result:
                logger.error(f"Error: {views_result['error']}")
            return False
        
        logger.info(f"✅ Vistas cargadas: {views_result['inserted_count']} insertadas")
        
        # Cargar roles
        roles_result = loader.load_roles(transformed_roles, clear_existing=True)
        if not roles_result['success']:
            logger.error("❌ Error en la carga de roles")
            if 'error' in roles_result:
                logger.error(f"Error: {roles_result['error']}")
            return False
        
        logger.info(f"✅ Roles cargados: {roles_result['inserted_count']} insertados")
        
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
        
        logger.info("🎉 === MIGRACIÓN DE ROLES Y VISTAS COMPLETADA EXITOSAMENTE ===")
        logger.info(f"⏱️  Duración total: {duration}")
        logger.info(f"📊 Roles migrados: {integrity_validation['stats']['total_roles']}")
        logger.info(f"📊 Vistas migradas: {integrity_validation['stats']['total_views']}")
        
        # Guardar reporte simplificado
        save_migration_report({
            'summary': {
                'success': True,
                'duration': str(duration),
                'roles_migrated': integrity_validation['stats']['total_roles'],
                'views_migrated': integrity_validation['stats']['total_views']
            },
            'extraction': {
                'total_roles_extracted': len(roles_data),
                'total_views_extracted': len(views_data)
            },
            'transformation': {
                'views_transformed': transform_summary['views_transformed'],
                'roles_transformed': transform_summary['roles_transformed'],
                'total_errors': transform_summary['total_errors'],
                'total_warnings': transform_summary['total_warnings'],
                'errors': transform_summary['errors'],
                'warnings': transform_summary['warnings'],
                'validation': transformation_validation
            },
            'loading': {
                'views_result': {
                    'success': views_result['success'],
                    'inserted_count': views_result['inserted_count'],
                    'deleted_count': views_result['deleted_count']
                },
                'roles_result': {
                    'success': roles_result['success'],
                    'inserted_count': roles_result['inserted_count'],
                    'deleted_count': roles_result['deleted_count']
                },
                'load_stats': loader.get_load_stats(),
                'integrity_validation': integrity_validation
            }
        })
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración de roles y vistas: {str(e)}")
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

def save_migration_report(report_data, filename_prefix="roles_views_migration_report"):
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

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    if not validate_environment():
        sys.exit(1)
    
    success = main()
    
    if success:
        print("\n🎉 ¡Migración de roles y vistas completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de roles y vistas falló. Revisa los logs.")
        sys.exit(1)
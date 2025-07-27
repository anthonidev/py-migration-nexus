
import os
import sys
from datetime import datetime


from extractors.roles_views_extractor import RolesViewsExtractor
from transformers.roles_views_transformer import RolesViewsTransformer
from loaders.mongo_loader import MongoLoader
from validators.migration_validator import MigrationValidator
from utils.logger import get_logger

def main():
    """Función principal de migración"""
    logger = get_logger(__name__)
    
    try:
        logger.info("🚀 === INICIANDO MIGRACIÓN DE ROLES Y VISTAS ===")
        start_time = datetime.now()
        
        # 1. EXTRACCIÓN
        logger.info("📤 PASO 1: Extrayendo datos de PostgreSQL")
        extractor = RolesViewsExtractor()
        roles_data = extractor.extract_roles_and_views()
        views_data = extractor.extract_all_views()
        logger.info(f"✅ Extraídos {len(roles_data)} roles y {len(views_data)} vistas")
        
        # 2. TRANSFORMACIÓN
        logger.info("🔄 PASO 2: Transformando datos para MongoDB")
        transformer = RolesViewsTransformer()
        
        # Transformar vistas
        transformed_views, view_id_mapping = transformer.transform_views_data(views_data)
        
        # Transformar roles
        transformed_roles, role_id_mapping = transformer.transform_roles_data(roles_data, view_id_mapping)
        
        # Actualizar relaciones
        transformer.update_views_with_roles(transformed_views, roles_data, view_id_mapping, role_id_mapping)
        
        summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada: {summary}")
        
        # 3. CARGA
        logger.info("📥 PASO 3: Cargando datos en MongoDB")
        loader = MongoLoader()
        
        # Cargar vistas
        views_result = loader.load_views(transformed_views, clear_existing=True)
        logger.info(f"✅ Vistas cargadas: {views_result['inserted_count']} insertadas")
        
        # Cargar roles
        roles_result = loader.load_roles(transformed_roles, clear_existing=True)
        logger.info(f"✅ Roles cargados: {roles_result['inserted_count']} insertados")
        
        # 4. CREAR ÍNDICES
        logger.info("🔍 PASO 4: Creando índices")
        loader.create_indexes()
        logger.info("✅ Índices creados")
        
        # 5. VALIDACIÓN
        logger.info("✅ PASO 5: Validando migración")
        validator = MigrationValidator()
        
        # Validar conteos
        counts_validation = validator.validate_counts()
        
        # Validar integridad
        integrity_validation = validator.validate_data_integrity()
        
        # Generar reporte
        report = validator.generate_migration_report()
        
        # 6. RESULTADOS
        end_time = datetime.now()
        duration = end_time - start_time
        
        if report['summary']['overall_success']:
            logger.info("🎉 === MIGRACIÓN COMPLETADA EXITOSAMENTE ===")
            logger.info(f"⏱️  Duración total: {duration}")
            logger.info(f"📊 Roles migrados: {counts_validation['mongo_counts']['roles']}")
            logger.info(f"📊 Vistas migradas: {counts_validation['mongo_counts']['views']}")
            
            # Guardar reporte
            save_migration_report(report, duration)
            
            return True
        else:
            logger.error("❌ === MIGRACIÓN FALLÓ ===")
            logger.error(f"🚨 Errores: {report['summary']['total_errors']}")
            logger.error(f"⚠️  Advertencias: {report['summary']['total_warnings']}")
            
            # Mostrar errores
            for error in report['data_integrity_validation']['errors']:
                logger.error(f"   - {error}")
            
            return False
            
    except Exception as e:
        logger.error(f"💥 Error crítico durante la migración: {str(e)}")
        logger.exception("Detalles del error:")
        return False

def save_migration_report(report, duration):
    """Guarda el reporte de migración en un archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"migration_report_{timestamp}.json"
    
    # Añadir información adicional al reporte
    report['execution_info'] = {
        'duration_seconds': duration.total_seconds(),
        'duration_human': str(duration),
        'timestamp': timestamp,
        'platform': sys.platform,
        'python_version': sys.version
    }
    
    import json
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"📄 Reporte guardado en: {report_filename}")

def validate_environment():
    """Valida que las variables de entorno estén configuradas"""
    required_vars = ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Variables de entorno faltantes:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Configura las variables en tu .env o sistema:")
        print("   NEXUS_POSTGRES_URL=postgresql://user:pass@host:port/db")
        print("   MS_NEXUS_USER=mongodb://user:pass@host:port/db")
        return False
    
    return True

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
    
    # Ejecutar migración
    success = main()
    
    if success:
        print("\n🎉 ¡Migración completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración falló. Revisa los logs.")
        sys.exit(1)
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.users_extractor import UsersExtractor
from src.transformers.users_transformer import UsersTransformer
from src.loaders.users_loader import UsersLoader
from src.utils.logger import title, subtitle, success, failure,  info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("users")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE USUARIOS ===")
        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = UsersExtractor()
        
        validation_result = extractor.validate_required_data()
        if not validation_result['valid']:
            failure("❌ Validación de datos fallida")
            for error in validation_result['errors']:
                failure(f"   - {error}")
            
            errors, warnings = extract_validation_issues(validation_result)
            report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        subtitle("📤 PASO 2: Extrayendo datos de PostgreSQL")
        users_data = extractor.extract_users_data()
        info(f"✅ Extraídos {len(users_data)} usuarios")
        
        report_builder.extraction_completed("users", len(users_data))
        
        subtitle("🔄 PASO 3: Transformando datos para MongoDB")
        transformer = UsersTransformer()
        
        transformed_users, user_id_mapping = transformer.transform_users_data(users_data)
        
        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("users", transform_summary['users_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
        
        transformation_validation = transformer.validate_transformation(transformed_users)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        info(f"✅ Transformación completada: {transform_summary['users_transformed']} usuarios")
        
        subtitle("📥 PASO 4: Cargando datos en MongoDB")
        loader = UsersLoader()
        
        users_result = loader.load_users(transformed_users, clear_existing=True)
        
        if not users_result['success']:
            failure("❌ Error en la carga de usuarios")
            if 'error' in users_result:
                failure(f"Error: {users_result['error']}")
            
            report_builder.loading_completed("users", 0, 0, 1)
            report_builder.add_validation_errors([users_result.get('error', 'Error en carga')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        inserted_count = users_result.get('inserted_count', 0)
        deleted_count = users_result.get('deleted_count', 0)
        report_builder.loading_completed("users", inserted_count, deleted_count)
        
        info(f"✅ Usuarios cargados: {inserted_count} insertados")
        
        subtitle("✅ PASO 5: Validando integridad de datos")
        integrity_validation = loader.validate_data_integrity()
        
        if not integrity_validation['valid']:
            failure("❌ Validación de integridad fallida")
            for error in integrity_validation['errors']:
                failure(f"   - {error}")
            
            int_errors, int_warnings = extract_validation_issues(integrity_validation)
            report_builder.add_validation_errors(int_errors).add_validation_warnings(int_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        if integrity_validation.get('warnings'):
            report_builder.add_validation_warnings(integrity_validation['warnings'])
        
        success("🎉 === MIGRACIÓN DE USUARIOS COMPLETADA EXITOSAMENTE ===")
        info(f"📊 Usuarios migrados: {integrity_validation['stats']['total_users']}")
        
        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()
        
        return True
        
    except Exception as e:
        failure(f"💥 Error crítico durante la migración de usuarios: {str(e)}")
        report_builder.add_validation_errors([f"Error crítico: {str(e)}"])
        report_builder.mark_failure()
        report_builder.build().save_to_file()
        
        return False
    
    finally:
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'loader' in locals():
                loader.close_connection()
        except Exception as e:
            failure(f"Error cerrando conexiones: {str(e)}")



def check_roles_collection():
    info("🔍 Verificando colección de roles...")
    
    try:
        from src.connections.mongo_connection import MongoConnection
        
        mongo_conn = MongoConnection()
        database = mongo_conn.get_database()
        roles_collection = database['roles']
        
        roles_count = roles_collection.count_documents({})
        
        if roles_count == 0:
            failure("❌ No hay roles en la base de datos MongoDB")
            failure("💡 Ejecuta primero la migración de roles y vistas")
            return False
        
        info(f"✅ Encontrados {roles_count} roles en MongoDB")
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        failure(f"❌ Error verificando roles: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    
    if not check_roles_collection():
        sys.exit(1)
    
    success = main()
    
    if success:
        print("\n🎉 ¡Migración de usuarios completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de usuarios falló. Revisa los logs.")
        sys.exit(1)
import os
import sys

from src.extractors.roles_views_extractor import RolesViewsExtractor
from src.transformers.roles_views_transformer import RolesViewsTransformer
from src.loaders.mongo_loader import MongoLoader
from src.utils.logger import title, subtitle, success, failure,  info

from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("roles_views")
    
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE ROLES Y VISTAS ===")
        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = RolesViewsExtractor()
        
        validation_result = extractor.validate_source_data()
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
        roles_data = extractor.extract_roles_and_views()
        views_data = extractor.extract_all_views()
        info(f"✅ Extraídos {len(roles_data)} roles y {len(views_data)} vistas")
        
        report_builder.extraction_completed("roles", len(roles_data))
        report_builder.extraction_completed("views", len(views_data))
        
        subtitle("🔄 PASO 3: Transformando datos para MongoDB")
        transformer = RolesViewsTransformer()
        
        transformed_views, view_id_mapping = transformer.transform_views_data(views_data)
        transformed_roles, role_id_mapping = transformer.transform_roles_data(roles_data, view_id_mapping)
        
        transformer.update_views_with_roles(transformed_views, roles_data, view_id_mapping, role_id_mapping)
        
        transform_summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(transform_summary)
        
        report_builder.transformation_completed("views", transform_summary['views_transformed'], total_errors)
        report_builder.transformation_completed("roles", transform_summary['roles_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
        
        transformation_validation = transformer.validate_transformation(transformed_views, transformed_roles)
        if not transformation_validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in transformation_validation['errors']:
                failure(f"   - {error}")
            
            val_errors, val_warnings = extract_validation_issues(transformation_validation)
            report_builder.add_validation_errors(val_errors).add_validation_warnings(val_warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        info(f"✅ Transformación completada: {transform_summary['views_transformed']} vistas, {transform_summary['roles_transformed']} roles")
        
        subtitle("📥 PASO 4: Cargando datos en MongoDB")
        loader = MongoLoader()
        
        views_result = loader.load_views(transformed_views, clear_existing=True)
        if not views_result['success']:
            failure("❌ Error en la carga de vistas")
            if 'error' in views_result:
                failure(f"Error: {views_result['error']}")
            
            report_builder.loading_completed("views", 0, 0, 1)
            report_builder.add_validation_errors([views_result.get('error', 'Error en carga de vistas')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        views_inserted = views_result.get('inserted_count', 0)
        views_deleted = views_result.get('deleted_count', 0)
        report_builder.loading_completed("views", views_inserted, views_deleted)
        
        info(f"✅ Vistas cargadas: {views_inserted} insertadas")
        
        roles_result = loader.load_roles(transformed_roles, clear_existing=True)
        if not roles_result['success']:
            failure("❌ Error en la carga de roles")
            if 'error' in roles_result:
                failure(f"Error: {roles_result['error']}")
            
            report_builder.loading_completed("roles", 0, 0, 1)
            report_builder.add_validation_errors([roles_result.get('error', 'Error en carga de roles')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False
        
        roles_inserted = roles_result.get('inserted_count', 0)
        roles_deleted = roles_result.get('deleted_count', 0)
        report_builder.loading_completed("roles", roles_inserted, roles_deleted)
        
        info(f"✅ Roles cargados: {roles_inserted} insertados")
        
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
        
        success("🎉 === MIGRACIÓN DE ROLES Y VISTAS COMPLETADA EXITOSAMENTE ===")
        info(f"📊 Roles migrados: {integrity_validation['stats']['total_roles']}")
        info(f"📊 Vistas migradas: {integrity_validation['stats']['total_views']}")
        
        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()
        
        return True
        
    except Exception as e:
        failure(f"💥 Error crítico durante la migración de roles y vistas: {str(e)}")
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


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    success = main()
    
    if success:
        print("\n🎉 ¡Migración de roles y vistas completada exitosamente!")
        sys.exit(0)
    else:
        print("\n💥 Migración de roles y vistas falló. Revisa los logs.")
        sys.exit(1)
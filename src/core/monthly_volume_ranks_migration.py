import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.monthly_volume_ranks_extractor import MonthlyVolumeRanksExtractor
from src.transformers.monthly_volume_ranks_transformer import MonthlyVolumeRanksTransformer
from src.loaders.monthly_volume_ranks_loader import MonthlyVolumeRanksLoader
from src.utils.logger import title, subtitle, success, failure, info
from src.utils.migration_reports import (
    MigrationReportBuilder,
    extract_validation_issues,
    process_transformation_summary
)


def main():
    report_builder = MigrationReportBuilder("monthly_volume_ranks")
    try:
        title("🚀 === INICIANDO MIGRACIÓN DE MONTHLY_VOLUME_RANKS ===")

        subtitle("🔍 PASO 1: Validando datos de origen")
        extractor = MonthlyVolumeRanksExtractor()
        validation_result = extractor.validate_source_data()
        if not validation_result['valid']:
            failure("❌ Validación de datos de origen fallida")
            for error in validation_result['errors']:
                failure(f"   - {error}")
            errors, warnings = extract_validation_issues(validation_result)
            report_builder.add_validation_errors(errors).add_validation_warnings(warnings)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        subtitle("📤 PASO 2: Extrayendo monthly_volume_ranks del monolito")
        rows = extractor.extract_monthly_volume_ranks()
        info(f"✅ Extraídos {len(rows)} monthly_volume_ranks")
        report_builder.extraction_completed("monthly_volume_ranks", len(rows))

        subtitle("🔄 PASO 3: Transformando para ms-points")
        transformer = MonthlyVolumeRanksTransformer()
        transformed = transformer.transform(rows)

        summary = transformer.get_transformation_summary()
        total_errors, errors, warnings = process_transformation_summary(summary)
        report_builder.transformation_completed("monthly_volume_ranks", summary['monthly_volumes_transformed'], total_errors)
        report_builder.add_validation_errors(errors).add_validation_warnings(warnings)

        validation = transformer.validate(transformed)
        if not validation['valid']:
            failure("❌ Validación de transformación fallida")
            for error in validation['errors']:
                failure(f"   - {error}")
            verrs, vwarns = extract_validation_issues(validation)
            report_builder.add_validation_errors(verrs).add_validation_warnings(vwarns)
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        subtitle("📥 PASO 4: Cargando en ms-points")
        loader = MonthlyVolumeRanksLoader()
        load_res = loader.load(transformed, clear_existing=True)
        if not load_res.get('success'):
            failure("❌ Error en la carga de monthly_volume_ranks")
            report_builder.loading_completed("monthly_volume_ranks", 0, 0, 1)
            report_builder.add_validation_errors([load_res.get('error', 'Error en carga')])
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        report_builder.loading_completed("monthly_volume_ranks", load_res.get('inserted_count', 0), load_res.get('deleted_count', 0))

        subtitle("✅ PASO 5: Validando integridad")
        integ = loader.validate_data_integrity(len(transformed))
        if not integ['valid']:
            failure("❌ Validación de integridad fallida")
            for e in integ['errors']:
                failure(f"   - {e}")
            report_builder.add_validation_errors(integ['errors']).add_validation_warnings(integ.get('warnings', []))
            report_builder.mark_failure()
            report_builder.build().save_to_file()
            return False

        success("🎉 === MIGRACIÓN DE MONTHLY_VOLUME_RANKS COMPLETADA EXITOSAMENTE ===")
        info(f"📈 Registros migrados: {integ['stats'].get('total_monthly_volume_ranks')}")

        report_builder.mark_success()
        final_report = report_builder.build()
        final_report.save_to_file()
        return True

    except Exception as e:
        failure(f"💥 Error crítico durante la migración de monthly_volume_ranks: {str(e)}")
        report_builder.add_validation_errors([f"Error crítico: {str(e)}"])
        report_builder.mark_failure()
        report_builder.build().save_to_file()
        return False

    finally:
        try:
            if 'extractor' in locals():
                extractor.close_connection()
            if 'transformer' in locals():
                transformer.close_connections()
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

    ok = main()
    sys.exit(0 if ok else 1)

from src.utils.logger import get_logger
from src.connections.postgres_connection import PostgresConnection
from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


logger = get_logger(__name__)


class PaymentConfigsExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_payment_configs(self) -> List[Dict[str, Any]]:
        logger.info(
            "Iniciando extracción de configuraciones de pago desde PostgreSQL")

        query = """
        SELECT 
            id,
            code,
            name,
            description,
            "isActive",
            "createdAt",
            "updatedAt"
        FROM 
            public.payment_configs
        ORDER BY 
            id;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            configs_data = []
            for row in results:
                config_dict = dict(zip(columns, row))
                configs_data.append(config_dict)

            logger.info(
                f"Extraídas {len(configs_data)} configuraciones de pago desde PostgreSQL")
            return configs_data

        except Exception as e:
            logger.error(f"Error extrayendo configuraciones de pago: {str(e)}")
            raise

    def get_extraction_summary(self) -> Dict[str, Any]:
        try:
            # Contar configuraciones totales
            total_configs_query = "SELECT COUNT(*) FROM public.payment_configs"
            total_results, _ = self.postgres_conn.execute_query(
                total_configs_query)
            total_configs = total_results[0][0]

            # Contar configuraciones activas
            active_configs_query = "SELECT COUNT(*) FROM public.payment_configs WHERE \"isActive\" = true"
            active_results, _ = self.postgres_conn.execute_query(
                active_configs_query)
            active_configs = active_results[0][0]

            # Obtener códigos únicos
            unique_codes_query = "SELECT COUNT(DISTINCT code) FROM public.payment_configs"
            unique_results, _ = self.postgres_conn.execute_query(
                unique_codes_query)
            unique_codes = unique_results[0][0]

            # Listar códigos existentes para validación
            codes_query = "SELECT code FROM public.payment_configs ORDER BY code"
            codes_results, _ = self.postgres_conn.execute_query(codes_query)
            existing_codes = [row[0] for row in codes_results]

            summary = {
                'total_configs': total_configs,
                'active_configs': active_configs,
                'inactive_configs': total_configs - active_configs,
                'unique_codes': unique_codes,
                'existing_codes': existing_codes,
                'has_duplicates': unique_codes != total_configs
            }

            logger.info(f"Resumen de extracción: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Error obteniendo resumen de extracción: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar códigos únicos
            duplicate_codes_query = """
            SELECT code, COUNT(*) as count 
            FROM public.payment_configs 
            GROUP BY code 
            HAVING COUNT(*) > 1
            """
            duplicate_results, _ = self.postgres_conn.execute_query(
                duplicate_codes_query)

            if duplicate_results:
                for row in duplicate_results:
                    code, count = row
                    validation_results['errors'].append(
                        f"Código duplicado: '{code}' ({count} veces)")
                validation_results['valid'] = False

            # Validar campos obligatorios
            missing_data_query = """
            SELECT id, code, name
            FROM public.payment_configs 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_results, _ = self.postgres_conn.execute_query(
                missing_data_query)

            if missing_results:
                for row in missing_results:
                    config_id, code, name = row
                    validation_results['errors'].append(
                        f"Configuración ID {config_id}: código='{code}', nombre='{name}' - campos obligatorios vacíos"
                    )
                validation_results['valid'] = False

            # Verificar configuraciones inactivas
            inactive_query = "SELECT COUNT(*) FROM public.payment_configs WHERE \"isActive\" = false"
            inactive_results, _ = self.postgres_conn.execute_query(
                inactive_query)
            inactive_count = inactive_results[0][0]

            if inactive_count > 0:
                validation_results['warnings'].append(
                    f"{inactive_count} configuraciones inactivas serán migradas"
                )

            logger.info(
                f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(
                f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        self.postgres_conn.disconnect()

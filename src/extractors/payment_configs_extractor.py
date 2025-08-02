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
        logger.info("Iniciando extracción de configuraciones de pago desde PostgreSQL")

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

            logger.info(f"Extraídas {len(configs_data)} configuraciones de pago desde PostgreSQL")
            return configs_data

        except Exception as e:
            logger.error(f"Error extrayendo configuraciones de pago: {str(e)}")
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
            duplicate_results, _ = self.postgres_conn.execute_query(duplicate_codes_query)

            if duplicate_results:
                for row in duplicate_results:
                    code, count = row
                    validation_results['errors'].append(f"Código duplicado: '{code}' ({count} veces)")
                validation_results['valid'] = False

            # Validar campos obligatorios
            missing_data_query = """
            SELECT id, code, name
            FROM public.payment_configs 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_results, _ = self.postgres_conn.execute_query(missing_data_query)

            if missing_results:
                for row in missing_results:
                    config_id, code, name = row
                    validation_results['errors'].append(
                        f"Configuración ID {config_id}: código='{code}', nombre='{name}' - campos obligatorios vacíos"
                    )
                validation_results['valid'] = False

            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        self.postgres_conn.disconnect()
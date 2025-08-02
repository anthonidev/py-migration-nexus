from typing import List, Dict, Any
from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PaymentConfigsLoader:

    def __init__(self):
        self.postgres_conn = PaymentsPostgresConnection()
        self.stats = {
            'configs_inserted': 0,
            'configs_deleted': 0,
            'errors': []
        }

    def _check_table_exists(self):
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'payment_configs'
        );
        """
        result, _ = self.postgres_conn.execute_query(check_query)
        if not result[0][0]:
            raise RuntimeError("Tabla 'payment_configs' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> int:
        logger.info("Eliminando configuraciones de pago existentes")

        try:
            delete_query = "DELETE FROM payment_configs"
            deleted_count, _ = self.postgres_conn.execute_query(delete_query)

            reset_sequence_query = """
            SELECT setval('payment_configs_id_seq', 
                         COALESCE((SELECT MAX(id) FROM payment_configs), 0) + 1, 
                         false);
            """
            self.postgres_conn.execute_query(reset_sequence_query)

            self.stats['configs_deleted'] = deleted_count
            logger.info(f"Eliminadas {deleted_count} configuraciones existentes")
            return deleted_count

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_payment_configs(self, configs_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Iniciando carga de {len(configs_data)} configuraciones de pago en PostgreSQL")

        try:
            self._check_table_exists()

            if clear_existing:
                self.clear_existing_data()

            if not configs_data:
                logger.warning("No hay configuraciones para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['configs_deleted']
                }

            inserted_count = self._insert_configs_with_original_ids(configs_data)

            self.stats['configs_inserted'] = inserted_count
            logger.info(f"Insertadas {inserted_count} configuraciones exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['configs_deleted']
            }

        except Exception as e:
            error_msg = f"Error inesperado cargando configuraciones: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)

            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['configs_deleted'],
                'error': str(e)
            }

    def _insert_configs_with_original_ids(self, configs_data: List[Dict[str, Any]]) -> int:
        insert_query = """
        INSERT INTO payment_configs (id, code, name, description, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for config in configs_data:
            params = (
                config['id'],
                config['code'],
                config['name'],
                config['description'],
                config['is_active'],
                config['created_at'],
                config['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            max_id = max(config['id'] for config in configs_data)
            update_sequence_query = f"SELECT setval('payment_configs_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

            logger.info(f"Secuencia actualizada para continuar desde ID {max_id}")
            return len(configs_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        logger.info("Validando integridad de datos de configuraciones en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            count_query = "SELECT COUNT(*) FROM payment_configs"
            total_count, _ = self.postgres_conn.execute_query(count_query)
            total_configs = total_count[0][0]

            validation_results['stats'] = {'total_configs': total_configs}

            # Validar códigos únicos
            unique_codes_query = "SELECT COUNT(DISTINCT code) FROM payment_configs"
            unique_count, _ = self.postgres_conn.execute_query(unique_codes_query)
            unique_codes = unique_count[0][0]

            if unique_codes != total_configs:
                validation_results['errors'].append("Códigos duplicados encontrados")
                validation_results['valid'] = False

            # Validar campos obligatorios
            missing_data_query = """
            SELECT COUNT(*) FROM payment_configs 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_count, _ = self.postgres_conn.execute_query(missing_data_query)
            missing_data = missing_count[0][0]

            if missing_data > 0:
                validation_results['errors'].append(f"{missing_data} configuraciones con campos obligatorios vacíos")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        return {
            'configs_inserted': self.stats['configs_inserted'],
            'configs_deleted': self.stats['configs_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        self.postgres_conn.disconnect()
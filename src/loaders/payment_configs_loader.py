"""
Cargador de configuraciones de pago a PostgreSQL (ms-payments)
"""
from typing import List, Dict, Any
from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentConfigsLoader:
    """Cargador de configuraciones de pago transformadas a PostgreSQL ms-payments"""

    def __init__(self):
        self.postgres_conn = PaymentsPostgresConnection()
        self.stats = {
            'configs_inserted': 0,
            'configs_deleted': 0,
            'errors': []
        }

    def create_table_if_not_exists(self):
        """Crea la tabla payment_configs si no existe"""
        logger.info("Verificando/creando tabla payment_configs")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS payment_configs (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(100) NOT NULL,
            description VARCHAR(500),
            is_active BOOLEAN DEFAULT true NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Crear índices
        create_indexes_queries = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_configs_code ON payment_configs(code);",
            "CREATE INDEX IF NOT EXISTS idx_payment_configs_is_active ON payment_configs(is_active);"
        ]

        try:
            # Crear tabla
            self.postgres_conn.execute_query(create_table_query)
            logger.info("Tabla payment_configs verificada/creada")

            # Crear índices
            for index_query in create_indexes_queries:
                self.postgres_conn.execute_query(index_query)

            logger.info("Índices de payment_configs creados")

        except Exception as e:
            error_msg = f"Error creando tabla/índices: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def clear_existing_data(self) -> int:
        """Elimina todas las configuraciones existentes"""
        logger.info("Eliminando configuraciones de pago existentes")

        try:
            # Desactivar restricciones de clave foránea temporalmente si es necesario
            # (asumiendo que payments table podría referenciar payment_configs)
            delete_query = "DELETE FROM payment_configs"
            deleted_count, _ = self.postgres_conn.execute_query(delete_query)

            # Reiniciar secuencia para que los IDs empiecen desde donde los dejamos
            reset_sequence_query = """
            SELECT setval('payment_configs_id_seq', 
                         COALESCE((SELECT MAX(id) FROM payment_configs), 0) + 1, 
                         false);
            """
            self.postgres_conn.execute_query(reset_sequence_query)

            self.stats['configs_deleted'] = deleted_count
            logger.info(
                f"Eliminadas {deleted_count} configuraciones existentes")
            return deleted_count

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_payment_configs(self, configs_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """
        Carga las configuraciones de pago en PostgreSQL

        Args:
            configs_data: Lista de configuraciones transformadas
            clear_existing: Si True, elimina las configuraciones existentes antes de cargar

        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(
            f"Iniciando carga de {len(configs_data)} configuraciones de pago en PostgreSQL")

        try:
            # Crear tabla si no existe
            self.create_table_if_not_exists()

            # Eliminar datos existentes si se solicita
            if clear_existing:
                self.clear_existing_data()

            # Preparar datos para inserción
            if not configs_data:
                logger.warning("No hay configuraciones para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['configs_deleted']
                }

            # Insertar configuraciones manteniendo IDs originales
            inserted_count = self._insert_configs_with_original_ids(
                configs_data)

            self.stats['configs_inserted'] = inserted_count
            logger.info(
                f"Insertadas {inserted_count} configuraciones exitosamente")

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
        """Inserta configuraciones manteniendo los IDs originales"""

        # Preparar la consulta de inserción con ID específico
        insert_query = """
        INSERT INTO payment_configs (id, code, name, description, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Preparar lista de parámetros
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
            # Insertar todos los registros
            inserted_count = self.postgres_conn.execute_bulk_insert(
                insert_query, params_list)

            # Actualizar la secuencia para que continúe desde el máximo ID insertado
            max_id = max(config['id'] for config in configs_data)
            update_sequence_query = f"SELECT setval('payment_configs_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

            logger.info(
                f"Secuencia actualizada para continuar desde ID {max_id}")
            # Retorna cantidad de configuraciones insertadas
            return len(configs_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva: {str(e)}")
            # Intentar inserción una por una para identificar registros problemáticos
            return self._insert_configs_one_by_one(configs_data)

    def _insert_configs_one_by_one(self, configs_data: List[Dict[str, Any]]) -> int:
        """Inserta configuraciones una por una para manejar errores individuales"""
        logger.warning(
            "Intentando inserción una por una debido a errores en inserción masiva")

        insert_query = """
        INSERT INTO payment_configs (id, code, name, description, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        successful_inserts = 0

        for config in configs_data:
            try:
                params = (
                    config['id'],
                    config['code'],
                    config['name'],
                    config['description'],
                    config['is_active'],
                    config['created_at'],
                    config['updated_at']
                )

                self.postgres_conn.execute_query(insert_query, params)
                successful_inserts += 1

            except Exception as e:
                error_msg = f"Error insertando configuración ID {config['id']}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        # Actualizar secuencia
        if successful_inserts > 0:
            max_id = max(config['id'] for config in configs_data)
            update_sequence_query = f"SELECT setval('payment_configs_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

        return successful_inserts

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info(
            "Validando integridad de datos de configuraciones en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            count_query = "SELECT COUNT(*) FROM payment_configs"
            total_count, _ = self.postgres_conn.execute_query(count_query)
            total_configs = total_count[0][0]

            # Contar configuraciones activas
            active_query = "SELECT COUNT(*) FROM payment_configs WHERE is_active = true"
            active_count, _ = self.postgres_conn.execute_query(active_query)
            active_configs = active_count[0][0]

            # Validar códigos únicos
            unique_codes_query = "SELECT COUNT(DISTINCT code) FROM payment_configs"
            unique_count, _ = self.postgres_conn.execute_query(
                unique_codes_query)
            unique_codes = unique_count[0][0]

            validation_results['stats'] = {
                'total_configs': total_configs,
                'active_configs': active_configs,
                'inactive_configs': total_configs - active_configs,
                'unique_codes': unique_codes
            }

            # Verificar duplicados de código
            if unique_codes != total_configs:
                validation_results['errors'].append(
                    "Códigos duplicados encontrados")
                validation_results['valid'] = False

            # Verificar campos obligatorios
            missing_data_query = """
            SELECT COUNT(*) FROM payment_configs 
            WHERE code IS NULL OR code = '' OR name IS NULL OR name = ''
            """
            missing_count, _ = self.postgres_conn.execute_query(
                missing_data_query)
            missing_data = missing_count[0][0]

            if missing_data > 0:
                validation_results['errors'].append(
                    f"{missing_data} configuraciones con campos obligatorios vacíos")
                validation_results['valid'] = False

            # Verificar longitudes de campos
            length_validation_query = """
            SELECT COUNT(*) FROM payment_configs 
            WHERE LENGTH(code) > 50 OR LENGTH(name) > 100 OR LENGTH(description) > 500
            """
            length_issues, _ = self.postgres_conn.execute_query(
                length_validation_query)
            length_problems = length_issues[0][0]

            if length_problems > 0:
                validation_results['errors'].append(
                    f"{length_problems} configuraciones exceden longitudes permitidas")
                validation_results['valid'] = False

            logger.info(
                f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la carga"""
        return {
            'configs_inserted': self.stats['configs_inserted'],
            'configs_deleted': self.stats['configs_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()

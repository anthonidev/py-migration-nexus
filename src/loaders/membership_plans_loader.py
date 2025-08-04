from typing import List, Dict, Any
from src.connections.membership_postgres_connection import MembershipPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MembershipPlansLoader:

    def __init__(self):
        self.postgres_conn = MembershipPostgresConnection()
        self.stats = {
            'plans_inserted': 0,
            'plans_deleted': 0,
            'errors': []
        }

    def _check_table_exists(self):
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'membership_plans'
        );
        """
        result, _ = self.postgres_conn.execute_query(check_query)
        if not result[0][0]:
            raise RuntimeError("Tabla 'membership_plans' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> int:
        logger.info("Eliminando planes de membresía existentes")

        try:
            delete_query = "DELETE FROM membership_plans"
            deleted_count, _ = self.postgres_conn.execute_query(delete_query)

            reset_sequence_query = """
            SELECT setval('membership_plans_id_seq', 
                         COALESCE((SELECT MAX(id) FROM membership_plans), 0) + 1, 
                         false);
            """
            self.postgres_conn.execute_query(reset_sequence_query)

            self.stats['plans_deleted'] = deleted_count
            logger.info(f"Eliminados {deleted_count} planes existentes")
            return deleted_count

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_membership_plans(self, plans_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Iniciando carga de {len(plans_data)} planes de membresía en PostgreSQL")

        try:
            self._check_table_exists()

            if clear_existing:
                self.clear_existing_data()

            if not plans_data:
                logger.warning("No hay planes para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['plans_deleted']
                }

            inserted_count = self._insert_plans_with_original_ids(plans_data)

            self.stats['plans_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} planes exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['plans_deleted']
            }

        except Exception as e:
            error_msg = f"Error inesperado cargando planes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)

            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['plans_deleted'],
                'error': str(e)
            }

    def _insert_plans_with_original_ids(self, plans_data: List[Dict[str, Any]]) -> int:
        insert_query = """
        INSERT INTO membership_plans (
            id, name, price, check_amount, binary_points, commission_percentage,
            direct_commission_amount, products, benefits, is_active, display_order,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for plan in plans_data:
            params = (
                plan['id'],
                plan['name'],
                plan['price'],
                plan['check_amount'],
                plan['binary_points'],
                plan['commission_percentage'],
                plan['direct_commission_amount'],
                plan['products'],  # PostgreSQL maneja arrays nativamente
                plan['benefits'],  # PostgreSQL maneja arrays nativamente
                plan['is_active'],
                plan['display_order'],
                plan['created_at'],
                plan['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            max_id = max(plan['id'] for plan in plans_data)
            update_sequence_query = f"SELECT setval('membership_plans_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

            logger.info(f"Secuencia actualizada para continuar desde ID {max_id}")
            return len(plans_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        logger.info("Validando integridad de datos de planes de membresía en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            count_query = "SELECT COUNT(*) FROM membership_plans"
            total_count, _ = self.postgres_conn.execute_query(count_query)
            total_plans = total_count[0][0]

            validation_results['stats'] = {'total_plans': total_plans}

            # Validar campos obligatorios
            missing_data_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE name IS NULL OR name = ''
               OR price IS NULL OR price < 0
               OR check_amount IS NULL OR check_amount < 0
               OR binary_points IS NULL OR binary_points < 0
               OR commission_percentage IS NULL 
               OR commission_percentage < 0 OR commission_percentage > 100
            """
            missing_count, _ = self.postgres_conn.execute_query(missing_data_query)
            missing_data = missing_count[0][0]

            if missing_data > 0:
                validation_results['errors'].append(f"{missing_data} planes con campos obligatorios inválidos")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def close_connection(self):
        self.postgres_conn.disconnect()
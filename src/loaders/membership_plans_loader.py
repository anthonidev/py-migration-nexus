"""
Cargador de planes de membresía a PostgreSQL (ms-membership)
"""
from typing import List, Dict, Any
from src.connections.membership_postgres_connection import MembershipPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MembershipPlansLoader:
    """Cargador de planes de membresía transformados a PostgreSQL ms-membership"""

    def __init__(self):
        self.postgres_conn = MembershipPostgresConnection()
        self.stats = {
            'plans_inserted': 0,
            'plans_deleted': 0,
            'errors': []
        }

    def create_table_if_not_exists(self):
        """Crea la tabla membership_plans si no existe"""
        logger.info("Verificando/creando tabla membership_plans")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS membership_plans (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            check_amount DECIMAL(10,2) NOT NULL,
            binary_points INTEGER NOT NULL,
            commission_percentage DECIMAL(5,2) NOT NULL,
            direct_commission_amount DECIMAL(10,2),
            products TEXT[] DEFAULT '{}',
            benefits TEXT[] DEFAULT '{}',
            is_active BOOLEAN DEFAULT true NOT NULL,
            display_order INTEGER DEFAULT 0 NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Crear índices según la entidad
        create_indexes_queries = [
            "CREATE INDEX IF NOT EXISTS idx_membership_plans_is_active_display_order ON membership_plans(is_active, display_order);",
            "CREATE INDEX IF NOT EXISTS idx_membership_plans_is_active ON membership_plans(is_active);",
            "CREATE INDEX IF NOT EXISTS idx_membership_plans_display_order ON membership_plans(display_order);"
        ]

        try:
            # Crear tabla
            self.postgres_conn.execute_query(create_table_query)
            logger.info("Tabla membership_plans verificada/creada")

            # Crear índices
            for index_query in create_indexes_queries:
                self.postgres_conn.execute_query(index_query)

            logger.info("Índices de membership_plans creados")

        except Exception as e:
            error_msg = f"Error creando tabla/índices: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def clear_existing_data(self) -> int:
        """Elimina todos los planes existentes"""
        logger.info("Eliminando planes de membresía existentes")

        try:
            # Eliminar todos los registros
            delete_query = "DELETE FROM membership_plans"
            deleted_count, _ = self.postgres_conn.execute_query(delete_query)

            # Reiniciar secuencia para que los IDs empiecen desde donde los dejamos
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
        """
        Carga los planes de membresía en PostgreSQL

        Args:
            plans_data: Lista de planes transformados
            clear_existing: Si True, elimina los planes existentes antes de cargar

        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(
            f"Iniciando carga de {len(plans_data)} planes de membresía en PostgreSQL")

        try:
            # Crear tabla si no existe
            self.create_table_if_not_exists()

            # Eliminar datos existentes si se solicita
            if clear_existing:
                self.clear_existing_data()

            # Preparar datos para inserción
            if not plans_data:
                logger.warning("No hay planes para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['plans_deleted']
                }

            # Insertar planes manteniendo IDs originales
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
        """Inserta planes manteniendo los IDs originales"""

        # Preparar la consulta de inserción con ID específico
        insert_query = """
        INSERT INTO membership_plans (
            id, name, price, check_amount, binary_points, commission_percentage,
            direct_commission_amount, products, benefits, is_active, display_order,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        # Preparar lista de parámetros
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
            # Insertar todos los registros
            inserted_count = self.postgres_conn.execute_bulk_insert(
                insert_query, params_list)

            # Actualizar la secuencia para que continúe desde el máximo ID insertado
            max_id = max(plan['id'] for plan in plans_data)
            update_sequence_query = f"SELECT setval('membership_plans_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

            logger.info(
                f"Secuencia actualizada para continuar desde ID {max_id}")
            return len(plans_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva: {str(e)}")
            # Intentar inserción una por una para identificar registros problemáticos
            return self._insert_plans_one_by_one(plans_data)

    def _insert_plans_one_by_one(self, plans_data: List[Dict[str, Any]]) -> int:
        """Inserta planes uno por uno para manejar errores individuales"""
        logger.warning(
            "Intentando inserción una por una debido a errores en inserción masiva")

        insert_query = """
        INSERT INTO membership_plans (
            id, name, price, check_amount, binary_points, commission_percentage,
            direct_commission_amount, products, benefits, is_active, display_order,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        successful_inserts = 0

        for plan in plans_data:
            try:
                params = (
                    plan['id'],
                    plan['name'],
                    plan['price'],
                    plan['check_amount'],
                    plan['binary_points'],
                    plan['commission_percentage'],
                    plan['direct_commission_amount'],
                    plan['products'],
                    plan['benefits'],
                    plan['is_active'],
                    plan['display_order'],
                    plan['created_at'],
                    plan['updated_at']
                )

                self.postgres_conn.execute_query(insert_query, params)
                successful_inserts += 1

            except Exception as e:
                error_msg = f"Error insertando plan ID {plan['id']}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        # Actualizar secuencia
        if successful_inserts > 0:
            max_id = max(plan['id'] for plan in plans_data)
            update_sequence_query = f"SELECT setval('membership_plans_id_seq', {max_id}, true);"
            self.postgres_conn.execute_query(update_sequence_query)

        return successful_inserts

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info(
            "Validando integridad de datos de planes de membresía en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            count_query = "SELECT COUNT(*) FROM membership_plans"
            total_count, _ = self.postgres_conn.execute_query(count_query)
            total_plans = total_count[0][0]

            # Contar planes activos
            active_query = "SELECT COUNT(*) FROM membership_plans WHERE is_active = true"
            active_count, _ = self.postgres_conn.execute_query(active_query)
            active_plans = active_count[0][0]

            # Estadísticas de precios
            price_stats_query = """
            SELECT MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price
            FROM membership_plans WHERE is_active = true
            """
            price_stats, price_columns = self.postgres_conn.execute_query(price_stats_query)
            price_data = dict(zip(price_columns, price_stats[0])) if price_stats else {}

            # Contar planes con productos y beneficios
            with_products_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE products IS NOT NULL AND array_length(products, 1) > 0
            """
            products_count, _ = self.postgres_conn.execute_query(with_products_query)
            plans_with_products = products_count[0][0]

            with_benefits_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE benefits IS NOT NULL AND array_length(benefits, 1) > 0
            """
            benefits_count, _ = self.postgres_conn.execute_query(with_benefits_query)
            plans_with_benefits = benefits_count[0][0]

            validation_results['stats'] = {
                'total_plans': total_plans,
                'active_plans': active_plans,
                'inactive_plans': total_plans - active_plans,
                'plans_with_products': plans_with_products,
                'plans_with_benefits': plans_with_benefits,
                'price_stats': price_data
            }

            # Verificar campos obligatorios
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
                validation_results['errors'].append(
                    f"{missing_data} planes con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Verificar longitudes de campos
            length_validation_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE LENGTH(name) > 100
            """
            length_issues, _ = self.postgres_conn.execute_query(length_validation_query)
            length_problems = length_issues[0][0]

            if length_problems > 0:
                validation_results['errors'].append(
                    f"{length_problems} planes exceden longitudes permitidas")
                validation_results['valid'] = False

            # Verificar arrays vacíos
            empty_arrays_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE (products IS NULL OR array_length(products, 1) IS NULL)
              AND (benefits IS NULL OR array_length(benefits, 1) IS NULL)
            """
            empty_arrays_count, _ = self.postgres_conn.execute_query(empty_arrays_query)
            empty_arrays = empty_arrays_count[0][0]

            if empty_arrays > 0:
                validation_results['warnings'].append(
                    f"{empty_arrays} planes sin productos ni beneficios")

            # Verificar duplicados (aunque no deberían existir por PK)
            duplicate_ids_query = """
            SELECT id, COUNT(*) as count 
            FROM membership_plans 
            GROUP BY id 
            HAVING COUNT(*) > 1
            """
            duplicate_results, _ = self.postgres_conn.execute_query(duplicate_ids_query)

            if duplicate_results:
                validation_results['errors'].append(
                    f"{len(duplicate_results)} IDs duplicados encontrados")
                validation_results['valid'] = False

            # Verificar comisiones directas negativas
            negative_commission_query = """
            SELECT COUNT(*) FROM membership_plans 
            WHERE direct_commission_amount IS NOT NULL 
              AND direct_commission_amount < 0
            """
            negative_commission_count, _ = self.postgres_conn.execute_query(negative_commission_query)
            negative_commissions = negative_commission_count[0][0]

            if negative_commissions > 0:
                validation_results['warnings'].append(
                    f"{negative_commissions} planes con comisión directa negativa")

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
            'plans_inserted': self.stats['plans_inserted'],
            'plans_deleted': self.stats['plans_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def get_detailed_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas detalladas de la base de datos"""
        logger.info("Obteniendo estadísticas detalladas de membership_plans")

        try:
            # Estadísticas básicas
            basic_stats_query = """
            SELECT 
                COUNT(*) as total_plans,
                COUNT(*) FILTER (WHERE is_active = true) as active_plans,
                COUNT(*) FILTER (WHERE is_active = false) as inactive_plans,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                MIN(display_order) as min_order,
                MAX(display_order) as max_order
            FROM membership_plans
            """
            basic_results, basic_columns = self.postgres_conn.execute_query(basic_stats_query)
            basic_stats = dict(zip(basic_columns, basic_results[0])) if basic_results else {}

            # Estadísticas de arrays
            array_stats_query = """
            SELECT 
                COUNT(*) FILTER (WHERE products IS NOT NULL AND array_length(products, 1) > 0) as plans_with_products,
                COUNT(*) FILTER (WHERE benefits IS NOT NULL AND array_length(benefits, 1) > 0) as plans_with_benefits,
                COUNT(*) FILTER (WHERE products IS NOT NULL AND array_length(products, 1) > 0 
                                    AND benefits IS NOT NULL AND array_length(benefits, 1) > 0) as plans_with_both
            FROM membership_plans
            """
            array_results, array_columns = self.postgres_conn.execute_query(array_stats_query)
            array_stats = dict(zip(array_columns, array_results[0])) if array_results else {}

            # Distribución por rangos de precio
            price_distribution_query = """
            SELECT 
                COUNT(*) FILTER (WHERE price < 100) as under_100,
                COUNT(*) FILTER (WHERE price >= 100 AND price < 500) as between_100_500,
                COUNT(*) FILTER (WHERE price >= 500 AND price < 1000) as between_500_1000,
                COUNT(*) FILTER (WHERE price >= 1000) as over_1000
            FROM membership_plans
            WHERE is_active = true
            """
            price_dist_results, price_dist_columns = self.postgres_conn.execute_query(price_distribution_query)
            price_distribution = dict(zip(price_dist_columns, price_dist_results[0])) if price_dist_results else {}

            # Top 5 planes por precio
            top_plans_query = """
            SELECT id, name, price, is_active, display_order
            FROM membership_plans 
            ORDER BY price DESC 
            LIMIT 5
            """
            top_plans_results, top_plans_columns = self.postgres_conn.execute_query(top_plans_query)
            top_plans = [dict(zip(top_plans_columns, row)) for row in top_plans_results]

            return {
                'basic_stats': basic_stats,
                'array_stats': array_stats,
                'price_distribution': price_distribution,
                'top_plans_by_price': top_plans,
                'load_stats': self.get_load_stats()
            }

        except Exception as e:
            logger.error(f"Error obteniendo estadísticas detalladas: {str(e)}")
            return {'error': str(e)}

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()
# src/loaders/ranks_loader.py
from typing import List, Dict, Any
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RanksLoader:
    """Loader para insertar datos de ranks en PostgreSQL preservando IDs originales"""

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()
        self.stats = {
            'ranks_inserted': 0,
            'ranks_deleted': 0,
            'errors': []
        }

    def _check_table_exists(self):
        """Verifica que la tabla 'ranks' existe"""
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'ranks'
        );
        """
        result, _ = self.postgres_conn.execute_query(check_query)
        if not result[0][0]:
            raise RuntimeError("Tabla 'ranks' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> int:
        """Elimina todos los ranks existentes"""
        logger.info("Eliminando ranks existentes")

        try:
            # Primero eliminamos las referencias en user_ranks si existen
            try:
                # Verificar si existe la tabla user_ranks
                check_user_ranks_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'user_ranks'
                );
                """
                result, _ = self.postgres_conn.execute_query(check_user_ranks_query)
                
                if result[0][0]:  # Si la tabla user_ranks existe
                    logger.info("Limpiando referencias en user_ranks")
                    self.postgres_conn.execute_query("UPDATE user_ranks SET current_rank_id = NULL, highest_rank_id = NULL")
            except Exception as e:
                logger.warning(f"No se pudo limpiar user_ranks (es normal si no existe): {str(e)}")

            # Eliminar todos los ranks
            delete_query = "DELETE FROM ranks"
            deleted_count, _ = self.postgres_conn.execute_query(delete_query)

            # Resetear la secuencia para que acepte IDs específicos
            reset_sequence_query = """
            SELECT setval('ranks_id_seq', 
                         COALESCE((SELECT MAX(id) FROM ranks), 0) + 1, 
                         false);
            """
            self.postgres_conn.execute_query(reset_sequence_query)

            self.stats['ranks_deleted'] = deleted_count
            logger.info(f"Eliminados {deleted_count} ranks existentes")
            return deleted_count

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_ranks(self, ranks_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """Carga ranks en PostgreSQL preservando los IDs originales"""
        logger.info(f"Iniciando carga de {len(ranks_data)} ranks en PostgreSQL")

        try:
            self._check_table_exists()

            if clear_existing:
                self.clear_existing_data()

            if not ranks_data:
                logger.warning("No hay ranks para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['ranks_deleted']
                }

            inserted_count = self._insert_ranks_with_original_ids(ranks_data)

            self.stats['ranks_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} ranks exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['ranks_deleted'],
                'errors': self.stats['errors']
            }

        except Exception as e:
            error_msg = f"Error durante la carga de ranks: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['ranks_deleted'],
                'errors': self.stats['errors']
            }

    def _insert_ranks_with_original_ids(self, ranks_data: List[Dict[str, Any]]) -> int:
        """Inserta ranks preservando los IDs originales"""
        logger.info("Insertando ranks con IDs originales preservados")

        insert_query = """
        INSERT INTO ranks (
            id, name, code, required_pay_leg_qv, required_total_tree_qv, 
            required_directs, required_active_teams, required_qualified_teams, 
            required_qualified_rank_id, required_sponsorship_branch_qv, 
            required_leg_balance_percentage, min_depth_levels, is_active, 
            benefits, rank_order, description, created_at, updated_at
        ) VALUES (
            %(id)s, %(name)s, %(code)s, %(required_pay_leg_qv)s, %(required_total_tree_qv)s,
            %(required_directs)s, %(required_active_teams)s, %(required_qualified_teams)s,
            %(required_qualified_rank_id)s, %(max_sponsorship_branch_qv)s,
            %(max_leg_balance_percentage)s, %(min_depth_levels)s, %(is_active)s,
            %(benefits)s, %(rank_order)s, %(description)s, %(created_at)s, %(updated_at)s
        )
        """

        params_list = []
        for rank in ranks_data:
            params = {
                'id': rank['id'],
                'name': rank['name'],
                'code': rank['code'],
                'required_pay_leg_qv': rank['required_pay_leg_qv'],
                'required_total_tree_qv': rank['required_total_tree_qv'],
                'required_directs': rank['required_directs'],
                'required_active_teams': rank.get('required_active_teams'),
                'required_qualified_teams': rank.get('required_qualified_teams'),
                'required_qualified_rank_id': rank.get('required_qualified_rank_id'),
                'max_sponsorship_branch_qv': rank.get('max_sponsorship_branch_qv'),
                'max_leg_balance_percentage': rank.get('max_leg_balance_percentage'),
                'min_depth_levels': rank.get('min_depth_levels'),
                'is_active': rank['is_active'],
                'benefits': rank.get('benefits'),
                'rank_order': rank['rank_order'],
                'description': rank.get('description'),
                'created_at': rank['created_at'],
                'updated_at': rank['updated_at']
            }
            params_list.append(params)

        # Insertar todos los ranks
        inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

        # Actualizar la secuencia para que continue desde el ID más alto
        max_id_query = "SELECT COALESCE(MAX(id), 0) FROM ranks"
        max_id_result, _ = self.postgres_conn.execute_query(max_id_query)
        max_id = max_id_result[0][0]

        update_sequence_query = f"SELECT setval('ranks_id_seq', {max_id + 1}, false)"
        self.postgres_conn.execute_query(update_sequence_query)

        return inserted_count

    def validate_data_integrity(self, original_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Valida la integridad de los datos migrados"""
        logger.info("Validando integridad de datos migrados")

        errors = []
        warnings = []

        try:
            # Contar total de ranks en la BD
            count_query = "SELECT COUNT(*) FROM ranks"
            count_result, _ = self.postgres_conn.execute_query(count_query)
            total_ranks = count_result[0][0]

            # Verificar que el número de ranks coincide
            if total_ranks != len(original_data):
                errors.append(f"Número de ranks no coincide: esperados {len(original_data)}, encontrados {total_ranks}")

            # Verificar que todos los IDs originales están presentes
            ids_query = "SELECT id FROM ranks ORDER BY id"
            ids_result, _ = self.postgres_conn.execute_query(ids_query)
            db_ids = set(row[0] for row in ids_result)
            
            original_ids = set(rank['id'] for rank in original_data)
            missing_ids = original_ids - db_ids
            extra_ids = db_ids - original_ids

            if missing_ids:
                errors.append(f"IDs faltantes en la BD: {sorted(missing_ids)}")

            if extra_ids:
                warnings.append(f"IDs extra en la BD: {sorted(extra_ids)}")

            # Verificar unicidad de códigos y rankOrder
            codes_query = "SELECT code, COUNT(*) FROM ranks GROUP BY code HAVING COUNT(*) > 1"
            codes_result, _ = self.postgres_conn.execute_query(codes_query)
            if codes_result:
                errors.append(f"Códigos duplicados encontrados: {[row[0] for row in codes_result]}")

            rank_order_query = "SELECT rank_order, COUNT(*) FROM ranks GROUP BY rank_order HAVING COUNT(*) > 1"
            rank_order_result, _ = self.postgres_conn.execute_query(rank_order_query)
            if rank_order_result:
                errors.append(f"RankOrder duplicados encontrados: {[row[0] for row in rank_order_result]}")

            # Verificar que los valores numéricos están en rangos válidos
            validation_query = """
            SELECT id, name 
            FROM ranks 
            WHERE required_pay_leg_qv < 0 
               OR required_total_tree_qv < 0 
               OR required_directs < 0
               OR rank_order < 1
            """
            validation_result, _ = self.postgres_conn.execute_query(validation_query)
            if validation_result:
                errors.append(f"Ranks con valores inválidos: {[(row[0], row[1]) for row in validation_result]}")

            stats = {
                'total_ranks': total_ranks,
                'original_count': len(original_data),
                'missing_ids': len(missing_ids),
                'extra_ids': len(extra_ids)
            }

            is_valid = len(errors) == 0

            logger.info(f"Validación de integridad completada. Válida: {is_valid}")
            if errors:
                logger.error(f"Errores encontrados: {len(errors)}")
            if warnings:
                logger.warning(f"Advertencias encontradas: {len(warnings)}")

            return {
                'valid': is_valid,
                'errors': errors,
                'warnings': warnings,
                'stats': stats
            }

        except Exception as e:
            error_msg = f"Error durante validación de integridad: {str(e)}"
            logger.error(error_msg)
            return {
                'valid': False,
                'errors': [error_msg],
                'warnings': warnings,
                'stats': {'total_ranks': 0, 'original_count': len(original_data)}
            }

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        if self.postgres_conn:
            self.postgres_conn.disconnect()
from typing import List, Dict, Any
import json
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class UserRanksLoader:

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()

    def _check_table_exists(self):
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'user_ranks'
        );
        """
        result, _ = self.postgres_conn.execute_query(check_query)
        if not result[0][0]:
            raise RuntimeError("Tabla 'user_ranks' no existe en ms-points")

    def clear_existing_data(self) -> int:
        logger.info("Eliminando datos existentes de user_ranks")
        delete_query = "DELETE FROM user_ranks"
        deleted, _ = self.postgres_conn.execute_query(delete_query)
        # Resetear secuencia para permitir reinserción con IDs preservados
        self.postgres_conn.execute_query(
            "SELECT setval('user_ranks_id_seq', COALESCE((SELECT MAX(id) FROM user_ranks), 0) + 1, false);"
        )
        return deleted

    def load(self, rows: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Cargando {len(rows)} user_ranks en ms-points")
        try:
            self._check_table_exists()
            deleted = 0
            if clear_existing:
                deleted = self.clear_existing_data()
            if not rows:
                return {'success': True, 'inserted_count': 0, 'deleted_count': deleted}

            insert_query = """
            INSERT INTO user_ranks (
                id, user_id, user_email, user_name, current_rank_id, highest_rank_id, metadata, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            params = []
            for r in rows:
                md = r.get('metadata')
                if isinstance(md, dict):
                    md = json.dumps(md)
                params.append((
                    r['id'],
                    r.get('user_id'),
                    r.get('user_email'),
                    r.get('user_name'),
                    r.get('current_rank_id'),
                    r.get('highest_rank_id'),
                    md,
                    r.get('created_at'),
                    r.get('updated_at'),
                ))

            inserted = self.postgres_conn.execute_bulk_insert(insert_query, params)

            # Actualizar la secuencia al mayor ID
            max_id = max(r['id'] for r in rows)
            self.postgres_conn.execute_query("SELECT setval('user_ranks_id_seq', %s, true);", (max_id,))

            return {'success': True, 'inserted_count': inserted, 'deleted_count': deleted}

        except Exception as e:
            logger.error(f"Error cargando user_ranks: {str(e)}")
            return {'success': False, 'error': str(e)}

    def validate_data_integrity(self, expected_count: int) -> Dict[str, Any]:
        res = {'valid': True, 'errors': [], 'warnings': [], 'stats': {}}
        try:
            cnt, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM user_ranks")
            total = cnt[0][0]
            res['stats']['total_user_ranks'] = total
            if expected_count != total:
                res['warnings'].append(f"Conteo distinto: esperado {expected_count}, insertado {total}")
            # Verificar unicidad por user_id/user_email
            dup, _ = self.postgres_conn.execute_query(
                """
                SELECT user_email, COUNT(*) c
                FROM user_ranks
                GROUP BY user_email
                HAVING COUNT(*) > 1
                """
            )
            if dup:
                res['errors'].append(f"user_email duplicados: {[d[0] for d in dup]}")
                res['valid'] = False
            return res
        except Exception as e:
            res['valid'] = False
            res['errors'].append(str(e))
            return res

    def close_connection(self):
        self.postgres_conn.disconnect()

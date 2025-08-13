from typing import List, Dict, Any
import json
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MonthlyVolumeRanksLoader:

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()

    def _check_table_exists(self):
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'monthly_volume_ranks'
        );
        """
        result, _ = self.postgres_conn.execute_query(check_query)
        if not result[0][0]:
            raise RuntimeError("Tabla 'monthly_volume_ranks' no existe en ms-points")

    def clear_existing_data(self) -> int:
        logger.info("Eliminando datos existentes de monthly_volume_ranks")
        delete_query = "DELETE FROM monthly_volume_ranks"
        deleted, _ = self.postgres_conn.execute_query(delete_query)
        # Resetear secuencia para permitir reinserción con IDs preservados
        self.postgres_conn.execute_query(
            "SELECT setval('monthly_volume_ranks_id_seq', COALESCE((SELECT MAX(id) FROM monthly_volume_ranks), 0) + 1, false);"
        )
        return deleted

    def load(self, rows: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        logger.info(f"Cargando {len(rows)} monthly_volume_ranks en ms-points")
        try:
            self._check_table_exists()
            deleted = 0
            if clear_existing:
                deleted = self.clear_existing_data()
            if not rows:
                return {'success': True, 'inserted_count': 0, 'deleted_count': deleted}

            insert_query = """
            INSERT INTO monthly_volume_ranks (
                id, user_id, user_email, user_name, assigned_rank_id,
                total_volume, left_volume, right_volume,
                left_directs, right_directs,
                month_start_date, month_end_date,
                status, metadata, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s
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
                    r.get('assigned_rank_id'),
                    r.get('total_volume'),
                    r.get('left_volume'),
                    r.get('right_volume'),
                    r.get('left_directs'),
                    r.get('right_directs'),
                    r.get('month_start_date'),
                    r.get('month_end_date'),
                    r.get('status'),
                    md,
                    r.get('created_at'),
                    r.get('updated_at'),
                ))

            inserted = self.postgres_conn.execute_bulk_insert(insert_query, params)

            # Actualizar la secuencia al mayor ID
            max_id = max(r['id'] for r in rows)
            self.postgres_conn.execute_query("SELECT setval('monthly_volume_ranks_id_seq', %s, true);", (max_id,))

            return {'success': True, 'inserted_count': inserted, 'deleted_count': deleted}

        except Exception as e:
            logger.error(f"Error cargando monthly_volume_ranks: {str(e)}")
            return {'success': False, 'error': str(e)}

    def validate_data_integrity(self, expected_count: int = None) -> Dict[str, Any]:
        res = {'valid': True, 'errors': [], 'warnings': [], 'stats': {}}
        try:
            cnt, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM monthly_volume_ranks")
            total = cnt[0][0]
            res['stats']['total_monthly_volume_ranks'] = total
            if expected_count is not None and expected_count != total:
                res['warnings'].append(f"Conteo distinto: esperado {expected_count}, insertado {total}")

            # Fechas inválidas
            inv, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM monthly_volume_ranks
                WHERE month_end_date <= month_start_date
            """)
            if inv[0][0] > 0:
                res['errors'].append(f"{inv[0][0]} registros con rango de mes inválido")
                res['valid'] = False

            # Volúmenes negativos
            neg, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM monthly_volume_ranks
                WHERE left_volume < 0 OR right_volume < 0 OR total_volume < 0
            """)
            if neg[0][0] > 0:
                res['errors'].append(f"{neg[0][0]} registros con volúmenes negativos")
                res['valid'] = False

            # Unicidad por usuario/mes si user_id no es null
            dup, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM (
                    SELECT user_id, month_start_date, COUNT(*) c
                    FROM monthly_volume_ranks
                    WHERE user_id IS NOT NULL
                    GROUP BY user_id, month_start_date
                    HAVING COUNT(*) > 1
                ) t
            """)
            if dup[0][0] > 0:
                res['warnings'].append(f"{dup[0][0]} duplicados por usuario/mes")

            return res
        except Exception as e:
            res['valid'] = False
            res['errors'].append(str(e))
            return res

    def close_connection(self):
        self.postgres_conn.disconnect()

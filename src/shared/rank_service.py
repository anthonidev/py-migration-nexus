# src/shared/rank_service.py
from typing import Optional, Dict, Any, List
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RankService:
    """Servicio para obtener información de ranks desde ms-points"""

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()

    def get_rank_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Obtiene un rank por su código único"""
        try:
            code = code.upper().strip()
            
            query = """
            SELECT 
                id,
                name,
                code,
                required_pay_leg_qv,
                required_total_tree_qv,
                required_directs,
                rank_order,
                is_active
            FROM ranks 
            WHERE code = %s AND is_active = true
            """
            
            results, columns = self.postgres_conn.execute_query(query, (code,))
            
            if not results:
                logger.warning(f"Rank no encontrado: CODE {code}")
                return None
            
            rank_data = dict(zip(columns, results[0]))
            
            result = {
                'id': rank_data['id'],
                'name': rank_data['name'],
                'code': rank_data['code'],
                'requiredPayLegQv': float(rank_data['required_pay_leg_qv']) if rank_data['required_pay_leg_qv'] else 0.0,
                'requiredTotalTreeQv': float(rank_data['required_total_tree_qv']) if rank_data['required_total_tree_qv'] else 0.0,
                'requiredDirects': rank_data['required_directs'],
                'rankOrder': rank_data['rank_order'],
                'isActive': rank_data['is_active']
            }
            
            logger.info(f"Rank encontrado: {code} -> ID {result['id']}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo rank por código {code}: {str(e)}")
            return None

    def get_rank_id_by_code(self, code: str) -> Optional[int]:
        """Obtiene solo el ID de un rank por su código (método optimizado)"""
        try:
            code = code.upper().strip()
            
            query = """
            SELECT id
            FROM ranks 
            WHERE code = %s AND is_active = true
            """
            
            results, _ = self.postgres_conn.execute_query(query, (code,))
            
            if not results:
                logger.warning(f"Rank ID no encontrado: CODE {code}")
                return None
            
            rank_id = results[0][0]
            logger.info(f"Rank ID encontrado: {code} -> {rank_id}")
            return rank_id
            
        except Exception as e:
            logger.error(f"Error obteniendo rank ID por código {code}: {str(e)}")
            return None

    def get_ranks_batch_by_codes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Obtiene múltiples ranks por sus códigos en una sola consulta"""
        if not codes:
            logger.warning("Lista de códigos vacía")
            return {}
        
        try:
            # Normalizar códigos (uppercase y eliminar duplicados)
            normalized_codes = list(set(code.upper().strip() for code in codes if code and code.strip()))
            
            if not normalized_codes:
                logger.warning("No hay códigos válidos después de la normalización")
                return {}
            
            placeholders = ','.join(['%s'] * len(normalized_codes))
            
            query = f"""
            SELECT 
                id,
                name,
                code,
                required_pay_leg_qv,
                required_total_tree_qv,
                required_directs,
                rank_order,
                is_active
            FROM ranks 
            WHERE code IN ({placeholders}) AND is_active = true
            """
            
            results, columns = self.postgres_conn.execute_query(query, normalized_codes)
            
            if not results:
                logger.warning(f"Ningún rank encontrado para los códigos: {normalized_codes}")
                return {}
            
            ranks_dict = {}
            
            for row in results:
                rank_data = dict(zip(columns, row))
                code = rank_data['code']
                
                ranks_dict[code] = {
                    'id': rank_data['id'],
                    'name': rank_data['name'],
                    'code': rank_data['code'],
                    'requiredPayLegQv': float(rank_data['required_pay_leg_qv']) if rank_data['required_pay_leg_qv'] else 0.0,
                    'requiredTotalTreeQv': float(rank_data['required_total_tree_qv']) if rank_data['required_total_tree_qv'] else 0.0,
                    'requiredDirects': rank_data['required_directs'],
                    'rankOrder': rank_data['rank_order'],
                    'isActive': rank_data['is_active']
                }
            
            found_count = len(ranks_dict)
            total_requested = len(normalized_codes)
            
            if found_count < total_requested:
                missing_codes = set(normalized_codes) - set(ranks_dict.keys())
                logger.warning(f"No se encontraron {total_requested - found_count} ranks: {missing_codes}")
            
            logger.info(f"Ranks obtenidos en lote: {found_count}/{total_requested}")
            return ranks_dict
            
        except Exception as e:
            logger.error(f"Error obteniendo ranks en lote {codes}: {str(e)}")
            return {}

    def get_rank_ids_by_codes(self, codes: List[str]) -> Dict[str, int]:
        """Obtiene solo los IDs de múltiples ranks por sus códigos (método optimizado)"""
        if not codes:
            logger.warning("Lista de códigos vacía")
            return {}
        
        try:
            # Normalizar códigos
            normalized_codes = list(set(code.upper().strip() for code in codes if code and code.strip()))
            
            if not normalized_codes:
                logger.warning("No hay códigos válidos después de la normalización")
                return {}
            
            placeholders = ','.join(['%s'] * len(normalized_codes))
            
            query = f"""
            SELECT code, id
            FROM ranks 
            WHERE code IN ({placeholders}) AND is_active = true
            """
            
            results, _ = self.postgres_conn.execute_query(query, normalized_codes)
            
            if not results:
                logger.warning(f"Ningún rank ID encontrado para los códigos: {normalized_codes}")
                return {}
            
            rank_ids_dict = {row[0]: row[1] for row in results}
            
            found_count = len(rank_ids_dict)
            total_requested = len(normalized_codes)
            
            if found_count < total_requested:
                missing_codes = set(normalized_codes) - set(rank_ids_dict.keys())
                logger.warning(f"No se encontraron {total_requested - found_count} rank IDs: {missing_codes}")
            
            logger.info(f"Rank IDs obtenidos en lote: {found_count}/{total_requested}")
            return rank_ids_dict
            
        except Exception as e:
            logger.error(f"Error obteniendo rank IDs en lote {codes}: {str(e)}")
            return {}

    def get_all_ranks(self, only_active: bool = True) -> List[Dict[str, Any]]:
        """Obtiene todos los ranks disponibles"""
        try:
            query = """
            SELECT 
                id,
                name,
                code,
                required_pay_leg_qv,
                required_total_tree_qv,
                required_directs,
                required_active_teams,
                required_qualified_teams,
                required_qualified_rank_id,
                max_sponsorship_branch_qv,
                max_leg_balance_percentage,
                min_depth_levels,
                rank_order,
                is_active,
                benefits,
                description,
                created_at,
                updated_at
            FROM ranks 
            WHERE is_active = %s OR %s = false
            ORDER BY rank_order ASC
            """
            
            results, columns = self.postgres_conn.execute_query(query, (True, only_active))
            
            if not results:
                logger.warning("No se encontraron ranks")
                return []
            
            ranks_list = []
            
            for row in results:
                rank_data = dict(zip(columns, row))
                
                rank = {
                    'id': rank_data['id'],
                    'name': rank_data['name'],
                    'code': rank_data['code'],
                    'requiredPayLegQv': float(rank_data['required_pay_leg_qv']) if rank_data['required_pay_leg_qv'] else 0.0,
                    'requiredTotalTreeQv': float(rank_data['required_total_tree_qv']) if rank_data['required_total_tree_qv'] else 0.0,
                    'requiredDirects': rank_data['required_directs'],
                    'requiredActiveTeams': rank_data['required_active_teams'],
                    'requiredQualifiedTeams': rank_data['required_qualified_teams'],
                    'requiredQualifiedRankId': rank_data['required_qualified_rank_id'],
                    'maxSponsorshipBranchQv': float(rank_data['max_sponsorship_branch_qv']) if rank_data['max_sponsorship_branch_qv'] else None,
                    'maxLegBalancePercentage': float(rank_data['max_leg_balance_percentage']) if rank_data['max_leg_balance_percentage'] else None,
                    'minDepthLevels': rank_data['min_depth_levels'],
                    'rankOrder': rank_data['rank_order'],
                    'isActive': rank_data['is_active'],
                    'benefits': rank_data['benefits'],
                    'description': rank_data['description'],
                    'createdAt': rank_data['created_at'],
                    'updatedAt': rank_data['updated_at']
                }
                
                ranks_list.append(rank)
            
            logger.info(f"Obtenidos {len(ranks_list)} ranks")
            return ranks_list
            
        except Exception as e:
            logger.error(f"Error obteniendo todos los ranks: {str(e)}")
            return []

    def rank_exists(self, code: str) -> bool:
        """Verifica si existe un rank con el código especificado"""
        try:
            code = code.upper().strip()
            
            query = """
            SELECT EXISTS(
                SELECT 1 FROM ranks 
                WHERE code = %s AND is_active = true
            )
            """
            
            results, _ = self.postgres_conn.execute_query(query, (code,))
            exists = results[0][0] if results else False
            
            logger.info(f"Rank {code} {'existe' if exists else 'no existe'}")
            return exists
            
        except Exception as e:
            logger.error(f"Error verificando existencia del rank {code}: {str(e)}")
            return False

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        if self.postgres_conn:
            self.postgres_conn.disconnect()
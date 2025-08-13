from typing import List, Dict, Any, Tuple
from src.shared.user_service import UserService
from src.shared.rank_service import RankService
from src.utils.logger import get_logger

logger = get_logger(__name__)


class UserRanksTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.rank_service = RankService()
        self.stats = {
            'user_ranks_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transforma filas del monolito a esquema ms-points.user_ranks
        - Conserva id original
        - Resuelve userId, userEmail, userName desde ms-users
        - Resuelve IDs de ranks por código vía RankService en batch
        """
        if not rows:
            return []

        # 1) Preparar lookups masivos
        emails = [r.get('user_email') for r in rows if r.get('user_email')]
        users_by_email = self.user_service.get_users_batch(emails)

        # Recolectar códigos de ranks a resolver
        codes = []
        for r in rows:
            if r.get('current_rank_code'):
                codes.append(str(r['current_rank_code']).upper().strip())
            if r.get('highest_rank_code'):
                codes.append(str(r['highest_rank_code']).upper().strip())
        rank_ids_by_code = self.rank_service.get_rank_ids_by_codes(codes) if codes else {}

        transformed: List[Dict[str, Any]] = []

        for r in rows:
            try:
                email = r.get('user_email') or ''
                uinfo = users_by_email.get(email) if email else None

                if not uinfo:
                    # Usuario no encontrado; mantenemos email y dejamos user_id/user_name en None
                    self.stats['warnings'].append(f"Usuario no encontrado en ms-users: {email}")

                current_code = (r.get('current_rank_code') or '').upper().strip()
                highest_code = (r.get('highest_rank_code') or None)
                highest_code = highest_code.upper().strip() if highest_code else None

                current_rank_id = rank_ids_by_code.get(current_code)
                highest_rank_id = rank_ids_by_code.get(highest_code) if highest_code else None

                if not current_rank_id:
                    raise ValueError(f"Rank actual no encontrado para código: {current_code}")

                # metadata puede venir como dict o string JSON; lo dejamos tal cual y el loader lo manejará
                transformed.append({
                    'id': r['id'],
                    'user_id': uinfo['id'] if uinfo else None,
                    'user_email': uinfo['email'] if uinfo else email,
                    'user_name': uinfo.get('fullName') if uinfo else None,
                    'current_rank_id': current_rank_id,
                    'highest_rank_id': highest_rank_id,
                    'metadata': r.get('metadata') or {},
                    'created_at': r.get('created_at'),
                    'updated_at': r.get('updated_at'),
                })
                self.stats['user_ranks_transformed'] += 1

            except Exception as e:
                msg = f"Error transformando user_rank id={r.get('id')}: {str(e)}"
                logger.error(msg)
                self.stats['errors'].append(msg)

        return transformed

    def validate(self, transformed: List[Dict[str, Any]]) -> Dict[str, Any]:
        res = {'valid': True, 'errors': [], 'warnings': []}
        ids = set()
        for ur in transformed:
            if ur['id'] in ids:
                res['errors'].append(f"ID duplicado: {ur['id']}")
            ids.add(ur['id'])
            if not ur.get('user_email'):
                res['errors'].append(f"user_email requerido en id={ur['id']}")
            if not ur.get('current_rank_id'):
                res['errors'].append(f"current_rank_id requerido en id={ur['id']}")
        if res['errors']:
            res['valid'] = False
        return res

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'user_ranks_transformed': self.stats['user_ranks_transformed'],
            'total_errors': len(self.stats['errors']),
            'total_warnings': len(self.stats['warnings']),
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings'],
        }

    def close_connections(self):
        try:
            self.user_service.close_connection()
            self.rank_service.close_connection()
        except Exception:
            pass

from typing import List, Dict, Any, Optional
from datetime import datetime, date
import json
from src.shared.user_service import UserService
from src.shared.rank_service import RankService
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MonthlyVolumeRanksTransformer:

    def __init__(self):
        self.user_service = UserService()
        self.rank_service = RankService()
        self.users_cache: Dict[str, Dict[str, Any]] = {}
        self.rank_ids_cache: Dict[str, int] = {}
        self.stats = {
            'monthly_volumes_transformed': 0,
            'errors': [],
            'warnings': []
        }

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []

        # 1) Cache de usuarios por email (batch)
        emails = [r.get('user_email') for r in rows if r.get('user_email')]
        self.users_cache = self.user_service.get_users_batch(emails)

        # 2) Cache de ranks (batch por códigos)
        codes = []
        for r in rows:
            if r.get('assigned_rank_code'):
                codes.append(str(r['assigned_rank_code']).upper().strip())
        self.rank_ids_cache = self.rank_service.get_rank_ids_by_codes(codes) if codes else {}

        transformed: List[Dict[str, Any]] = []
        for r in rows:
            try:
                t = self._transform_single(r)
                transformed.append(t)
                self.stats['monthly_volumes_transformed'] += 1
            except Exception as e:
                msg = f"Error transformando monthly_volume id={r.get('id')}: {str(e)}"
                logger.error(msg)
                self.stats['errors'].append(msg)

        return transformed

    def _transform_single(self, r: Dict[str, Any]) -> Dict[str, Any]:
        email = (r.get('user_email') or '').lower().strip()
        uinfo = self.users_cache.get(email)
        if not uinfo:
            self.stats['warnings'].append(f"Usuario no encontrado en ms-users: {email}")

        # Volúmenes numéricos >= 0
        total = self._validate_decimal(r.get('totalVolume'), 'totalVolume', 0)
        left = self._validate_decimal(r.get('leftVolume'), 'leftVolume', 0)
        right = self._validate_decimal(r.get('rightVolume'), 'rightVolume', 0)

        # Directos int >= 0
        left_directs = self._validate_int(r.get('leftDirects'), 'leftDirects', 0)
        right_directs = self._validate_int(r.get('rightDirects'), 'rightDirects', 0)

        # Fechas del mes
        msd = self._to_date(r.get('monthStartDate'))
        med = self._to_date(r.get('monthEndDate'))
        if not msd or not med:
            raise ValueError('monthStartDate y monthEndDate son requeridos')
        if med <= msd:
            raise ValueError('monthEndDate debe ser posterior a monthStartDate')

        # Estado
        status = self._map_status(r.get('status'))

        # Rank asignado por código (opcional)
        assigned_code = r.get('assigned_rank_code')
        assigned_rank_id = None
        if assigned_code:
            assigned_rank_id = self.rank_ids_cache.get(str(assigned_code).upper().strip())
            if assigned_rank_id is None:
                # Si no existe en ms-points, lo dejamos null y warning
                self.stats['warnings'].append(f"Rank asignado no encontrado: {assigned_code}")

        metadata = r.get('metadata') or {}
        # Normalizar metadata a dict si viene string JSON
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        created_at = self._to_datetime(r.get('createdAt'))
        updated_at = self._to_datetime(r.get('updatedAt'))

        return {
            'id': r['id'],  # conservar ID
            'user_id': uinfo['id'] if uinfo else None,
            'user_email': uinfo['email'] if uinfo else email,
            'user_name': uinfo.get('fullName') if uinfo else None,
            'assigned_rank_id': assigned_rank_id,
            'total_volume': total,
            'left_volume': left,
            'right_volume': right,
            'left_directs': left_directs,
            'right_directs': right_directs,
            'month_start_date': msd,
            'month_end_date': med,
            'status': status,
            'metadata': metadata,
            'created_at': created_at,
            'updated_at': updated_at,
        }

    def validate(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        res = {'valid': True, 'errors': [], 'warnings': []}
        ids = set()
        for r in rows:
            rid = r['id']
            if rid in ids:
                res['errors'].append(f"ID duplicado: {rid}")
            ids.add(rid)
            if not r.get('user_email'):
                res['errors'].append(f"user_email requerido en id={rid}")
            if r['left_volume'] < 0 or r['right_volume'] < 0 or r['total_volume'] < 0:
                res['errors'].append(f"volúmenes negativos en id={rid}")
            if not r.get('month_start_date') or not r.get('month_end_date'):
                res['errors'].append(f"fechas de mes requeridas en id={rid}")
            elif r['month_end_date'] <= r['month_start_date']:
                res['errors'].append(f"rango de mes inválido en id={rid}")
        if res['errors']:
            res['valid'] = False
        return res

    def get_transformation_summary(self) -> Dict[str, Any]:
        return {
            'monthly_volumes_transformed': self.stats['monthly_volumes_transformed'],
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

    # Helpers
    def _validate_decimal(self, v: Any, name: str, min_v: float = None) -> float:
        if v is None:
            raise ValueError(f"{name} es requerido")
        try:
            f = float(v)
        except Exception:
            raise ValueError(f"{name} debe ser numérico")
        if min_v is not None and f < min_v:
            raise ValueError(f"{name} no puede ser menor que {min_v}")
        return f

    def _validate_int(self, v: Any, name: str, min_v: int = None) -> int:
        if v is None:
            return 0 if min_v is not None and min_v == 0 else 0
        try:
            i = int(v)
        except Exception:
            i = 0
        if min_v is not None and i < min_v:
            i = min_v
        return i

    def _to_date(self, v: Any) -> Optional[date]:
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            for fmt in (
                '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                '%d/%m/%Y', '%Y/%m/%d'
            ):
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
        return None

    def _to_datetime(self, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            for fmt in (
                '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d'
            ):
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
        return None

    def _map_status(self, status: Optional[str]) -> str:
        if not status:
            return 'PENDING'
        s = status.upper().strip()
        mapping = {
            'PENDING': 'PENDING', 'PENDIENTE': 'PENDING',
            'PROCESSED': 'PROCESSED', 'PROCESADO': 'PROCESSED', 'FINALIZADO': 'PROCESSED',
            'CANCELLED': 'CANCELLED', 'CANCELADO': 'CANCELLED', 'CANCELED': 'CANCELLED'
        }
        return mapping.get(s, 'PENDING')

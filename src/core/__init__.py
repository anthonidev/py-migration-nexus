"""
Scripts principales de migración
"""
from .roles_views_migration import main as migrate_roles_views
from .users_migration import main as migrate_users

__all__ = ['migrate_roles_views', 'migrate_users']
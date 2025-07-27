"""
Scripts principales de migración
"""
from .roles_views_migration import main as migrate_roles_views

__all__ = ['migrate_roles_views']

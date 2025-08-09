"""
Scripts principales de migración
"""
from .roles_views_migration import main as migrate_roles_views
from .users_migration import main as migrate_users
from .payment_configs_migration import main as migrate_payment_configs
from .payments_migration import main as migrate_payments
from .withdrawals_migration import main as migrate_withdrawals 
from .membership_plans_migration import main as migrate_membership_plans
from .memberships_migration import main as migrate_memberships
from .user_points_migration import main as migrate_user_points
from .weekly_volumes_migration import main as migrate_weekly_volumes
from .products_migration import main as migrate_products
from .orders_migration import main as migrate_orders
from .ranks_migration import main as migrate_ranks

__all__ = ['migrate_roles_views', 'migrate_users',
           'migrate_payment_configs', 'migrate_payments', 'migrate_withdrawals',
           'migrate_membership_plans', 'migrate_memberships',
           'migrate_user_points', 'migrate_weekly_volumes',
           'migrate_products', 'migrate_orders', 'migrate_ranks']
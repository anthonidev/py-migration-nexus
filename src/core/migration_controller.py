import os
import sys
from typing import Dict, List, Callable
from rich.console import Console

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.logger import get_logger

console = Console()
logger = get_logger(__name__)

class MigrationController:


    def __init__(self):
        self.modules = self._init_modules()
        self.env_requirements = self._init_env_requirements()

    def _init_modules(self) -> Dict[str, Dict[str, Callable]]:
        
        def migrate_roles_views():
            from src.core.roles_views_migration import main
            return main()

        def migrate_users():
            from src.core.users_migration import main
            return main()

        def migrate_payment_configs():
            from src.core.payment_configs_migration import main
            return main()

        def migrate_payments():
            from src.core.payments_migration import main
            return main()
        
        def migrate_withdrawals():
            from src.core.withdrawals_migration import main
            return main()

        def migrate_membership_plans():
            from src.core.membership_plans_migration import main
            return main()

        def migrate_memberships():
            from src.core.memberships_migration import main
            return main()
        
        def migrate_user_points():
            from src.core.user_points_migration import main
            return main()
        
        def migrate_weekly_volumes():
            from src.core.weekly_volumes_migration import main
            return main()
        
        def migrate_products():
            from src.core.products_migration import main
            return main()
        
        def migrate_orders():
            from src.core.orders_migration import main
            return main()
        
        def migrate_ranks():
            from src.core.ranks_migration import main
            return main()


        return {
            "ms-users": {
                "roles-views": migrate_roles_views,
                "users": migrate_users,
            },
            "ms-payments": {
                "payment-configs": migrate_payment_configs,
                "payments": migrate_payments,
                "withdrawals": migrate_withdrawals,  
            },
            "ms-membership": {
                "membership-plans": migrate_membership_plans,
                "memberships": migrate_memberships,
            },
             "ms-points": {
                "user-points": migrate_user_points,
                "weekly-volumes": migrate_weekly_volumes,
                "ranks": migrate_ranks, 
            },
            "ms-orders": {
                "products": migrate_products,
                "orders": migrate_orders,
            }
        }

    def _init_env_requirements(self) -> Dict[str, Dict[str, List[str]]]:
        """Variables de entorno requeridas por migración"""
        return {
            "ms-users": {
                "roles-views": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER'],
                "users": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER']
            },
            "ms-payments": {
                "payment-configs": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS'],
                "payments": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS', 'MS_NEXUS_USER'],
                "withdrawals": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS', 'MS_NEXUS_USER'] 
            },
            "ms-membership": {
                "membership-plans": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_MEMBERSHIP'],
                "memberships": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_MEMBERSHIP', 'MS_NEXUS_USER']
            },
            "ms-points": {
                "user-points": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_POINTS', 'MS_NEXUS_USER', 'MS_NEXUS_PAYMENTS'],
                "weekly-volumes": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_POINTS', 'MS_NEXUS_USER'],
                "ranks": ['MS_NEXUS_POINTS'] 
            },
             "ms-orders": {
                "products": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_ORDERS', 'MS_NEXUS_USER'],
                "orders": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_ORDERS', 'MS_NEXUS_USER']
            }
        }

    def get_modules(self) -> Dict[str, Dict[str, Callable]]:
        """Retorna módulos disponibles"""
        return self.modules

    def get_all_env_vars(self) -> List[str]:
        """Retorna todas las variables de entorno únicas"""
        all_vars = set(['NEXUS_POSTGRES_URL'])
        for module in self.env_requirements.values():
            for vars_list in module.values():
                all_vars.update(vars_list)
        return sorted(list(all_vars))

    def check_env_vars(self, module_name: str, submodule_name: str) -> List[str]:
        required = self.env_requirements.get(module_name, {}).get(submodule_name, [])
        return [var for var in required if not os.getenv(var)]

    def execute_migration(self, module_name: str, submodule_name: str) -> bool:
        try:
            logger.info(f"🚀 Iniciando: {module_name} -> {submodule_name}")
            
            migration_func = self.modules[module_name][submodule_name]
            success = migration_func()
            
            if success:
                logger.info(f"✅ Completado: {module_name} -> {submodule_name}")
            else:
                logger.error(f"❌ Falló: {module_name} -> {submodule_name}")
            
            return success

        except Exception as e:
            logger.error(f"💥 Error crítico: {str(e)}")
            return False
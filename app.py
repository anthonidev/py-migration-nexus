#!/usr/bin/env python3
"""
Aplicación principal de migración de monolito a microservicios
"""
from src.utils.logger import get_logger
import os
import sys
from typing import Dict, List, Callable

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = get_logger(__name__)


class MigrationApp:
    """Aplicación principal para gestionar migraciones"""

    def __init__(self):
        self.modules = self._initialize_modules()

    def _initialize_modules(self) -> Dict[str, Dict[str, Callable]]:
        """Inicializa los módulos y submódulos disponibles"""
        # Importar dinámicamente para evitar errores de importación al inicio
        def migrate_roles_views():
            from src.core.roles_views_migration import main
            return main()

        def migrate_users():
            from src.core.users_migration import main
            return main()

        def migrate_payment_configs():
            from src.core.payment_configs_migration import main
            return main()

        return {
            "ms-users": {
                "roles-views": migrate_roles_views,
                "users": migrate_users,
            },
            "ms-payments": {
                "payment-configs": migrate_payment_configs,
                # "payments": migrate_payments,  # TODO: Implementar después
            },
            # TODO: Agregar otros microservicios
            # "ms-membership": {
            #     "memberships": migrate_memberships,
            # },
            # "ms-points": {
            #     "points": migrate_points,
            # },
            # "ms-orders": {
            #     "orders": migrate_orders,
            # }
        }

    def display_banner(self):
        """Muestra el banner de la aplicación"""
        print("""
╔══════════════════════════════════════════════════════════════════╗
║                    🚀 NEXUS MIGRATION TOOL 🚀                   ║
║              Migración de Monolito a Microservicios             ║
╚══════════════════════════════════════════════════════════════════╝
        """)

    def display_modules(self):
        """Muestra los módulos disponibles"""
        print("\n📦 MÓDULOS DISPONIBLES:")
        print("=" * 50)

        for i, (module_name, submodules) in enumerate(self.modules.items(), 1):
            status = "✅ Disponible" if submodules else "🚧 En desarrollo"
            print(f"{i}. {module_name} - {status}")

            if submodules:
                for j, submodule_name in enumerate(submodules.keys(), 1):
                    print(f"   {i}.{j} {submodule_name}")

        print(f"{len(self.modules) + 1}. 🚪 Salir")

    def display_submodules(self, module_name: str):
        """Muestra los submódulos de un módulo específico"""
        submodules = self.modules.get(module_name, {})

        if not submodules:
            print(f"\n⚠️  No hay submódulos disponibles para {module_name}")
            return

        print(f"\n📋 SUBMÓDULOS DE {module_name.upper()}:")
        print("=" * 50)

        for i, submodule_name in enumerate(submodules.keys(), 1):
            print(f"{i}. {submodule_name}")

        print(f"{len(submodules) + 1}. ⬅️  Volver al menú principal")

    def validate_environment(self) -> bool:
        """Valida que las variables de entorno necesarias estén configuradas"""
        required_vars = ['NEXUS_POSTGRES_URL']
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            print("\n❌ VARIABLES DE ENTORNO FALTANTES:")
            for var in missing_vars:
                print(f"   - {var}")
            print("\n💡 Configura las variables en tu archivo .env:")
            print("   NEXUS_POSTGRES_URL=postgresql://user:pass@host:port/db")
            print("   MS_NEXUS_USER=mongodb://user:pass@host:port/db")
            print("   MS_NEXUS_PAYMENTS=postgresql://user:pass@host:port/db")
            return False

        print("✅ Variables de entorno básicas configuradas correctamente")
        return True

    def validate_specific_environment(self, module_name: str, submodule_name: str) -> bool:
        """Valida variables de entorno específicas para cada migración"""
        module_requirements = {
            "ms-users": {
                "roles-views": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER'],
                "users": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_USER']
            },
            "ms-payments": {
                "payment-configs": ['NEXUS_POSTGRES_URL', 'MS_NEXUS_PAYMENTS']
            }
        }

        required_vars = module_requirements.get(
            module_name, {}).get(submodule_name, [])
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            print(
                f"\n❌ VARIABLES FALTANTES PARA {module_name} -> {submodule_name}:")
            for var in missing_vars:
                print(f"   - {var}")
            return False

        return True

    def get_user_choice(self, options_count: int) -> int:
        """Obtiene la elección del usuario"""
        while True:
            try:
                choice = input(
                    f"\n🎯 Selecciona una opción (1-{options_count}): ").strip()
                choice_num = int(choice)

                if 1 <= choice_num <= options_count:
                    return choice_num
                else:
                    print(
                        f"❌ Por favor, ingresa un número entre 1 y {options_count}")

            except ValueError:
                print("❌ Por favor, ingresa un número válido")
            except KeyboardInterrupt:
                print("\n\n👋 ¡Hasta luego!")
                sys.exit(0)

    def confirm_migration(self, module_name: str, submodule_name: str) -> bool:
        """Confirma la migración antes de ejecutarla"""
        print(f"\n⚠️  CONFIRMACIÓN DE MIGRACIÓN")
        print("=" * 50)
        print(f"📦 Módulo: {module_name}")
        print(f"📋 Submódulo: {submodule_name}")
        print("\n🚨 ADVERTENCIA: Esta operación:")
        print("   • Eliminará datos existentes en el destino")
        print("   • Migrará datos desde la base origen")
        print("   • Puede tomar varios minutos")

        # Advertencias específicas por tipo de migración
        if submodule_name == "users":
            print("\n📋 REQUISITOS ESPECÍFICOS PARA USUARIOS:")
            print("   • Los roles y vistas deben estar migrados previamente")
            print(
                "   • Se generarán documentos aleatorios para usuarios sin documentNumber")
            print("   • Se establecerán relaciones jerárquicas padre-hijo")
        elif submodule_name == "payment-configs":
            print("\n📋 REQUISITOS ESPECÍFICOS PARA CONFIGURACIONES DE PAGO:")
            print("   • Se conservarán los IDs originales de las configuraciones")
            print(
                "   • Los códigos se transformarán a mayúsculas y se reemplazarán espacios")
            print("   • Se validarán longitudes de campos según la nueva entidad")

        while True:
            confirm = input(
                "\n¿Estás seguro de continuar? (s/N): ").strip().lower()

            if confirm in ['s', 'si', 'sí', 'y', 'yes']:
                return True
            elif confirm in ['n', 'no', ''] or not confirm:
                return False
            else:
                print("❌ Por favor responde 's' para sí o 'n' para no")

    def execute_migration(self, module_name: str, submodule_name: str) -> bool:
        """Ejecuta la migración seleccionada"""
        # Validar variables de entorno específicas
        if not self.validate_specific_environment(module_name, submodule_name):
            print("❌ No se puede continuar sin las variables de entorno específicas")
            return False

        migration_func = self.modules[module_name][submodule_name]

        print(f"\n🚀 INICIANDO MIGRACIÓN: {module_name} -> {submodule_name}")
        print("=" * 60)

        try:
            success = migration_func()

            if success:
                print(f"\n🎉 ¡MIGRACIÓN COMPLETADA EXITOSAMENTE!")
                print(
                    f"✅ {module_name} -> {submodule_name} migrado correctamente")

                # Consejos post-migración
                if submodule_name == "roles-views":
                    print("\n💡 SIGUIENTE PASO RECOMENDADO:")
                    print("   • Ahora puedes migrar los usuarios")
                elif submodule_name == "users":
                    print("\n💡 MIGRACIÓN COMPLETADA:")
                    print("   • Usuarios migrados con sus relaciones jerárquicas")
                    print(
                        "   • Revisa el reporte generado para estadísticas detalladas")
                elif submodule_name == "payment-configs":
                    print("\n💡 MIGRACIÓN COMPLETADA:")
                    print(
                        "   • Configuraciones de pago migradas conservando IDs originales")
                    print("   • Ahora puedes migrar los pagos de usuarios")
            else:
                print(f"\n💥 MIGRACIÓN FALLÓ")
                print(f"❌ Error en {module_name} -> {submodule_name}")
                print("📄 Revisa los logs y el reporte de errores generado")

            return success

        except Exception as e:
            logger.error(f"Error crítico durante la migración: {str(e)}")
            print(f"\n💥 ERROR CRÍTICO: {str(e)}")
            return False

    def run(self):
        """Ejecuta la aplicación principal"""
        self.display_banner()

        # Validar entorno básico
        if not self.validate_environment():
            print("\n❌ No se puede continuar sin las variables de entorno básicas")
            sys.exit(1)

        while True:
            self.display_modules()

            # Obtener selección de módulo
            module_choice = self.get_user_choice(len(self.modules) + 1)

            # Opción de salir
            if module_choice == len(self.modules) + 1:
                print("\n👋 ¡Hasta luego!")
                break

            # Obtener módulo seleccionado
            module_names = list(self.modules.keys())
            selected_module = module_names[module_choice - 1]
            submodules = self.modules[selected_module]

            # Verificar si el módulo tiene submódulos
            if not submodules:
                print(
                    f"\n🚧 {selected_module} está en desarrollo. Intenta más tarde.")
                input("\nPresiona Enter para continuar...")
                continue

            # Mostrar submódulos
            while True:
                self.display_submodules(selected_module)

                # Obtener selección de submódulo
                submodule_choice = self.get_user_choice(len(submodules) + 1)

                # Opción de volver
                if submodule_choice == len(submodules) + 1:
                    break

                # Obtener submódulo seleccionado
                submodule_names = list(submodules.keys())
                selected_submodule = submodule_names[submodule_choice - 1]

                # Confirmar migración
                if self.confirm_migration(selected_module, selected_submodule):
                    # Ejecutar migración
                    success = self.execute_migration(
                        selected_module, selected_submodule)

                    if success:
                        # Preguntar si quiere hacer otra migración
                        while True:
                            another = input(
                                "\n¿Quieres realizar otra migración? (s/N): ").strip().lower()
                            if another in ['s', 'si', 'sí', 'y', 'yes']:
                                break
                            elif another in ['n', 'no', ''] or not another:
                                print("\n👋 ¡Hasta luego!")
                                sys.exit(0)
                            else:
                                print(
                                    "❌ Por favor responde 's' para sí o 'n' para no")
                    else:
                        input("\nPresiona Enter para continuar...")
                else:
                    print("❌ Migración cancelada")
                    input("\nPresiona Enter para continuar...")


def main():
    """Función principal"""
    try:
        app = MigrationApp()
        app.run()
    except KeyboardInterrupt:
        print("\n\n👋 ¡Hasta luego!")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error en la aplicación: {str(e)}")
        print(f"\n💥 Error inesperado: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

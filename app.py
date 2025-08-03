import os
import sys
from rich.console import Console

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.ui.components import UIComponents
from src.core.migration_controller import MigrationController
from src.utils.logger import get_logger

console = Console()
logger = get_logger(__name__)

class MigrationApp:

    def __init__(self):
        self.controller = MigrationController()
        self.ui = UIComponents()

    def run(self):
        try:
            if not self._check_basic_env():
                return False

            while True:
                self._show_main_menu()
                
                choice = self.ui.get_choice(
                    "Selecciona un módulo",
                    len(self.controller.get_modules()) + 1
                )
                
                if choice == len(self.controller.get_modules()) + 1:
                    console.print("👋 ¡Hasta luego!", style="bold yellow")
                    break
                
                module_names = list(self.controller.get_modules().keys())
                selected_module = module_names[choice - 1]
                
                if not self.controller.get_modules()[selected_module]:
                    self.ui.info(f"{selected_module} está en desarrollo")
                    self.ui.wait()
                    continue
                
                self._handle_submodules(selected_module)

        except KeyboardInterrupt:
            console.print("\n👋 ¡Hasta luego!", style="bold yellow")
        except Exception as e:
            logger.error(f"Error en aplicación: {str(e)}")
            self.ui.error(f"Error inesperado: {str(e)}")
            return False
        
        return True

    def _check_basic_env(self) -> bool:
        if not os.getenv('NEXUS_POSTGRES_URL'):
            self.ui.error("Falta NEXUS_POSTGRES_URL en las variables de entorno")
            return False
        return True

    def _show_main_menu(self):
        self.ui.show_banner()
        table = self.ui.show_modules(self.controller.get_modules())
        console.print(table)
        console.print()

    def _handle_submodules(self, module_name: str):
        while True:
            self.ui.show_banner()
            
            submodules = self.controller.get_modules()[module_name]
            table = self.ui.show_submodules(module_name, submodules)
            console.print(table)
            console.print()
            
            choice = self.ui.get_choice(
                "Selecciona un submódulo",
                len(submodules) + 1
            )
            
            if choice == len(submodules) + 1:
                break
            
            submodule_names = list(submodules.keys())
            selected_submodule = submodule_names[choice - 1]
            
            # Ejecutar migración
            if self._execute_migration(module_name, selected_submodule):
                if not self.ui.confirm("¿Realizar otra migración?"):
                    sys.exit(0)
            else:
                self.ui.wait()

    def _execute_migration(self, module_name: str, submodule_name: str) -> bool:
        try:
            missing_vars = self.controller.check_env_vars(module_name, submodule_name)
            if missing_vars:
                self.ui.error(f"Variables faltantes: {', '.join(missing_vars)}")
                return False

            if not self.ui.confirm(
                f"¿Migrar {module_name} -> {submodule_name}?\n"
                "⚠️ Esto eliminará datos existentes en el destino"
            ):
                console.print("❌ Migración cancelada", style="yellow")
                return False

            console.print(f"\n🚀 Ejecutando: {module_name} -> {submodule_name}", style="bold blue")
            
            success = self.controller.execute_migration(module_name, submodule_name)
            
            if success:
                self.ui.success(f"Migración {submodule_name} completada exitosamente")
            else:
                self.ui.error(f"Migración {submodule_name} falló. Revisa los logs.")
            
            return success

        except Exception as e:
            logger.error(f"Error en migración: {str(e)}")
            self.ui.error(f"Error durante migración: {str(e)}")
            return False

def main():
    try:
        app = MigrationApp()
        success = app.run()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        console.print("\n👋 ¡Hasta luego!", style="bold yellow")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error en aplicación: {str(e)}")
        console.print(f"💥 Error: {str(e)}", style="bold red")
        sys.exit(1)

if __name__ == "__main__":
    main()
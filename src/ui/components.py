import os
from typing import List, Dict, Callable
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt
from rich.text import Text
from rich.align import Align
from rich.columns import Columns
import pyfiglet
from src.utils.migration_reports import get_migration_status_indicator

# Create console with proper encoding handling
console = Console(force_terminal=True, width=120)

class UIComponents:
    
    @staticmethod
    def clear_screen():
        os.system('cls' if os.name == 'nt' else 'clear')
    
    @staticmethod
    def show_banner():
        UIComponents.clear_screen()
        
        banner_text = pyfiglet.figlet_format("NEXUS", font="slant")
        banner = Text(banner_text, style="bold bright_cyan")
        console.print(Align.center(banner))
        
        subtitle = Text(">>> Migration Tool", style="bold yellow")
        console.print(Align.center(subtitle))
        console.print()
    
    @staticmethod
    def show_modules(modules: Dict[str, Dict[str, Callable]]) -> Table:
        table = Table(
            show_header=True, 
            header_style="bold magenta",
            border_style="cyan",
            expand=True
        )
        
        table.add_column("", style="bold bright_white", width=3, justify="center")
        table.add_column("Modulo", style="bold blue", width=20)
        table.add_column("Submodulos", style="cyan")
        
        for i, (module_name, submodules) in enumerate(modules.items(), 1):
            if submodules:
                submodule_list = " | ".join(submodules.keys())
            else:
                submodule_list = "[dim]En desarrollo[/]"
            
            table.add_row(
                str(i),
                module_name,
                submodule_list
            )
        
        table.add_row(str(len(modules) + 1), "[ ] Salir", "")
        return table
    
    @staticmethod
    def show_submodules(module_name: str, submodules: Dict[str, Callable]) -> Table:
        table = Table(
            title=f"{module_name.upper()}", 
            show_header=True, 
            header_style="bold magenta",
            border_style="cyan",
            expand=True
        )
        
        table.add_column("", style="bold bright_white", width=3, justify="center")
        table.add_column("Submodulo", style="bold cyan", width=25)
        table.add_column("Estado", style="bold green", width=8, justify="center")
        
        for i, submodule_name in enumerate(submodules.keys(), 1):
            # Get migration status indicator
            status_indicator = get_migration_status_indicator(submodule_name.replace('-', '_'))
            status_text = status_indicator if status_indicator else "[ ]"
            
            table.add_row(str(i), submodule_name, status_text)
        
        table.add_row(str(len(submodules) + 1), "<- Volver", "")
        return table
    
    @staticmethod
    def get_choice(prompt: str, max_value: int) -> int:
        while True:
            try:
                choice = IntPrompt.ask(f"🎯 {prompt}", default=1)
                if 1 <= choice <= max_value:
                    return choice
                console.print(f"❌ Entre 1 y {max_value}", style="red")
            except KeyboardInterrupt:
                console.print("\n👋 ¡Hasta luego!", style="bold yellow")
                exit(0)
    
    @staticmethod
    def confirm(message: str) -> bool:
        return Confirm.ask(f"❓ {message}", default=False)
    
    @staticmethod
    def success(message: str):
        panel = Panel(
            Text(message, justify="center"),
            title="✅ Éxito", 
            border_style="green",
            padding=(0, 1)
        )
        console.print(panel)
    
    @staticmethod
    def error(message: str):
        panel = Panel(
            Text(message, justify="center"),
            title="❌ Error", 
            border_style="red",
            padding=(0, 1)
        )
        console.print(panel)
    
    @staticmethod
    def info(message: str):
        panel = Panel(
            Text(message, justify="center"),
            title="ℹ️ Info", 
            border_style="blue",
            padding=(0, 1)
        )
        console.print(panel)
    
    @staticmethod
    def wait():
        console.input("\n🔄 Enter para continuar...")
    
    @staticmethod
    def show_status_legend():
        """Show the meaning of status indicators"""
        legend_table = Table(
            title="Leyenda de Estados", 
            show_header=True,
            header_style="bold cyan",
            border_style="blue",
            width=50
        )
        legend_table.add_column("Simbolo", style="bold", width=8, justify="center")
        legend_table.add_column("Significado", style="white")
        
        legend_table.add_row("[+]", "Migracion completada exitosamente")
        legend_table.add_row("[X]", "Migracion fallo")
        legend_table.add_row("[!]", "Reporte existe pero no se puede leer")
        legend_table.add_row("[ ]", "No se ha ejecutado")
        
        console.print(legend_table)
        console.print()
        
    @staticmethod
    def show_env_status(env_vars: List[str]):
        table = Table(
            title="🔧 Variables de Entorno", 
            show_header=True,
            border_style="cyan"
        )
        table.add_column("Variable", style="bold blue")
        table.add_column("", style="bold", justify="center", width=3)
        
        for var in env_vars:
            status = "✅" if os.getenv(var) else "❌"
            table.add_row(var, status)
        
        console.print(table)
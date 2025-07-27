#!/usr/bin/env python3
"""
Script de configuración del proyecto
"""
import os
import sys

def create_directories():
    """Crea los directorios necesarios si no existen"""
    directories = [
        'src',
        'src/config',
        'src/connections',
        'src/utils',
        'src/extractors',
        'src/transformers',
        'src/loaders',
        'src/validators',
        'src/core'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ Directorio creado: {directory}")
        else:
            print(f"📁 Directorio existe: {directory}")

def create_init_files():
    """Crea los archivos __init__.py si no existen"""
    init_files = [
        'src/__init__.py',
        'src/config/__init__.py',
        'src/connections/__init__.py',
        'src/utils/__init__.py',
        'src/extractors/__init__.py',
        'src/transformers/__init__.py',
        'src/loaders/__init__.py',
        'src/validators/__init__.py',
        'src/core/__init__.py'
    ]
    
    for init_file in init_files:
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write('"""Package init file"""\n')
            print(f"✅ Archivo creado: {init_file}")
        else:
            print(f"📄 Archivo existe: {init_file}")

def main():
    """Función principal de configuración"""
    print("🚀 Configurando estructura del proyecto...")
    
    create_directories()
    create_init_files()
    
    print("\n✅ Configuración completada!")
    print("\n📋 Próximos pasos:")
    print("1. Copia los archivos Python a sus respectivos directorios")
    print("2. Configura tu archivo .env")
    print("3. Ejecuta: python app.py")

if __name__ == "__main__":
    main()
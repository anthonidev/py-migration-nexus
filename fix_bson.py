#!/usr/bin/env python3
"""
Script para resolver conflictos de BSON
"""
import subprocess
import sys

def run_command(command):
    """Ejecuta un comando y muestra el resultado"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"🔄 Ejecutando: {command}")
        if result.stdout:
            print(f"✅ Salida: {result.stdout.strip()}")
        if result.stderr and result.returncode != 0:
            print(f"⚠️  Error: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error ejecutando comando: {e}")
        return False

def main():
    """Función principal para resolver conflictos de BSON"""
    print("🛠️  RESOLVIENDO CONFLICTOS DE BSON")
    print("=" * 50)
    
    # 1. Desinstalar paquete bson conflictivo
    print("\n1. Desinstalando paquete 'bson' conflictivo...")
    run_command("pip uninstall -y bson")
    
    # 2. Verificar que pymongo esté instalado
    print("\n2. Verificando/instalando PyMongo...")
    run_command("pip install --upgrade pymongo")
    
    # 3. Verificar instalación
    print("\n3. Verificando que la instalación sea correcta...")
    try:
        from bson import ObjectId
        from pymongo import MongoClient
        print("✅ Verificación exitosa: bson y pymongo funcionan correctamente")
        
        # Probar creación de ObjectId
        test_id = ObjectId()
        print(f"✅ Test ObjectId: {test_id}")
        
    except ImportError as e:
        print(f"❌ Error de importación: {e}")
        print("\n💡 Intenta reinstalar PyMongo:")
        print("   pip uninstall pymongo")
        print("   pip install pymongo")
        return False
    
    print("\n🎉 ¡Conflictos de BSON resueltos!")
    print("\n📋 Próximos pasos:")
    print("   1. Ejecuta: python run_migration.py")
    print("   2. Si sigues teniendo problemas, reinicia tu entorno virtual")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
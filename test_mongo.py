#!/usr/bin/env python3
"""
Script corregido para probar Railway MongoDB
"""
import os
import sys
from urllib.parse import urlparse

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import pymongo
    from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
except ImportError:
    print("❌ PyMongo no está instalado")
    sys.exit(1)

def test_with_database():
    """Prueba la conexión con diferentes bases de datos"""
    base_url = "mongodb://mongo:UIeaObTCMScCtUhWYSeCKycUluzXVghl@yamabiko.proxy.rlwy.net:46924"
    
    # Bases de datos a probar
    databases_to_test = ['test', 'nexus_users', 'admin']
    
    print(f"🧪 PROBANDO CONEXIÓN CON DIFERENTES BASES DE DATOS:")
    print("=" * 55)
    
    working_urls = []
    
    for db_name in databases_to_test:
        url = f"{base_url}/{db_name}"
        print(f"\n🔄 Probando: {db_name}")
        
        try:
            client = pymongo.MongoClient(url, serverSelectionTimeoutMS=5000)
            
            # Hacer ping
            client.admin.command('ping')
            print(f"   ✅ Ping exitoso")
            
            # Probar acceso a la base específica
            db = client[db_name]
            collections = db.list_collection_names()
            print(f"   📊 Colecciones existentes: {collections}")
            
            # Probar operación de escritura/lectura
            test_collection = db['migration_test']
            
            # Insertar documento de prueba
            result = test_collection.insert_one({'test': 'migration', 'db': db_name})
            print(f"   ✅ Inserción exitosa: {result.inserted_id}")
            
            # Leer documento
            doc = test_collection.find_one({'test': 'migration'})
            print(f"   ✅ Lectura exitosa: {doc['db']}")
            
            # Limpiar
            test_collection.delete_one({'_id': result.inserted_id})
            print(f"   ✅ Limpieza exitosa")
            
            client.close()
            working_urls.append(url)
            
            print(f"   🎉 ¡{db_name} FUNCIONA PERFECTAMENTE!")
            
        except Exception as e:
            print(f"   ❌ Error con {db_name}: {e}")
    
    return working_urls

def test_migration_with_working_url(url):
    """Simula el proceso de migración con una URL que funciona"""
    print(f"\n🚀 SIMULANDO MIGRACIÓN CON URL FUNCIONAL:")
    print("=" * 45)
    print(f"URL: {url}")
    
    try:
        client = pymongo.MongoClient(url, serverSelectionTimeoutMS=5000)
        
        # Obtener nombre de base de datos
        db_name = url.split('/')[-1]
        db = client[db_name]
        
        print(f"📊 Base de datos: {db_name}")
        
        # Simular inserción de roles y vistas
        print(f"\n1. 📥 Simulando carga de vistas...")
        views_collection = db['views']
        
        # Limpiar colección existente
        views_collection.delete_many({})
        
        # Insertar vistas de prueba
        sample_views = [
            {
                'code': 'DASHBOARD',
                'name': 'Dashboard Principal',
                'isActive': True,
                'order': 1,
                'roles': [],
                'parent': None,
                'children': []
            },
            {
                'code': 'USERS',
                'name': 'Gestión de Usuarios',
                'isActive': True,
                'order': 2,
                'roles': [],
                'parent': None,
                'children': []
            }
        ]
        
        views_result = views_collection.insert_many(sample_views)
        print(f"   ✅ {len(views_result.inserted_ids)} vistas insertadas")
        
        print(f"\n2. 📥 Simulando carga de roles...")
        roles_collection = db['roles']
        
        # Limpiar colección existente
        roles_collection.delete_many({})
        
        # Insertar roles de prueba
        sample_roles = [
            {
                'code': 'ADMIN',
                'name': 'Administrador',
                'isActive': True,
                'views': views_result.inserted_ids
            },
            {
                'code': 'USER',
                'name': 'Usuario',
                'isActive': True,
                'views': [views_result.inserted_ids[0]]  # Solo dashboard
            }
        ]
        
        roles_result = roles_collection.insert_many(sample_roles)
        print(f"   ✅ {len(roles_result.inserted_ids)} roles insertados")
        
        print(f"\n3. 🔍 Creando índices...")
        # Crear índices
        views_collection.create_index('code', unique=True)
        roles_collection.create_index('code', unique=True)
        print(f"   ✅ Índices creados")
        
        print(f"\n4. ✅ Validando datos...")
        views_count = views_collection.count_documents({})
        roles_count = roles_collection.count_documents({})
        
        print(f"   📊 Vistas en DB: {views_count}")
        print(f"   📊 Roles en DB: {roles_count}")
        
        # Mostrar muestra de datos
        print(f"\n📋 MUESTRA DE DATOS INSERTADOS:")
        print(f"Roles:")
        for role in roles_collection.find():
            print(f"   - {role['code']}: {role['name']} ({len(role['views'])} vistas)")
        
        print(f"Vistas:")
        for view in views_collection.find():
            print(f"   - {view['code']}: {view['name']}")
        
        client.close()
        
        print(f"\n🎉 ¡SIMULACIÓN DE MIGRACIÓN EXITOSA!")
        return True
        
    except Exception as e:
        print(f"❌ Error en simulación: {e}")
        return False

def main():
    print("🔧 TESTER CORREGIDO RAILWAY MONGODB")
    print("=" * 40)
    
    # Probar diferentes bases de datos
    working_urls = test_with_database()
    
    if working_urls:
        print(f"\n✅ URLs QUE FUNCIONAN:")
        for i, url in enumerate(working_urls, 1):
            print(f"   {i}. {url}")
        
        # Usar la primera URL que funcione para simular migración
        test_migration_with_working_url(working_urls[0])
        
        print(f"\n📋 PARA USAR EN TU .env:")
        print("=" * 25)
        print(f"MS_NEXUS_USER={working_urls[0]}")
        
        print(f"\n🚀 EJECUTAR MIGRACIÓN REAL:")
        print("=" * 30)
        print("1. Actualiza tu archivo .env con la URL que funciona")
        print("2. Ejecuta: python run_migration.py")
        print("3. ¡Disfruta de tu migración exitosa!")
        
    else:
        print(f"\n❌ Ninguna configuración funcionó")
        print("Verifica las credenciales en Railway")

if __name__ == "__main__":
    main()
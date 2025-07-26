# Migración de Datos: Monolito a Microservicios

Este proyecto maneja la migración de datos desde un monolito PostgreSQL hacia microservicios con bases de datos especializadas (MongoDB, PostgreSQL).

## 📁 Estructura del Proyecto

```
migration_project/
├── dags/                           # DAGs de Airflow
│   └── roles_views_migration_dag.py
├── src/                            # Código fuente modular
│   ├── config/                     # Configuraciones
│   │   └── database_config.py
│   ├── connections/                # Gestión de conexiones DB
│   │   ├── postgres_connection.py
│   │   └── mongo_connection.py
│   ├── extractors/                 # Extracción de datos
│   │   └── roles_views_extractor.py
│   ├── transformers/               # Transformación de datos
│   │   └── roles_views_transformer.py
│   ├── loaders/                    # Carga de datos
│   │   └── mongo_loader.py
│   ├── validators/                 # Validación de migración
│   │   └── migration_validator.py
│   └── utils/                      # Utilidades
│       └── logger.py
├── requirements.txt                # Dependencias Python
├── .env.example                   # Variables de entorno ejemplo
└── README.md                      # Este archivo
```

## 🚀 Instalación y Configuración

### 1. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno

Copia el archivo `.env.example` a `.env` y configura las conexiones:

```bash
cp .env.example .env
```

Edita el archivo `.env`:

```bash
# Base de datos PostgreSQL origen (monolito)
NEXUS_POSTGRES_URL=postgresql://usuario:password@host:5432/database_name

# Base de datos MongoDB destino (microservicio de usuarios)
MS_NEXUS_USER=mongodb://usuario:password@host:27017/ms_nexus_user
```

### 3. Configurar Airflow 3.x

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar Airflow para usar SQLite (desarrollo) o PostgreSQL (producción)
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=~/migration_project/dags

# Inicializar base de datos de Airflow
airflow db migrate

# Crear usuario admin
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Iniciar servicios
airflow webserver --port 8080 &
airflow scheduler &
```

**Nota para Airflow 3.x**: Los comandos han cambiado:

- `airflow db init` → `airflow db migrate`
- Algunos operadores se movieron a providers

## 📊 Uso del DAG de Migración

### Migración de Roles y Vistas

El DAG `roles_views_migration` realiza los siguientes pasos:

1. **extract_data**: Extrae roles y vistas desde PostgreSQL
2. **transform_data**: Convierte datos al formato MongoDB
3. **load_views_to_mongo**: Carga vistas en MongoDB
4. **load_roles_to_mongo**: Carga roles en MongoDB
5. **create_indexes**: Crea índices optimizados
6. **validate_migration**: Valida integridad de datos
7. **send_notification**: Envía notificación de finalización

### Ejecutar la Migración

1. Accede a la interfaz web de Airflow: `http://localhost:8080`
2. Busca el DAG `roles_views_migration`
3. Actívalo y ejecuta manualmente
4. Monitorea el progreso en tiempo real

## 🔧 Uso Programático

También puedes usar los módulos directamente:

```python
from src.extractors.roles_views_extractor import RolesViewsExtractor
from src.transformers.roles_views_transformer import RolesViewsTransformer
from src.loaders.mongo_loader import MongoLoader
from src.validators.migration_validator import MigrationValidator

# Extraer datos
extractor = RolesViewsExtractor()
roles_data = extractor.extract_roles_and_views()
views_data = extractor.extract_all_views()

# Transformar datos
transformer = RolesViewsTransformer()
transformed_views, view_mapping = transformer.transform_views_data(views_data)
transformed_roles, role_mapping = transformer.transform_roles_data(roles_data, view_mapping)

# Cargar datos
loader = MongoLoader()
loader.load_views(transformed_views)
loader.load_roles(transformed_roles)

# Validar migración
validator = MigrationValidator()
report = validator.generate_migration_report()
```

## 📈 Monitoreo y Validación

### Validaciones Automáticas

- **Conteos**: Verifica que la cantidad de registros coincida
- **Estructura**: Valida campos requeridos y formatos
- **Relaciones**: Verifica integridad referencial
- **Índices**: Confirma creación correcta de índices

### Logs Detallados

Los logs incluyen:

- Progreso de cada fase
- Estadísticas de migración
- Errores y advertencias
- Tiempo de ejecución por tarea
- Validaciones de integridad

### Reporte de Migración

Al finalizar, se genera un reporte completo con:

- Conteos de registros migrados
- Validaciones de integridad
- Estadísticas de rendimiento
- Lista de errores/advertencias

## 🧪 Testing

### Validación Manual

```bash
# Verificar conteos en PostgreSQL
psql -c "SELECT COUNT(*) FROM roles;" $NEXUS_POSTGRES_URL
psql -c "SELECT COUNT(*) FROM views;" $NEXUS_POSTGRES_URL

# Verificar conteos en MongoDB
mongo $MS_NEXUS_USER --eval "db.roles.count()"
mongo $MS_NEXUS_USER --eval "db.views.count()"
```

### Ejecutar Validaciones

```python
from src.validators.migration_validator import MigrationValidator

validator = MigrationValidator()

# Validar solo conteos
counts_result = validator.validate_counts()
print(f"Validación exitosa: {counts_result['success']}")

# Validar integridad completa
integrity_result = validator.validate_data_integrity()
print(f"Errores encontrados: {len(integrity_result['errors'])}")

# Generar reporte completo
report = validator.generate_migration_report()
```

## 🔄 Extending para Otros Módulos

### Crear Nuevo Extractor

```python
# src/extractors/products_extractor.py
from src.connections.postgres_connection import PostgresConnection

class ProductsExtractor:
    def __init__(self):
        self.pg_connection = PostgresConnection()

    def extract_products(self):
        # Tu lógica de extracción
        pass
```

### Crear Nuevo Transformador

```python
# src/transformers/products_transformer.py
class ProductsTransformer:
    def transform_products_data(self, products_data):
        # Tu lógica de transformación
        pass
```

### Crear Nuevo DAG

```python
# dags/products_migration_dag.py
from src.extractors.products_extractor import ProductsExtractor
from src.transformers.products_transformer import ProductsTransformer
# ... resto del DAG
```

## 📚 Esquemas de MongoDB

### Colección: roles

```javascript
{
  _id: ObjectId,
  code: String,        // UPPERCASE
  name: String,
  isActive: Boolean,
  views: [ObjectId],   // Referencias a views
  createdAt: Date,
  updatedAt: Date
}
```

### Colección: views

```javascript
{
  _id: ObjectId,
  code: String,        // UPPERCASE
  name: String,
  icon: String,
  url: String,
  isActive: Boolean,
  order: Number,
  metadata: Object,
  parent: ObjectId,    // Referencia a vista padre
  children: [ObjectId], // Referencias a vistas hijas
  roles: [ObjectId],   // Referencias a roles
  createdAt: Date,
  updatedAt: Date
}
```

## ⚠️ Consideraciones Importantes

### Antes de la Migración

1. **Backup**: Haz backup completo de las bases de datos
2. **Pruebas**: Ejecuta la migración en un entorno de desarrollo
3. **Validación**: Verifica que las consultas funcionen correctamente
4. **Downtime**: Planifica ventana de mantenimiento si es necesario

### Durante la Migración

1. **Monitoreo**: Supervisa los logs de Airflow continuamente
2. **Recursos**: Asegúrate de tener suficiente memoria y CPU
3. **Conectividad**: Verifica estabilidad de conexiones de red
4. **Rollback**: Ten plan de rollback preparado

### Después de la Migración

1. **Validación**: Ejecuta todas las validaciones
2. **Testing**: Prueba funcionalidad de las aplicaciones
3. **Performance**: Monitorea rendimiento de consultas
4. **Limpieza**: Limpia datos temporales si es necesario

## 🐛 Troubleshooting

### Errores Comunes

**Error de Conexión PostgreSQL**

```bash
# Verificar conectividad
psql $NEXUS_POSTGRES_URL -c "SELECT 1;"
```

**Error de Conexión MongoDB**

```bash
# Verificar conectividad
mongo $MS_NEXUS_USER --eval "db.runCommand('ping')"
```

**Falta de Memoria**

```python
# Ajustar tamaño de lotes en extractors
batch_size = 500  # Reducir si hay problemas de memoria
```

**Problemas de Permisos**

```bash
# Verificar permisos de usuario en bases de datos
# PostgreSQL: GRANT SELECT ON ALL TABLES...
# MongoDB: db.grantRolesToUser(...)
```

### Logs de Debugging

```python
# Habilitar logging debug
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contribución

### Estructura de Commits

```bash
git commit -m "feat(extractors): add products extractor"
git commit -m "fix(validators): handle null metadata fields"
git commit -m "docs(readme): update installation instructions"
```

### Añadir Nuevos Módulos

1. Crea el archivo en la carpeta correspondiente
2. Añade imports en `__init__.py`
3. Escribe tests unitarios
4. Actualiza documentación
5. Crea DAG correspondiente

## 📄 Licencia

Este proyecto es para uso interno de la organización.

## 📞 Soporte

Para soporte técnico:

- Revisar logs de Airflow
- Consultar troubleshooting en este README
- Contactar al equipo de Data Engineering

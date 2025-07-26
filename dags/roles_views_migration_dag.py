import sys
import os
from datetime import datetime, timedelta

# Añadir el directorio src al path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow import DAG

# Imports compatibles con Airflow 3.x
try:
    # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    try:
        # Fallback para versiones anteriores
        from airflow.operators import PythonOperator
    except ImportError:
        # Último fallback
        from airflow.operators import PythonOperator

from src.extractors.roles_views_extractor import RolesViewsExtractor
from src.transformers.roles_views_transformer import RolesViewsTransformer
from src.loaders.mongo_loader import MongoLoader
from src.validators.migration_validator import MigrationValidator
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Configuración del DAG
default_args = {
    'owner': 'data-migration-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'roles_views_migration_v3',
    default_args=default_args,
    description='Migración de roles y vistas de PostgreSQL a MongoDB (Airflow 3.x)',
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=['migration', 'roles', 'views', 'postgres-to-mongo', 'airflow-3x']
)

def extract_data(**context):
    """Tarea para extraer datos de PostgreSQL"""
    try:
        logger.info("=== INICIANDO EXTRACCIÓN DE DATOS ===")
        
        extractor = RolesViewsExtractor()
        
        # Extraer datos
        roles_data = extractor.extract_roles_and_views()
        views_data = extractor.extract_all_views()
        
        # Guardar en XCom para las siguientes tareas - Airflow 3.x style
        task_instance = context['task_instance']
        task_instance.xcom_push(key='roles_data', value=roles_data)
        task_instance.xcom_push(key='views_data', value=views_data)
        
        logger.info(f"✅ Extracción completada - Roles: {len(roles_data)}, Vistas: {len(views_data)}")
        
    except Exception as e:
        logger.error(f"❌ Error en extracción: {str(e)}")
        raise

def transform_data(**context):
    """Tarea para transformar datos"""
    try:
        logger.info("=== INICIANDO TRANSFORMACIÓN DE DATOS ===")
        
        # Obtener datos de XCom - Airflow 3.x style
        task_instance = context['task_instance']
        roles_data = task_instance.xcom_pull(key='roles_data', task_ids='extract_data')
        views_data = task_instance.xcom_pull(key='views_data', task_ids='extract_data')
        
        transformer = RolesViewsTransformer()
        
        # Transformar datos
        transformed_views, view_id_mapping = transformer.transform_views_data(views_data)
        transformed_roles, role_id_mapping = transformer.transform_roles_data(roles_data, view_id_mapping)
        
        # Actualizar vistas con roles
        transformer.update_views_with_roles(transformed_views, roles_data, view_id_mapping, role_id_mapping)
        
        # Guardar en XCom
        task_instance.xcom_push(key='transformed_views', value=transformed_views)
        task_instance.xcom_push(key='transformed_roles', value=transformed_roles)
        task_instance.xcom_push(key='view_id_mapping', value=view_id_mapping)
        task_instance.xcom_push(key='role_id_mapping', value=role_id_mapping)
        
        # Log del resumen de transformación
        summary = transformer.get_transformation_summary()
        logger.info(f"✅ Transformación completada - {summary}")
        
    except Exception as e:
        logger.error(f"❌ Error en transformación: {str(e)}")
        raise

def load_views_to_mongo(**context):
    """Tarea para cargar vistas en MongoDB"""
    try:
        logger.info("=== INICIANDO CARGA DE VISTAS ===")
        
        task_instance = context['task_instance']
        transformed_views = task_instance.xcom_pull(key='transformed_views', task_ids='transform_data')
        
        loader = MongoLoader()
        result = loader.load_views(transformed_views, clear_existing=True)
        
        logger.info(f"✅ Vistas cargadas - Insertadas: {result['inserted_count']}, Eliminadas: {result['deleted_count']}")
        
    except Exception as e:
        logger.error(f"❌ Error cargando vistas: {str(e)}")
        raise

def load_roles_to_mongo(**context):
    """Tarea para cargar roles en MongoDB"""
    try:
        logger.info("=== INICIANDO CARGA DE ROLES ===")
        
        task_instance = context['task_instance']
        transformed_roles = task_instance.xcom_pull(key='transformed_roles', task_ids='transform_data')
        
        loader = MongoLoader()
        result = loader.load_roles(transformed_roles, clear_existing=True)
        
        logger.info(f"✅ Roles cargados - Insertados: {result['inserted_count']}, Eliminados: {result['deleted_count']}")
        
    except Exception as e:
        logger.error(f"❌ Error cargando roles: {str(e)}")
        raise

def create_indexes(**context):
    """Tarea para crear índices en MongoDB"""
    try:
        logger.info("=== CREANDO ÍNDICES ===")
        
        loader = MongoLoader()
        loader.create_indexes()
        
        logger.info("✅ Índices creados exitosamente")
        
    except Exception as e:
        logger.error(f"❌ Error creando índices: {str(e)}")
        raise

def validate_migration(**context):
    """Tarea para validar la migración"""
    try:
        logger.info("=== INICIANDO VALIDACIÓN ===")
        
        validator = MigrationValidator()
        
        # Validar conteos
        counts_validation = validator.validate_counts()
        
        # Validar integridad de datos
        integrity_validation = validator.validate_data_integrity()
        
        # Generar reporte completo
        report = validator.generate_migration_report()
        
        # Guardar reporte en XCom
        task_instance = context['task_instance']
        task_instance.xcom_push(key='migration_report', value=report)
        
        # Verificar si la migración fue exitosa
        if not report['summary']['overall_success']:
            error_msg = f"❌ Migración falló - Errores: {report['summary']['total_errors']}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"✅ Validación exitosa - Migración completada correctamente")
        logger.info(f"📊 Resumen: Roles={counts_validation['mongo_counts']['roles']}, Vistas={counts_validation['mongo_counts']['views']}")
        
    except Exception as e:
        logger.error(f"❌ Error en validación: {str(e)}")
        raise

def send_notification(**context):
    """Tarea para enviar notificación de finalización"""
    try:
        logger.info("=== ENVIANDO NOTIFICACIÓN ===")
        
        # Obtener reporte de la validación
        task_instance = context['task_instance']
        report = task_instance.xcom_pull(key='migration_report', task_ids='validate_migration')
        
        # En un entorno real, aquí enviarías emails, Slack, etc.
        logger.info("📧 Notificación enviada - Migración de roles y vistas completada exitosamente")
        logger.info(f"📈 Estadísticas finales: {report['summary']}")
        
    except Exception as e:
        logger.error(f"❌ Error enviando notificación: {str(e)}")
        # No fallar el DAG por problemas de notificación
        pass

# Definir tareas del DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_views_task = PythonOperator(
    task_id='load_views_to_mongo',
    python_callable=load_views_to_mongo,
    dag=dag
)

load_roles_task = PythonOperator(
    task_id='load_roles_to_mongo',
    python_callable=load_roles_to_mongo,
    dag=dag
)

create_indexes_task = PythonOperator(
    task_id='create_indexes',
    python_callable=create_indexes,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_migration',
    python_callable=validate_migration,
    dag=dag
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag,
    trigger_rule='all_done'  # Se ejecuta aunque fallen tareas anteriores
)

# Definir dependencias
extract_task >> transform_task >> [load_views_task, load_roles_task] >> create_indexes_task >> validate_task >> notify_task
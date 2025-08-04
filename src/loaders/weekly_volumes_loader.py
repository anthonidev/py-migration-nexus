from typing import List, Dict, Any
from src.connections.points_postgres_connection import PointsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class WeeklyVolumesLoader:

    def __init__(self):
        self.postgres_conn = PointsPostgresConnection()

    def _check_tables_exist(self):
        """Verifica que las tablas necesarias existan"""
        tables_to_check = ['weekly_volumes', 'weekly_volume_history']
        
        for table in tables_to_check:
            check_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table}'
            );
            """
            result, _ = self.postgres_conn.execute_query(check_query)
            if not result[0][0]:
                raise RuntimeError(f"Tabla '{table}' no existe. Debe ser creada por las migraciones del microservicio.")

    def clear_existing_data(self) -> Dict[str, int]:
        """Elimina todos los datos existentes en las tablas de volúmenes semanales"""
        logger.info("Eliminando datos de volúmenes semanales existentes")

        try:
            # Eliminar en orden por dependencias FK
            delete_history_query = "DELETE FROM weekly_volume_history"
            history_deleted, _ = self.postgres_conn.execute_query(delete_history_query)

            delete_volumes_query = "DELETE FROM weekly_volumes"
            volumes_deleted, _ = self.postgres_conn.execute_query(delete_volumes_query)

            # Resetear solo la secuencia de weekly_volumes (history usa autoincremental normal)
            self.postgres_conn.execute_query("SELECT setval('weekly_volumes_id_seq', COALESCE((SELECT MAX(id) FROM weekly_volumes), 0) + 1, false);")

            logger.info(f"Eliminados {volumes_deleted} volúmenes semanales, {history_deleted} history")

            return {
                'weekly_volumes_deleted': volumes_deleted,
                'history_deleted': history_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            raise

    def load_weekly_volumes(self, volumes_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """Carga los volúmenes semanales en la base de datos"""
        logger.info(f"Iniciando carga de {len(volumes_data)} volúmenes semanales en PostgreSQL")

        try:
            self._check_tables_exist()
            
            deleted_count = 0
            if clear_existing:
                clear_result = self.clear_existing_data()
                deleted_count = clear_result['weekly_volumes_deleted']

            if not volumes_data:
                logger.warning("No hay volúmenes semanales para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': deleted_count
                }

            inserted_count = self._insert_weekly_volumes_with_original_ids(volumes_data)
            logger.info(f"Insertados {inserted_count} volúmenes semanales exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': deleted_count
            }

        except Exception as e:
            error_msg = f"Error cargando volúmenes semanales: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': deleted_count if 'deleted_count' in locals() else 0,
                'error': str(e)
            }

    def load_volume_history(self, history_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga el historial de volúmenes en la base de datos"""
        logger.info(f"Iniciando carga de {len(history_data)} registros de historial en PostgreSQL")

        try:
            if not history_data:
                logger.warning("No hay registros de historial para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_volume_history_with_original_ids(history_data)
            logger.info(f"Insertados {inserted_count} registros de historial exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando historial: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def _insert_weekly_volumes_with_original_ids(self, volumes_data: List[Dict[str, Any]]) -> int:
        """Inserta volúmenes semanales conservando los IDs originales"""
        insert_query = """
        INSERT INTO weekly_volumes (
            id, user_id, user_email, user_name, left_volume, right_volume,
            commission_earned, week_start_date, week_end_date, status,
            selected_side, processed_at, metadata, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for volume in volumes_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = volume['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                volume['id'],
                volume['user_id'],
                volume['user_email'],
                volume['user_name'],
                volume['left_volume'],
                volume['right_volume'],
                volume['commission_earned'],
                volume['week_start_date'],
                volume['week_end_date'],
                volume['status'],
                volume['selected_side'],
                volume['processed_at'],
                metadata_value,
                volume['created_at'],
                volume['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia para continuar desde el ID más alto
            max_id = max(volume['id'] for volume in volumes_data)
            self.postgres_conn.execute_query("SELECT setval('weekly_volumes_id_seq', %s, true);", (max_id,))
            logger.info(f"Secuencia de weekly_volumes actualizada para continuar desde ID {max_id}")
            
            return len(volumes_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de volúmenes semanales: {str(e)}")
            raise

    def _insert_volume_history_with_original_ids(self, history_data: List[Dict[str, Any]]) -> int:
        """Inserta historial de volúmenes con IDs autoincrementables"""
        insert_query = """
        INSERT INTO weekly_volume_history (
            weekly_volume_id, payment_id, volume_side, volume,
            metadata, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for history_item in history_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = history_item['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                history_item['weekly_volume_id'],
                history_item['payment_id'],
                history_item['volume_side'],
                history_item['volume'],
                metadata_value,
                history_item['created_at'],
                history_item['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)
            logger.info(f"Insertados {inserted_count} registros de historial con IDs autoincrementables")
            return len(history_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de historial: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos de volúmenes semanales en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            volumes_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM weekly_volumes")
            history_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM weekly_volume_history")

            total_volumes = volumes_count[0][0]
            total_history = history_count[0][0]

            validation_results['stats'] = {
                'total_weekly_volumes': total_volumes,
                'total_volume_history': total_history
            }

            # Validar campos obligatorios en volúmenes semanales
            missing_volume_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM weekly_volumes 
                WHERE user_email IS NULL OR user_email = '' 
                   OR week_start_date IS NULL 
                   OR week_end_date IS NULL
                   OR left_volume < 0 
                   OR right_volume < 0
            """)
            
            if missing_volume_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_volume_data[0][0]} volúmenes con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar fechas inconsistentes
            invalid_dates, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM weekly_volumes 
                WHERE week_end_date <= week_start_date
            """)
            
            if invalid_dates[0][0] > 0:
                validation_results['errors'].append(f"{invalid_dates[0][0]} volúmenes con fechas inconsistentes")
                validation_results['valid'] = False

            # Validar history huérfano
            orphan_history, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM weekly_volume_history wvh 
                LEFT JOIN weekly_volumes wv ON wvh.weekly_volume_id = wv.id 
                WHERE wv.id IS NULL
            """)
            
            if orphan_history[0][0] > 0:
                validation_results['errors'].append(f"{orphan_history[0][0]} registros de history huérfanos sin volumen asociado")
                validation_results['valid'] = False

            # Validar volúmenes negativos en history
            negative_history_volumes, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM weekly_volume_history 
                WHERE volume < 0
            """)
            
            if negative_history_volumes[0][0] > 0:
                validation_results['errors'].append(f"{negative_history_volumes[0][0]} registros de history con volumen negativo")
                validation_results['valid'] = False

            # Validar duplicados por usuario y semana
            duplicates, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) 
                FROM (
                    SELECT user_id, week_start_date, COUNT(*) as count
                    FROM weekly_volumes 
                    WHERE user_id IS NOT NULL
                    GROUP BY user_id, week_start_date 
                    HAVING COUNT(*) > 1
                ) duplicates
            """)
            
            if duplicates[0][0] > 0:
                validation_results['warnings'].append(f"{duplicates[0][0]} usuarios con volúmenes duplicados por semana")

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        self.postgres_conn.disconnect()
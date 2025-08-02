from typing import List, Dict, Any
from src.connections.membership_postgres_connection import MembershipPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MembershipsLoader:

    def __init__(self):
        self.postgres_conn = MembershipPostgresConnection()
        self.stats = {
            'memberships_inserted': 0,
            'reconsumptions_inserted': 0,
            'history_inserted': 0,
            'memberships_deleted': 0,
            'reconsumptions_deleted': 0,
            'history_deleted': 0,
            'errors': []
        }

    def _check_tables_exist(self):
        """Verifica que las tablas necesarias existan"""
        tables_to_check = ['memberships', 'membership_reconsumptions', 'membership_history']
        
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
        """Elimina todos los datos existentes en las tablas de membresías"""
        logger.info("Eliminando datos de membresías existentes")

        try:
            # Eliminar en orden por dependencias FK
            delete_history_query = "DELETE FROM membership_history"
            history_deleted, _ = self.postgres_conn.execute_query(delete_history_query)

            delete_reconsumptions_query = "DELETE FROM membership_reconsumptions"
            reconsumptions_deleted, _ = self.postgres_conn.execute_query(delete_reconsumptions_query)

            delete_memberships_query = "DELETE FROM memberships"
            memberships_deleted, _ = self.postgres_conn.execute_query(delete_memberships_query)

            # Resetear secuencias
            self.postgres_conn.execute_query("SELECT setval('memberships_id_seq', COALESCE((SELECT MAX(id) FROM memberships), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('membership_reconsumptions_id_seq', COALESCE((SELECT MAX(id) FROM membership_reconsumptions), 0) + 1, false);")
            self.postgres_conn.execute_query("SELECT setval('membership_history_id_seq', COALESCE((SELECT MAX(id) FROM membership_history), 0) + 1, false);")

            self.stats['memberships_deleted'] = memberships_deleted
            self.stats['reconsumptions_deleted'] = reconsumptions_deleted
            self.stats['history_deleted'] = history_deleted

            logger.info(f"Eliminados {memberships_deleted} membresías, {reconsumptions_deleted} reconsumptions, {history_deleted} history")

            return {
                'memberships_deleted': memberships_deleted,
                'reconsumptions_deleted': reconsumptions_deleted,
                'history_deleted': history_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_memberships(self, memberships_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """Carga las membresías en la base de datos"""
        logger.info(f"Iniciando carga de {len(memberships_data)} membresías en PostgreSQL")

        try:
            self._check_tables_exist()
            
            if clear_existing:
                self.clear_existing_data()

            if not memberships_data:
                logger.warning("No hay membresías para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['memberships_deleted']
                }

            inserted_count = self._insert_memberships_with_original_ids(memberships_data)
            self.stats['memberships_inserted'] = inserted_count
            logger.info(f"Insertadas {inserted_count} membresías exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['memberships_deleted']
            }

        except Exception as e:
            error_msg = f"Error cargando membresías: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['memberships_deleted'],
                'error': str(e)
            }

    def load_reconsumptions(self, reconsumptions_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga los reconsumptions en la base de datos"""
        logger.info(f"Iniciando carga de {len(reconsumptions_data)} reconsumptions en PostgreSQL")

        try:
            if not reconsumptions_data:
                logger.warning("No hay reconsumptions para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_reconsumptions_with_original_ids(reconsumptions_data)
            self.stats['reconsumptions_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} reconsumptions exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando reconsumptions: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def load_history(self, history_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carga el historial en la base de datos"""
        logger.info(f"Iniciando carga de {len(history_data)} registros de historial en PostgreSQL")

        try:
            if not history_data:
                logger.warning("No hay registros de historial para insertar")
                return {'success': True, 'inserted_count': 0}

            inserted_count = self._insert_history_with_original_ids(history_data)
            self.stats['history_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} registros de historial exitosamente")

            return {'success': True, 'inserted_count': inserted_count}

        except Exception as e:
            error_msg = f"Error cargando historial: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'success': False, 'inserted_count': 0, 'error': str(e)}

    def _insert_memberships_with_original_ids(self, memberships_data: List[Dict[str, Any]]) -> int:
        """Inserta membresías conservando los IDs originales"""
        insert_query = """
        INSERT INTO memberships (
            id, user_id, user_email, user_name, from_plan, from_plan_id,
            plan_id, start_date, end_date, status, minimum_reconsumption_amount,
            auto_renewal, metadata, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params_list = []
        for membership in memberships_data:
            # Convertir metadata a JSON string si es dict
            metadata_value = membership['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                membership['id'],
                membership['user_id'],
                membership['user_email'],
                membership['user_name'],
                membership['from_plan'],
                membership['from_plan_id'],
                membership['plan_id'],
                membership['start_date'],
                membership['end_date'],
                membership['status'],
                membership['minimum_reconsumption_amount'],
                membership['auto_renewal'],
                metadata_value,
                membership['created_at'],
                membership['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia para continuar desde el ID más alto
            max_id = max(membership['id'] for membership in memberships_data)
            self.postgres_conn.execute_query("SELECT setval('memberships_id_seq', %s, true);", (max_id,))
            logger.info(f"Secuencia de memberships actualizada para continuar desde ID {max_id}")
            
            return len(memberships_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de membresías: {str(e)}")
            raise

    def _insert_reconsumptions_with_original_ids(self, reconsumptions_data: List[Dict[str, Any]]) -> int:
        """Inserta reconsumptions conservando los IDs originales"""
        insert_query = """
        INSERT INTO membership_reconsumptions (
            id, membership_id, amount, status, period_date, payment_reference,
            payment_details, notes, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for reconsumption in reconsumptions_data:
            # Convertir payment_details a JSON string si es dict
            payment_details_value = reconsumption['payment_details']
            if isinstance(payment_details_value, dict):
                import json
                payment_details_value = json.dumps(payment_details_value)

            params = (
                reconsumption['id'],
                reconsumption['membership_id'],
                reconsumption['amount'],
                reconsumption['status'],
                reconsumption['period_date'],
                reconsumption['payment_reference'],
                payment_details_value,
                reconsumption['notes'],
                reconsumption['created_at'],
                reconsumption['updated_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if reconsumptions_data:
                max_id = max(reconsumption['id'] for reconsumption in reconsumptions_data)
                self.postgres_conn.execute_query("SELECT setval('membership_reconsumptions_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de reconsumptions actualizada para continuar desde ID {max_id}")

            return len(reconsumptions_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de reconsumptions: {str(e)}")
            raise

    def _insert_history_with_original_ids(self, history_data: List[Dict[str, Any]]) -> int:
        """Inserta historial conservando los IDs originales"""
        insert_query = """
        INSERT INTO membership_history (
            id, membership_id, action, changes, notes, metadata, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for history_item in history_data:
            # Convertir changes y metadata a JSON string si son dict
            changes_value = history_item['changes']
            if isinstance(changes_value, dict):
                import json
                changes_value = json.dumps(changes_value)

            metadata_value = history_item['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)

            params = (
                history_item['id'],
                history_item['membership_id'],
                history_item['action'],
                changes_value,
                history_item['notes'],
                metadata_value,
                history_item['created_at']
            )
            params_list.append(params)

        try:
            inserted_count = self.postgres_conn.execute_bulk_insert(insert_query, params_list)

            # Actualizar secuencia
            if history_data:
                max_id = max(history_item['id'] for history_item in history_data)
                self.postgres_conn.execute_query("SELECT setval('membership_history_id_seq', %s, true);", (max_id,))
                logger.info(f"Secuencia de history actualizada para continuar desde ID {max_id}")

            return len(history_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de historial: {str(e)}")
            raise

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos de membresías en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            memberships_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM memberships")
            reconsumptions_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM membership_reconsumptions")
            history_count, _ = self.postgres_conn.execute_query("SELECT COUNT(*) FROM membership_history")

            total_memberships = memberships_count[0][0]
            total_reconsumptions = reconsumptions_count[0][0]
            total_history = history_count[0][0]

            validation_results['stats'] = {
                'total_memberships': total_memberships,
                'total_reconsumptions': total_reconsumptions,
                'total_history': total_history
            }

            # Validar campos obligatorios en membresías
            missing_membership_data, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM memberships 
                WHERE user_email IS NULL OR user_email = '' 
                   OR plan_id IS NULL 
                   OR start_date IS NULL
                   OR minimum_reconsumption_amount < 0
            """)
            
            if missing_membership_data[0][0] > 0:
                validation_results['errors'].append(f"{missing_membership_data[0][0]} membresías con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar fechas inconsistentes
            invalid_dates, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM memberships 
                WHERE end_date IS NOT NULL AND end_date < start_date
            """)
            
            if invalid_dates[0][0] > 0:
                validation_results['errors'].append(f"{invalid_dates[0][0]} membresías con fechas inconsistentes")
                validation_results['valid'] = False

            # Validar reconsumptions huérfanos
            orphan_reconsumptions, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM membership_reconsumptions mr 
                LEFT JOIN memberships m ON mr.membership_id = m.id 
                WHERE m.id IS NULL
            """)
            
            if orphan_reconsumptions[0][0] > 0:
                validation_results['errors'].append(f"{orphan_reconsumptions[0][0]} reconsumptions huérfanos sin membresía asociada")
                validation_results['valid'] = False

            # Validar history huérfano
            orphan_history, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM membership_history mh 
                LEFT JOIN memberships m ON mh.membership_id = m.id 
                WHERE m.id IS NULL
            """)
            
            if orphan_history[0][0] > 0:
                validation_results['errors'].append(f"{orphan_history[0][0]} registros de history huérfanos sin membresía asociada")
                validation_results['valid'] = False

            # Validar montos negativos en reconsumptions
            negative_amounts, _ = self.postgres_conn.execute_query("""
                SELECT COUNT(*) FROM membership_reconsumptions 
                WHERE amount < 0
            """)
            
            if negative_amounts[0][0] > 0:
                validation_results['errors'].append(f"{negative_amounts[0][0]} reconsumptions con montos negativos")
                validation_results['valid'] = False

            logger.info(f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de la carga"""
        return {
            'memberships_inserted': self.stats['memberships_inserted'],
            'reconsumptions_inserted': self.stats['reconsumptions_inserted'],
            'history_inserted': self.stats['history_inserted'],
            'memberships_deleted': self.stats['memberships_deleted'],
            'reconsumptions_deleted': self.stats['reconsumptions_deleted'],
            'history_deleted': self.stats['history_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        self.postgres_conn.disconnect()
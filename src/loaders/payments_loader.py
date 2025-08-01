"""
Cargador de pagos a PostgreSQL (ms-payments)
"""
from typing import List, Dict, Any
from src.connections.payments_postgres_connection import PaymentsPostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentsLoader:
    """Cargador de pagos transformados a PostgreSQL ms-payments"""

    def __init__(self):
        self.postgres_conn = PaymentsPostgresConnection()
        self.stats = {
            'payments_inserted': 0,
            'payment_items_inserted': 0,
            'payments_deleted': 0,
            'payment_items_deleted': 0,
            'errors': []
        }

    def create_tables_if_not_exist(self):
        """Crea las tablas si no existen"""
        logger.info("Verificando/creando tablas de pagos")

        # Crear tabla payments
        create_payments_table = """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            user_email VARCHAR(255) NOT NULL,
            user_name VARCHAR(255),
            payment_config_id INTEGER NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
            payment_method VARCHAR(50) NOT NULL DEFAULT 'VOUCHER',
            operation_code VARCHAR(255),
            bank_name VARCHAR(255),
            operation_date TIMESTAMP,
            ticket_number VARCHAR(255),
            rejection_reason VARCHAR(500),
            reviewed_by_id VARCHAR(255),
            reviewed_by_email VARCHAR(255),
            reviewed_at TIMESTAMP,
            is_archived BOOLEAN DEFAULT false,
            related_entity_type VARCHAR(255),
            related_entity_id VARCHAR(255),
            metadata JSONB,
            external_reference VARCHAR(255),
            gateway_transaction_id VARCHAR(255),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Crear secuencia para payments si no existe
        CREATE SEQUENCE IF NOT EXISTS payments_id_seq;
     
        """

        # Crear tabla payment_items
        create_payment_items_table = """
        CREATE TABLE IF NOT EXISTS payment_items (
            id INTEGER NOT NULL PRIMARY KEY,
            payment_id INTEGER NOT NULL,
            item_type VARCHAR(50) NOT NULL DEFAULT 'VOUCHER_IMAGE',
            url VARCHAR(500),
            url_key VARCHAR(200),
            points_transaction_id VARCHAR(100),
            amount DECIMAL(12,2),
            bank_name VARCHAR(100),
            transaction_date TIMESTAMP,
            FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE
        );
        
        -- Crear secuencia para payment_items si no existe
        CREATE SEQUENCE IF NOT EXISTS payment_items_id_seq;
        """

        # Crear índices para payments
        create_payments_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_payments_user_email ON payments(user_email);",
            "CREATE INDEX IF NOT EXISTS idx_payments_payment_config_id ON payments(payment_config_id);",
            "CREATE INDEX IF NOT EXISTS idx_payments_status_created_at ON payments(status, created_at);",
            "CREATE INDEX IF NOT EXISTS idx_payments_user_id_status ON payments(user_id, status);",
            "CREATE INDEX IF NOT EXISTS idx_payments_user_id_payment_config ON payments(user_id, payment_config_id);"
        ]

        # Crear índices para payment_items
        create_payment_items_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_payment_items_payment_id ON payment_items(payment_id);",
            "CREATE INDEX IF NOT EXISTS idx_payment_items_item_type ON payment_items(item_type);"
        ]

        try:
            # Crear tablas
            self.postgres_conn.execute_query(create_payments_table)
            self.postgres_conn.execute_query(create_payment_items_table)
            logger.info("Tablas de pagos verificadas/creadas")

            # Crear índices de payments
            for index_query in create_payments_indexes:
                self.postgres_conn.execute_query(index_query)

            # Crear índices de payment_items
            for index_query in create_payment_items_indexes:
                self.postgres_conn.execute_query(index_query)

            logger.info("Índices de pagos creados")

        except Exception as e:
            error_msg = f"Error creando tablas/índices: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def clear_existing_data(self) -> Dict[str, int]:
        """Elimina todos los datos existentes"""
        logger.info("Eliminando datos de pagos existentes")

        try:
            # Eliminar payment_items primero (por la clave foránea)
            delete_items_query = "DELETE FROM payment_items"
            items_deleted, _ = self.postgres_conn.execute_query(
                delete_items_query)

            # Eliminar payments
            delete_payments_query = "DELETE FROM payments"
            payments_deleted, _ = self.postgres_conn.execute_query(
                delete_payments_query)

            # Reiniciar secuencias
            reset_payments_seq = """
            SELECT setval('payments_id_seq', 
                         COALESCE((SELECT MAX(id) FROM payments), 0) + 1, 
                         false);
            """
            reset_items_seq = """
            SELECT setval('payment_items_id_seq', 
                         COALESCE((SELECT MAX(id) FROM payment_items), 0) + 1, 
                         false);
            """

            self.postgres_conn.execute_query(reset_payments_seq)
            self.postgres_conn.execute_query(reset_items_seq)

            self.stats['payments_deleted'] = payments_deleted
            self.stats['payment_items_deleted'] = items_deleted

            logger.info(
                f"Eliminados {payments_deleted} pagos y {items_deleted} items")

            return {
                'payments_deleted': payments_deleted,
                'payment_items_deleted': items_deleted
            }

        except Exception as e:
            error_msg = f"Error eliminando datos existentes: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise

    def load_payments(self, payments_data: List[Dict[str, Any]], clear_existing: bool = False) -> Dict[str, Any]:
        """
        Carga los pagos en PostgreSQL

        Args:
            payments_data: Lista de pagos transformados
            clear_existing: Si True, elimina los pagos existentes antes de cargar

        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(
            f"Iniciando carga de {len(payments_data)} pagos en PostgreSQL")

        try:
            # Crear tablas si no existen
            self.create_tables_if_not_exist()

            # Eliminar datos existentes si se solicita
            if clear_existing:
                self.clear_existing_data()

            # Preparar datos para inserción
            if not payments_data:
                logger.warning("No hay pagos para insertar")
                return {
                    'success': True,
                    'inserted_count': 0,
                    'deleted_count': self.stats['payments_deleted']
                }

            # Insertar pagos manteniendo IDs originales
            inserted_count = self._insert_payments_with_original_ids(
                payments_data)

            self.stats['payments_inserted'] = inserted_count
            logger.info(f"Insertados {inserted_count} pagos exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count,
                'deleted_count': self.stats['payments_deleted']
            }

        except Exception as e:
            error_msg = f"Error inesperado cargando pagos: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)

            return {
                'success': False,
                'inserted_count': 0,
                'deleted_count': self.stats['payments_deleted'],
                'error': str(e)
            }

    def load_payment_items(self, payment_items_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Carga los items de pago en PostgreSQL

        Args:
            payment_items_data: Lista de items de pago transformados

        Returns:
            Diccionario con el resultado de la operación
        """
        logger.info(
            f"Iniciando carga de {len(payment_items_data)} items de pago en PostgreSQL")

        try:
            if not payment_items_data:
                logger.warning("No hay items de pago para insertar")
                return {
                    'success': True,
                    'inserted_count': 0
                }

            # Insertar items de pago
            inserted_count = self._insert_payment_items(payment_items_data)

            self.stats['payment_items_inserted'] = inserted_count
            logger.info(
                f"Insertados {inserted_count} items de pago exitosamente")

            return {
                'success': True,
                'inserted_count': inserted_count
            }

        except Exception as e:
            error_msg = f"Error inesperado cargando items de pago: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)

            return {
                'success': False,
                'inserted_count': 0,
                'error': str(e)
            }

    def _insert_payments_with_original_ids(self, payments_data: List[Dict[str, Any]]) -> int:
        """Inserta pagos manteniendo los IDs originales"""

        insert_query = """
        INSERT INTO payments (
            id, user_id, user_email, user_name, payment_config_id, amount,
            status, payment_method, operation_code, bank_name, operation_date,
            ticket_number, rejection_reason, reviewed_by_id, reviewed_by_email,
            reviewed_at, is_archived, related_entity_type, related_entity_id,
            metadata, external_reference, gateway_transaction_id, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        # Preparar lista de parámetros
        params_list = []
        for payment in payments_data:
            # Convertir metadata a JSON string si es un dict
            metadata_value = payment['metadata']
            if isinstance(metadata_value, dict):
                import json
                metadata_value = json.dumps(metadata_value)
            elif metadata_value is None:
                metadata_value = None

            params = (
                payment['id'],
                payment['user_id'],
                payment['user_email'],
                payment['user_name'],
                payment['payment_config_id'],
                payment['amount'],
                payment['status'],
                payment['payment_method'],
                payment['operation_code'],
                payment['bank_name'],
                payment['operation_date'],
                payment['ticket_number'],
                payment['rejection_reason'],
                payment['reviewed_by_id'],
                payment['reviewed_by_email'],
                payment['reviewed_at'],
                payment['is_archived'],
                payment['related_entity_type'],
                payment['related_entity_id'],
                metadata_value,
                payment['external_reference'],
                payment['gateway_transaction_id'],
                payment['created_at'],
                payment['updated_at']
            )
            params_list.append(params)

        try:
            # Insertar todos los registros
            inserted_count = self.postgres_conn.execute_bulk_insert(
                insert_query, params_list)

            # Actualizar la secuencia para que continúe desde el máximo ID insertado
            max_id = max(payment['id'] for payment in payments_data)
            update_sequence_query = "SELECT setval('payments_id_seq', %s, true);"
            self.postgres_conn.execute_query(update_sequence_query, (max_id,))

            logger.info(
                f"Secuencia de pagos actualizada para continuar desde ID {max_id}")
            return len(payments_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de pagos: {str(e)}")
            # Intentar inserción una por una para identificar registros problemáticos
            return self._insert_payments_one_by_one(payments_data)

    def _insert_payments_one_by_one(self, payments_data: List[Dict[str, Any]]) -> int:
        """Inserta pagos uno por uno para manejar errores individuales"""
        logger.warning(
            "Intentando inserción de pagos una por una debido a errores en inserción masiva")

        insert_query = """
        INSERT INTO payments (
            id, user_id, user_email, user_name, payment_config_id, amount,
            status, payment_method, operation_code, bank_name, operation_date,
            ticket_number, rejection_reason, reviewed_by_id, reviewed_by_email,
            reviewed_at, is_archived, related_entity_type, related_entity_id,
            metadata, external_reference, gateway_transaction_id, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        successful_inserts = 0

        for payment in payments_data:
            try:
                # Convertir metadata a JSON string si es un dict
                metadata_value = payment['metadata']
                if isinstance(metadata_value, dict):
                    import json
                    metadata_value = json.dumps(metadata_value)
                elif metadata_value is None:
                    metadata_value = None

                params = (
                    payment['id'],
                    payment['user_id'],
                    payment['user_email'],
                    payment['user_name'],
                    payment['payment_config_id'],
                    payment['amount'],
                    payment['status'],
                    payment['payment_method'],
                    payment['operation_code'],
                    payment['bank_name'],
                    payment['operation_date'],
                    payment['ticket_number'],
                    payment['rejection_reason'],
                    payment['reviewed_by_id'],
                    payment['reviewed_by_email'],
                    payment['reviewed_at'],
                    payment['is_archived'],
                    payment['related_entity_type'],
                    payment['related_entity_id'],
                    metadata_value,
                    payment['external_reference'],
                    payment['gateway_transaction_id'],
                    payment['created_at'],
                    payment['updated_at']
                )

                self.postgres_conn.execute_query(insert_query, params)
                successful_inserts += 1

            except Exception as e:
                error_msg = f"Error insertando pago ID {payment['id']}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        # Actualizar secuencia
        if successful_inserts > 0:
            max_id = max(payment['id'] for payment in payments_data)
            update_sequence_query = "SELECT setval('payments_id_seq', %s, true);"
            self.postgres_conn.execute_query(update_sequence_query, (max_id,))

        return successful_inserts

    def _insert_payment_items(self, payment_items_data: List[Dict[str, Any]]) -> int:
        """Inserta items de pago conservando IDs originales"""

        insert_query = """
        INSERT INTO payment_items (
            id, payment_id, item_type, url, url_key, points_transaction_id, 
            amount, bank_name, transaction_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Preparar lista de parámetros
        params_list = []
        for item in payment_items_data:
            params = (
                item['id'],  # Incluir ID original
                item['payment_id'],
                item['item_type'],
                item['url'],
                item['url_key'],
                item['points_transaction_id'],
                item['amount'],
                item['bank_name'],
                item['transaction_date']
            )
            params_list.append(params)

        try:
            # Insertar todos los registros
            inserted_count = self.postgres_conn.execute_bulk_insert(
                insert_query, params_list)

            # Actualizar la secuencia para que continúe desde el máximo ID insertado
            if payment_items_data:
                max_id = max(item['id'] for item in payment_items_data)
                update_sequence_query = "SELECT setval('payment_items_id_seq', %s, true);"
                self.postgres_conn.execute_query(
                    update_sequence_query, (max_id,))
                logger.info(
                    f"Secuencia de payment_items actualizada para continuar desde ID {max_id}")

            return len(payment_items_data)

        except Exception as e:
            logger.error(f"Error en inserción masiva de items: {str(e)}")
            # Intentar inserción una por una
            return self._insert_payment_items_one_by_one(payment_items_data)

    def _insert_payment_items_one_by_one(self, payment_items_data: List[Dict[str, Any]]) -> int:
        """Inserta items de pago uno por uno para manejar errores individuales"""
        logger.warning(
            "Intentando inserción de items una por una debido a errores en inserción masiva")

        insert_query = """
        INSERT INTO payment_items (
            id, payment_id, item_type, url, url_key, points_transaction_id, 
            amount, bank_name, transaction_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        successful_inserts = 0

        for item in payment_items_data:
            try:
                params = (
                    item['id'],  # Incluir ID original
                    item['payment_id'],
                    item['item_type'],
                    item['url'],
                    item['url_key'],
                    item['points_transaction_id'],
                    item['amount'],
                    item['bank_name'],
                    item['transaction_date']
                )

                self.postgres_conn.execute_query(insert_query, params)
                successful_inserts += 1

            except Exception as e:
                error_msg = f"Error insertando item ID {item.get('id', 'unknown')} del pago {item['payment_id']}: {str(e)}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)

        # Actualizar secuencia
        if successful_inserts > 0 and payment_items_data:
            max_id = max(item['id'] for item in payment_items_data)
            update_sequence_query = "SELECT setval('payment_items_id_seq', %s, true);"
            self.postgres_conn.execute_query(update_sequence_query, (max_id,))

        return successful_inserts

    def validate_data_integrity(self) -> Dict[str, Any]:
        """Valida la integridad de los datos cargados"""
        logger.info("Validando integridad de datos de pagos en PostgreSQL")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        try:
            # Contar registros totales
            payments_count_query = "SELECT COUNT(*) FROM payments"
            payments_count, _ = self.postgres_conn.execute_query(
                payments_count_query)
            total_payments = payments_count[0][0]

            items_count_query = "SELECT COUNT(*) FROM payment_items"
            items_count, _ = self.postgres_conn.execute_query(
                items_count_query)
            total_items = items_count[0][0]

            # Contar por estado
            status_query = """
            SELECT status, COUNT(*) as count 
            FROM payments 
            GROUP BY status
            """
            status_results, _ = self.postgres_conn.execute_query(status_query)
            payments_by_status = {}
            for row in status_results:
                payments_by_status[row[0]] = row[1]

            # Contar por método
            method_query = """
            SELECT payment_method, COUNT(*) as count 
            FROM payments 
            GROUP BY payment_method
            """
            method_results, _ = self.postgres_conn.execute_query(method_query)
            payments_by_method = {}
            for row in method_results:
                payments_by_method[row[0]] = row[1]

            validation_results['stats'] = {
                'total_payments': total_payments,
                'total_payment_items': total_items,
                'payments_by_status': payments_by_status,
                'payments_by_method': payments_by_method
            }

            # Validar campos obligatorios en pagos
            missing_data_query = """
            SELECT COUNT(*) FROM payments 
            WHERE user_email IS NULL OR user_email = '' 
               OR payment_config_id IS NULL 
               OR amount IS NULL OR amount <= 0
            """
            missing_count, _ = self.postgres_conn.execute_query(
                missing_data_query)
            missing_data = missing_count[0][0]

            if missing_data > 0:
                validation_results['errors'].append(
                    f"{missing_data} pagos con campos obligatorios inválidos")
                validation_results['valid'] = False

            # Validar referencias de configuración
            invalid_config_query = """
            SELECT COUNT(*) FROM payments p 
            LEFT JOIN payment_configs pc ON p.payment_config_id = pc.id 
            WHERE pc.id IS NULL
            """
            invalid_config_count, _ = self.postgres_conn.execute_query(
                invalid_config_query)
            invalid_configs = invalid_config_count[0][0]

            if invalid_configs > 0:
                validation_results['errors'].append(
                    f"{invalid_configs} pagos con configuración inválida")
                validation_results['valid'] = False

            # Validar integridad referencial de items
            orphan_items_query = """
            SELECT COUNT(*) FROM payment_items pi 
            LEFT JOIN payments p ON pi.payment_id = p.id 
            WHERE p.id IS NULL
            """
            orphan_items_count, _ = self.postgres_conn.execute_query(
                orphan_items_query)
            orphan_items = orphan_items_count[0][0]

            if orphan_items > 0:
                validation_results['errors'].append(
                    f"{orphan_items} items huérfanos sin pago asociado")
                validation_results['valid'] = False

            # Validar items sin datos válidos
            invalid_items_query = """
            SELECT COUNT(*) FROM payment_items 
            WHERE (url IS NULL OR url = '') AND (points_transaction_id IS NULL OR points_transaction_id = '')
            """
            invalid_items_count, _ = self.postgres_conn.execute_query(
                invalid_items_query)
            invalid_items = invalid_items_count[0][0]

            if invalid_items > 0:
                validation_results['warnings'].append(
                    f"{invalid_items} items sin URL ni referencia de transacción")

            logger.info(
                f"Validación de integridad: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")

        except Exception as e:
            error_msg = f"Error en validación de integridad: {str(e)}"
            logger.error(error_msg)
            validation_results['errors'].append(error_msg)
            validation_results['valid'] = False

        return validation_results

    def get_load_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la carga"""
        return {
            'payments_inserted': self.stats['payments_inserted'],
            'payment_items_inserted': self.stats['payment_items_inserted'],
            'payments_deleted': self.stats['payments_deleted'],
            'payment_items_deleted': self.stats['payment_items_deleted'],
            'total_errors': len(self.stats['errors']),
            'errors': self.stats['errors']
        }

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()

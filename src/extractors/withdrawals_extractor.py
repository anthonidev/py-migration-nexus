import os
import sys
import json
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WithdrawalsExtractor:
    
    def __init__(self):
        self.postgres_conn = PostgresConnection()
    
    def extract_withdrawals_data(self) -> List[Dict[str, Any]]:
        """Extrae todos los withdrawals con sus withdrawal_points de la BD del monolito"""
        logger.info("Iniciando extracción de withdrawals desde PostgreSQL")
        
        query = """
        -- Consulta completa de withdrawals con todas sus entidades relacionadas en JSON
        SELECT
            w.id,
            w.amount,
            w.status,
            w."rejectionReason",
            w."createdAt",
            w."updatedAt",
            w."reviewedAt",
            w."isArchived",
            w."bankName",
            w."accountNumber",
            w.cci,
            w.metadata,

            -- Email del usuario que realizó el withdrawal
            u.email AS user_email,

            -- Email del usuario que revisó el withdrawal
            reviewer.email AS reviewed_by_email,

            -- Withdrawal points asociados con sus points transactions
            (
                SELECT COALESCE(
                               JSON_AGG(
                                       JSON_BUILD_OBJECT(
                                               'id', wp.id,
                                               'amountUsed', wp."amountUsed",
                                               'createdAt', wp."createdAt",
                                               'pointsTransaction', CASE
                                                                        WHEN pt.id IS NOT NULL THEN
                                                                            JSON_BUILD_OBJECT(
                                                                                    'id', pt.id,
                                                                                    'type', pt.type,
                                                                                    'amount', pt.amount,
                                                                                    'status', pt.status,
                                                                                    'pendingAmount', pt."pendingAmount",
                                                                                    'withdrawnAmount', pt."withdrawnAmount",
                                                                                    'createdAt', pt."createdAt",
                                                                                    'metadata', pt.metadata,
                                                                                    'membershipPlan', CASE
                                                                                                          WHEN mp.id IS NOT NULL THEN
                                                                                                              JSON_BUILD_OBJECT(
                                                                                                                      'id', mp.id,
                                                                                                                      'name', mp.name,
                                                                                                                      'price', mp.price,
                                                                                                                      'binaryPoints', mp."binaryPoints",
                                                                                                                      'commissionPercentage', mp."commissionPercentage"
                                                                                                              )
                                                                                                          ELSE NULL
                                                                                        END
                                                                            )
                                                                        ELSE NULL
                                                   END
                                       )
                               ), '[]'::json
                       )
                FROM withdrawal_points wp
                         LEFT JOIN points_transactions pt ON wp.points_transaction_id = pt.id
                         LEFT JOIN membership_plans mp ON pt.membership_plan_id = mp.id
                WHERE wp.withdrawal_id = w.id
            ) AS withdrawal_points

        FROM withdrawals w
                 LEFT JOIN users u ON w.user_id = u.id
                 LEFT JOIN users reviewer ON w.reviewed_by_id = reviewer.id

        ORDER BY w."createdAt" DESC;
        """
        
        try:
            results, columns = self.postgres_conn.execute_query(query)
            
            withdrawals_data = []
            for row in results:
                withdrawal_dict = dict(zip(columns, row))
                
                # Parsear el JSON de withdrawal_points
                if withdrawal_dict['withdrawal_points']:
                    if isinstance(withdrawal_dict['withdrawal_points'], str):
                        withdrawal_dict['withdrawal_points'] = json.loads(withdrawal_dict['withdrawal_points'])
                else:
                    withdrawal_dict['withdrawal_points'] = []
                
                withdrawals_data.append(withdrawal_dict)
            
            logger.info(f"Extraídos {len(withdrawals_data)} withdrawals desde PostgreSQL")
            return withdrawals_data
            
        except Exception as e:
            logger.error(f"Error extrayendo withdrawals: {str(e)}")
            raise
    
    def validate_source_data(self) -> Dict[str, Any]:
        """Valida los datos de origen antes de la migración"""
        logger.info("Validando datos de withdrawals en el monolito")
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Verificar que existan withdrawals
            count_query = "SELECT COUNT(*) FROM withdrawals"
            count_result, _ = self.postgres_conn.execute_query(count_query)
            withdrawals_count = count_result[0][0]
            
            if withdrawals_count == 0:
                validation_results['warnings'].append("No hay withdrawals para migrar")
                logger.warning("No se encontraron withdrawals en la BD del monolito")
                return validation_results
            
            logger.info(f"Total de withdrawals encontrados: {withdrawals_count}")
            
            # Verificar withdrawals sin usuario
            no_user_query = """
            SELECT COUNT(*) FROM withdrawals w 
            LEFT JOIN users u ON w.user_id = u.id 
            WHERE u.id IS NULL
            """
            no_user_results, _ = self.postgres_conn.execute_query(no_user_query)
            withdrawals_without_user = no_user_results[0][0]
            
            if withdrawals_without_user > 0:
                validation_results['errors'].append(
                    f"{withdrawals_without_user} withdrawals sin usuario válido"
                )
                validation_results['valid'] = False
            
            # Verificar withdrawals sin email de usuario
            no_email_query = """
            SELECT COUNT(*) FROM withdrawals w 
            LEFT JOIN users u ON w.user_id = u.id 
            WHERE u.email IS NULL OR u.email = ''
            """
            no_email_results, _ = self.postgres_conn.execute_query(no_email_query)
            withdrawals_without_email = no_email_results[0][0]
            
            if withdrawals_without_email > 0:
                validation_results['errors'].append(
                    f"{withdrawals_without_email} withdrawals con usuarios sin email"
                )
                validation_results['valid'] = False
            
            # Verificar withdrawals con amounts nulos o negativos
            invalid_amount_query = """
            SELECT COUNT(*) FROM withdrawals 
            WHERE amount IS NULL OR amount <= 0
            """
            invalid_amount_results, _ = self.postgres_conn.execute_query(invalid_amount_query)
            invalid_amounts = invalid_amount_results[0][0]
            
            if invalid_amounts > 0:
                validation_results['errors'].append(
                    f"{invalid_amounts} withdrawals con amounts inválidos (nulo o <= 0)"
                )
                validation_results['valid'] = False
            
            # Verificar withdrawals sin datos bancarios requeridos
            no_bank_data_query = """
            SELECT COUNT(*) FROM withdrawals 
            WHERE "bankName" IS NULL OR "bankName" = '' 
               OR "accountNumber" IS NULL OR "accountNumber" = ''
            """
            no_bank_data_results, _ = self.postgres_conn.execute_query(no_bank_data_query)
            no_bank_data = no_bank_data_results[0][0]
            
            if no_bank_data > 0:
                validation_results['warnings'].append(
                    f"{no_bank_data} withdrawals sin datos bancarios completos"
                )
            
            # Verificar withdrawal_points huérfanos (sin withdrawal)
            orphan_points_query = """
            SELECT COUNT(*) FROM withdrawal_points wp 
            LEFT JOIN withdrawals w ON wp.withdrawal_id = w.id 
            WHERE w.id IS NULL
            """
            orphan_points_results, _ = self.postgres_conn.execute_query(orphan_points_query)
            orphan_points = orphan_points_results[0][0]
            
            if orphan_points > 0:
                validation_results['warnings'].append(
                    f"{orphan_points} withdrawal_points huérfanos (sin withdrawal padre)"
                )
            
            # Verificar withdrawal_points sin points_transaction
            no_transaction_query = """
            SELECT COUNT(*) FROM withdrawal_points wp 
            LEFT JOIN points_transactions pt ON wp.points_transaction_id = pt.id 
            WHERE pt.id IS NULL
            """
            no_transaction_results, _ = self.postgres_conn.execute_query(no_transaction_query)
            no_transactions = no_transaction_results[0][0]
            
            if no_transactions > 0:
                validation_results['warnings'].append(
                    f"{no_transactions} withdrawal_points sin points_transaction válida"
                )
            
            logger.info("Validación de datos de origen completada")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validando datos de origen: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results
    
    def validate_migration_integrity(self, loader) -> Dict[str, Any]:
        """Valida la integridad de los datos migrados"""
        logger.info("Validando integridad de la migración de withdrawals")
        
        integrity_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            # Contar datos en origen
            origin_withdrawals_query = "SELECT COUNT(*) FROM withdrawals"
            origin_withdrawals, _ = self.postgres_conn.execute_query(origin_withdrawals_query)
            origin_withdrawals_count = origin_withdrawals[0][0]
            
            origin_points_query = "SELECT COUNT(*) FROM withdrawal_points"
            origin_points, _ = self.postgres_conn.execute_query(origin_points_query)
            origin_points_count = origin_points[0][0]
            
            # Contar datos en destino
            dest_withdrawals_query = "SELECT COUNT(*) FROM withdrawals"
            dest_withdrawals, _ = loader.payments_conn.execute_query(dest_withdrawals_query)
            dest_withdrawals_count = dest_withdrawals[0][0]
            
            dest_points_query = "SELECT COUNT(*) FROM withdrawal_points"
            dest_points, _ = loader.payments_conn.execute_query(dest_points_query)
            dest_points_count = dest_points[0][0]
            
            # Almacenar estadísticas
            integrity_results['stats'] = {
                'origin_withdrawals': origin_withdrawals_count,
                'dest_withdrawals': dest_withdrawals_count,
                'total_withdrawals': dest_withdrawals_count,
                'origin_withdrawal_points': origin_points_count,
                'dest_withdrawal_points': dest_points_count,
                'total_withdrawal_points': dest_points_count
            }
            
            # Validar que los conteos coincidan
            if origin_withdrawals_count != dest_withdrawals_count:
                integrity_results['errors'].append(
                    f"Conteo de withdrawals no coincide: origen={origin_withdrawals_count}, destino={dest_withdrawals_count}"
                )
                integrity_results['valid'] = False
            
            if origin_points_count != dest_points_count:
                integrity_results['errors'].append(
                    f"Conteo de withdrawal_points no coincide: origen={origin_points_count}, destino={dest_points_count}"
                )
                integrity_results['valid'] = False
            
            # Verificar integridad referencial
            orphan_points_dest_query = """
            SELECT COUNT(*) FROM withdrawal_points wp 
            LEFT JOIN withdrawals w ON wp.withdrawal_id = w.id 
            WHERE w.id IS NULL
            """
            orphan_dest, _ = loader.payments_conn.execute_query(orphan_points_dest_query)
            orphan_dest_count = orphan_dest[0][0]
            
            if orphan_dest_count > 0:
                integrity_results['errors'].append(
                    f"{orphan_dest_count} withdrawal_points huérfanos en destino"
                )
                integrity_results['valid'] = False
            
            logger.info(f"Validación de integridad completada: {integrity_results['stats']}")
            return integrity_results
            
        except Exception as e:
            logger.error(f"Error validando integridad: {str(e)}")
            integrity_results['valid'] = False
            integrity_results['errors'].append(f"Error en validación de integridad: {str(e)}")
            return integrity_results
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.postgres_conn:
            self.postgres_conn.disconnect()
from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from src.connections.postgres_connection import PostgresConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MembershipsExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_memberships_data(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando extracción de membresías de usuarios desde PostgreSQL")

        query = """
        SELECT
            m.id as membership_id,
            u.email as "userEmail",
            mp.id as plan_id,
            mp.name as plan,
            m."startDate",
            m."endDate",
            m.status,
            m."minimumReconsumptionAmount",
            m."autoRenewal",
            m."createdAt",
            m."updatedAt",
            -- Subconsulta para obtener las reconsumptions como JSON
            COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'id', mr.id,
                                            'amount', mr.amount,
                                            'status', mr.status,
                                            'periodDate', mr."periodDate",
                                            'paymentReference', mr."paymentReference",
                                            'paymentDetails', mr."paymentDetails",
                                            'notes', mr.notes,
                                            'createdAt', mr."createdAt",
                                            'updatedAt', mr."updatedAt"
                                    )
                            )
             FROM membership_reconsumptions mr
             WHERE mr.membership_id = m.id),
                    '[]'::json
            ) as reconsumptions,
            -- Subconsulta para obtener el membership_history como JSON
            COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'id', history_data.id,
                                            'action', history_data.action,
                                            'changes', history_data.changes,
                                            'notes', history_data.notes,
                                            'metadata', history_data.metadata,
                                            'createdAt', history_data."createdAt"
                                    ) ORDER BY history_data."createdAt" DESC
                            )
             FROM (
                          SELECT
                              mh.id,
                              mh.action,
                              mh.changes,
                              mh.notes,
                              mh.metadata,
                              mh."createdAt"
                          FROM membership_history mh
                          WHERE mh.membership_id = m.id
                      ) as history_data),
                    '[]'::json
            ) as membership_history
        FROM memberships m
                 INNER JOIN users u ON m.user_id = u.id
                 INNER JOIN membership_plans mp ON m.plan_id = mp.id
        ORDER BY m."createdAt" DESC;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            memberships_data = []
            for row in results:
                membership_dict = dict(zip(columns, row))
                memberships_data.append(membership_dict)

            logger.info(f"Extraídas {len(memberships_data)} membresías desde PostgreSQL")
            return memberships_data

        except Exception as e:
            logger.error(f"Error extrayendo membresías: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen para membresías")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar membresías sin usuario válido
            orphan_memberships_query = """
            SELECT COUNT(*) 
            FROM memberships m 
            LEFT JOIN users u ON m.user_id = u.id 
            WHERE u.id IS NULL
            """
            orphan_results, _ = self.postgres_conn.execute_query(orphan_memberships_query)
            orphan_memberships = orphan_results[0][0]

            if orphan_memberships > 0:
                validation_results['errors'].append(f"{orphan_memberships} membresías sin usuario válido")
                validation_results['valid'] = False

            # Validar membresías sin plan válido
            invalid_plan_query = """
            SELECT COUNT(*) 
            FROM memberships m 
            LEFT JOIN membership_plans mp ON m.plan_id = mp.id 
            WHERE mp.id IS NULL
            """
            invalid_plan_results, _ = self.postgres_conn.execute_query(invalid_plan_query)
            invalid_plans = invalid_plan_results[0][0]

            if invalid_plans > 0:
                validation_results['errors'].append(f"{invalid_plans} membresías con plan inválido")
                validation_results['valid'] = False

            # Validar fechas inconsistentes
            invalid_dates_query = """
            SELECT COUNT(*) 
            FROM memberships 
            WHERE "startDate" IS NULL 
               OR ("endDate" IS NOT NULL AND "endDate" < "startDate")
            """
            invalid_dates_results, _ = self.postgres_conn.execute_query(invalid_dates_query)
            invalid_dates = invalid_dates_results[0][0]

            if invalid_dates > 0:
                validation_results['errors'].append(f"{invalid_dates} membresías con fechas inválidas")
                validation_results['valid'] = False

            # Validar montos de reconsumo negativos
            invalid_amounts_query = """
            SELECT COUNT(*) 
            FROM memberships 
            WHERE "minimumReconsumptionAmount" < 0
            """
            invalid_amounts_results, _ = self.postgres_conn.execute_query(invalid_amounts_query)
            invalid_amounts = invalid_amounts_results[0][0]

            if invalid_amounts > 0:
                validation_results['errors'].append(f"{invalid_amounts} membresías con monto de reconsumo negativo")
                validation_results['valid'] = False

            # Validar reconsumptions huérfanos
            orphan_reconsumptions_query = """
            SELECT COUNT(*) 
            FROM membership_reconsumptions mr 
            LEFT JOIN memberships m ON mr.membership_id = m.id 
            WHERE m.id IS NULL
            """
            orphan_reconsumptions_results, _ = self.postgres_conn.execute_query(orphan_reconsumptions_query)
            orphan_reconsumptions = orphan_reconsumptions_results[0][0]

            if orphan_reconsumptions > 0:
                validation_results['warnings'].append(f"{orphan_reconsumptions} reconsumptions huérfanos (se omitirán)")

            # Validar history huérfano
            orphan_history_query = """
            SELECT COUNT(*) 
            FROM membership_history mh 
            LEFT JOIN memberships m ON mh.membership_id = m.id 
            WHERE m.id IS NULL
            """
            orphan_history_results, _ = self.postgres_conn.execute_query(orphan_history_query)
            orphan_history = orphan_history_results[0][0]

            if orphan_history > 0:
                validation_results['warnings'].append(f"{orphan_history} registros de history huérfanos (se omitirán)")

            logger.info(f"Validación de datos: {'EXITOSA' if validation_results['valid'] else 'FALLÓ'}")
            return validation_results

        except Exception as e:
            logger.error(f"Error en validación de datos: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Error en validación: {str(e)}")
            return validation_results

    def close_connection(self):
        """Cierra la conexión a PostgreSQL"""
        self.postgres_conn.disconnect()
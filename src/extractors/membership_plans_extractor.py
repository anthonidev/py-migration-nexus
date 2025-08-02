from src.utils.logger import get_logger
from src.connections.postgres_connection import PostgresConnection
from typing import List, Dict, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

logger = get_logger(__name__)

class MembershipPlansExtractor:

    def __init__(self):
        self.postgres_conn = PostgresConnection()

    def extract_membership_plans(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando extracción de planes de membresía desde PostgreSQL")

        query = """
        SELECT 
            id,
            name, 
            price,
            "checkAmount", 
            "binaryPoints",
            "commissionPercentage",
            "directCommissionAmount",
            products,
            benefits,
            "isActive",
            "displayOrder",
            "createdAt", 
            "updatedAt" 
        FROM 
            membership_plans
        ORDER BY 
            "displayOrder", id;
        """

        try:
            results, columns = self.postgres_conn.execute_query(query)

            plans_data = []
            for row in results:
                plan_dict = dict(zip(columns, row))
                plans_data.append(plan_dict)

            logger.info(f"Extraídos {len(plans_data)} planes de membresía desde PostgreSQL")
            return plans_data

        except Exception as e:
            logger.error(f"Error extrayendo planes de membresía: {str(e)}")
            raise

    def validate_source_data(self) -> Dict[str, Any]:
        """Valida la integridad de los datos de origen"""
        logger.info("Validando datos de origen para planes de membresía")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Validar nombres requeridos
            missing_names_query = """
            SELECT id, name FROM membership_plans 
            WHERE name IS NULL or name = ''
            """
            missing_names_results, _ = self.postgres_conn.execute_query(missing_names_query)

            if missing_names_results:
                for row in missing_names_results:
                    plan_id, name = row
                    validation_results['errors'].append(f"Plan ID {plan_id}: nombre requerido está vacío")
                validation_results['valid'] = False

            # Validar precios válidos
            invalid_prices_query = """
            SELECT id, price FROM membership_plans 
            WHERE price IS NULL OR price < 0
            """
            invalid_prices_results, _ = self.postgres_conn.execute_query(invalid_prices_query)

            if invalid_prices_results:
                for row in invalid_prices_results:
                    plan_id, price = row
                    validation_results['errors'].append(f"Plan ID {plan_id}: precio inválido ({price})")
                validation_results['valid'] = False

            # Validar checkAmount válidos
            invalid_check_query = """
            SELECT id, "checkAmount" FROM membership_plans 
            WHERE "checkAmount" IS NULL OR "checkAmount" < 0
            """
            invalid_check_results, _ = self.postgres_conn.execute_query(invalid_check_query)

            if invalid_check_results:
                for row in invalid_check_results:
                    plan_id, check_amount = row
                    validation_results['errors'].append(f"Plan ID {plan_id}: checkAmount inválido ({check_amount})")
                validation_results['valid'] = False

            # Validar binaryPoints válidos
            invalid_binary_query = """
            SELECT id, "binaryPoints" FROM membership_plans 
            WHERE "binaryPoints" IS NULL OR "binaryPoints" < 0
            """
            invalid_binary_results, _ = self.postgres_conn.execute_query(invalid_binary_query)

            if invalid_binary_results:
                for row in invalid_binary_results:
                    plan_id, binary_points = row
                    validation_results['errors'].append(f"Plan ID {plan_id}: binaryPoints inválido ({binary_points})")
                validation_results['valid'] = False

            # Validar porcentajes de comisión
            invalid_commission_query = """
            SELECT id, "commissionPercentage" FROM membership_plans 
            WHERE "commissionPercentage" IS NULL 
            OR "commissionPercentage" < 0 
            OR "commissionPercentage" > 100
            """
            invalid_commission_results, _ = self.postgres_conn.execute_query(invalid_commission_query)

            if invalid_commission_results:
                for row in invalid_commission_results:
                    plan_id, commission = row
                    validation_results['errors'].append(f"Plan ID {plan_id}: commissionPercentage inválido ({commission})")
                validation_results['valid'] = False

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
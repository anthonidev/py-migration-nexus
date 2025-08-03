import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class EntityReport:
    name: str
    extracted: int = 0
    transformed: int = 0
    inserted: int = 0
    deleted: int = 0
    errors: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "extracted": self.extracted,
            "transformed": self.transformed,
            "inserted": self.inserted,
            "deleted": self.deleted,
            "errors": self.errors
        }

@dataclass
class MigrationReport:
    migration_name: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    success: bool = True
    entities: Dict[str, EntityReport] = field(default_factory=dict)
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    def add_entity(self, name: str) -> EntityReport:
        entity = EntityReport(name=name)
        self.entities[name] = entity
        return entity
    
    def get_entity(self, name: str) -> EntityReport:
        if name not in self.entities:
            return self.add_entity(name)
        return self.entities[name]
    
    def mark_completed(self, success: bool = True):
        self.end_time = datetime.now()
        self.success = success
    
    def get_duration(self) -> str:
        if self.end_time:
            return str(self.end_time - self.start_time)
        return str(datetime.now() - self.start_time)
    
    def get_totals(self) -> Dict[str, int]:
        totals = {
            "extracted": 0,
            "transformed": 0,
            "inserted": 0,
            "deleted": 0,
            "errors": 0,
            "warnings": len(self.validation_warnings)
        }
        
        for entity in self.entities.values():
            totals["extracted"] += entity.extracted
            totals["transformed"] += entity.transformed
            totals["inserted"] += entity.inserted
            totals["deleted"] += entity.deleted
            totals["errors"] += entity.errors
        
        totals["errors"] += len(self.validation_errors)
        
        return totals
    
    def is_data_integrity_valid(self) -> bool:
        return len(self.validation_errors) == 0
    
    def to_dict(self) -> Dict[str, Any]:
        entities_dict = {}
        for name, entity in self.entities.items():
            entities_dict[name] = entity.to_dict()
        
        return {
            "success": self.success,
            "duration": self.get_duration(),
            "entities": entities_dict,
            "totals": self.get_totals(),
            "validation": {
                "data_integrity": self.is_data_integrity_valid(),
                "errors": self.validation_errors,
                "warnings": self.validation_warnings
            }
        }
    
    def save_to_file(self, output_dir: str = ".", filename_prefix: Optional[str] = None) -> str:
        if filename_prefix is None:
            filename_prefix = f"{self.migration_name}_migration_report"
        
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = Path(output_dir) / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2, default=str)
            
            logger.info(f"📄 Reporte de migración guardado en: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {str(e)}")
            raise

class MigrationReportBuilder:
    
    def __init__(self, migration_name: str):
        self.report = MigrationReport(migration_name=migration_name)
    
    def extraction_completed(self, entity_name: str, count: int) -> 'MigrationReportBuilder':
        entity = self.report.get_entity(entity_name)
        entity.extracted = count
        return self
    
    def transformation_completed(self, entity_name: str, count: int, errors: int = 0) -> 'MigrationReportBuilder':
        entity = self.report.get_entity(entity_name)
        entity.transformed = count
        entity.errors += errors
        return self
    
    def loading_completed(self, entity_name: str, inserted: int, deleted: int = 0, errors: int = 0) -> 'MigrationReportBuilder':
        entity = self.report.get_entity(entity_name)
        entity.inserted = inserted
        entity.deleted = deleted
        entity.errors += errors
        return self
    
    def add_validation_errors(self, errors: List[str]) -> 'MigrationReportBuilder':
        self.report.validation_errors.extend(errors)
        return self
    
    def add_validation_warnings(self, warnings: List[str]) -> 'MigrationReportBuilder':
        self.report.validation_warnings.extend(warnings)
        return self
    
    def mark_success(self) -> 'MigrationReportBuilder':
        self.report.mark_completed(success=True)
        return self
    
    def mark_failure(self) -> 'MigrationReportBuilder':
        self.report.mark_completed(success=False)
        return self
    
    def build(self) -> MigrationReport:
        if self.report.end_time is None:
            self.report.mark_completed()
        return self.report

def create_single_entity_report(migration_name: str, entity_name: str,
                               extracted: int, transformed: int, inserted: int,
                               deleted: int = 0, errors: List[str] = None,
                               warnings: List[str] = None) -> MigrationReport:
    builder = MigrationReportBuilder(migration_name)
    
    builder.extraction_completed(entity_name, extracted)
    builder.transformation_completed(entity_name, transformed)
    builder.loading_completed(entity_name, inserted, deleted)
    
    if errors:
        builder.add_validation_errors(errors)
    
    if warnings:
        builder.add_validation_warnings(warnings)
    
    success = len(errors) == 0 if errors else True
    
    if success:
        builder.mark_success()
    else:
        builder.mark_failure()
    
    return builder.build()

def save_migration_report(report: MigrationReport, output_dir: str = ".",
                         filename_prefix: Optional[str] = None) -> str:
    return report.save_to_file(output_dir, filename_prefix)


def extract_validation_issues(validation_result: Dict[str, Any]) -> tuple[List[str], List[str]]:
    errors = validation_result.get('errors', [])
    warnings = validation_result.get('warnings', [])
    
    if 'error' in validation_result:
        errors.append(validation_result['error'])
    
    return errors, warnings

def process_transformation_summary(summary: Dict[str, Any]) -> tuple[int, List[str], List[str]]:
    errors = summary.get('errors', [])
    warnings = summary.get('warnings', [])
    total_errors = summary.get('total_errors', len(errors))
    
    return total_errors, errors, warnings
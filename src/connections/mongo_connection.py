import pymongo
from pymongo.database import Database
from src.config.database_config import DatabaseConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MongoConnection:
    """Manejo de conexiones a MongoDB"""
    
    def __init__(self):
        self.client = None
        self.database = None
    
    def connect(self) -> Database:
        """Establece conexión a MongoDB"""
        try:
            mongo_url = DatabaseConfig.get_mongo_url()
            self.client = pymongo.MongoClient(mongo_url)
            
            # Verificar conexión
            self.client.admin.command('ping')
            
            # Obtener base de datos
            db_name = DatabaseConfig.get_mongo_database_name()
            if db_name:
                self.database = self.client[db_name]
            else:
                self.database = self.client.get_default_database()
            
            logger.info(f"Conexión a MongoDB establecida exitosamente. DB: {self.database.name}")
            return self.database
            
        except Exception as e:
            logger.error(f"Error conectando a MongoDB: {str(e)}")
            raise
    
    def disconnect(self):
        """Cierra la conexión a MongoDB"""
        if self.client:
            self.client.close()
            self.client = None
            self.database = None
            logger.info("Conexión a MongoDB cerrada")
    
    def get_database(self) -> Database:
        """Retorna la instancia de la base de datos"""
        if self.database is None:
            self.connect()
        return self.database
    
    def get_collection(self, collection_name: str):
        """Retorna una colección específica"""
        return self.get_database()[collection_name]
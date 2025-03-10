import psycopg2
from config.logger import logger

class Postgres:
    def __init__(self):
        data_manager = DataManager.get_instance()
        self.host = data_manager.postgres.host
        self.port = data_manager.postgres.port
        self.database = data_manager.postgres.database
        self.user = data_manager.postgres.user
        self.password = data_manager.postgres.password
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password
        )
        logger.info("Connected to PostgreSQL")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")

    def execute_query(self, query, values=None):
        try:
            with self.connection.cursor() as cursor:  # Автоматическое закрытие курсора
                cursor.execute(query, values or ())
                self.connection.commit()
                return cursor.fetchall() if cursor.description else None
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            self.connection.rollback()
            raise
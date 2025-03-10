from config.logger import logger
from config.data_manager import DataManager
import psycopg2

class Migration:
    def __init__(self):
        data_manager = DataManager.get_instance()
        self.host = data_manager.postgres.host
        self.port = data_manager.postgres.port
        self.database = data_manager.postgres.database
        self.user = data_manager.postgres.user
        self.password = data_manager.postgres.password

    def __connect_to_postgres(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname="postgres"
            )
            self.connection.autocommit = True
        except Exception as ex:
            logger.error(f"Connection error: {ex}")
            raise

    def __create_database(self):
        try:
            with self.connection.cursor() as cursor:  # Используем контекстный менеджер
                cursor.execute(f"CREATE DATABASE {self.database}")
            logger.info(f"Database '{self.database}' created")
        except Exception as ex:
            logger.warning(f"Database already exists: {ex}")

    def __connect_to_indexing_api_database(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password
            )
        except Exception as ex:
            logger.error(f"Connection error: {ex}")
            raise

    def __create_table(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'api_call_results'"
                    ")"
                )
                exists = cursor.fetchone()[0]
                if not exists:
                    # Исправлен тип данных для datetime
                    cursor.execute(
                        "CREATE TABLE api_call_results ("
                        "id SERIAL PRIMARY KEY, "
                        "url VARCHAR(255), "
                        "status TEXT, "
                        "datetime TIMESTAMP"
                        ")"
                    )
                    logger.info("Table 'api_call_results' created")
                else:
                    logger.info("Table already exists")
        except Exception as ex:
            logger.error(f"Table creation error: {ex}")
            raise

    def run_migration(self):
        try:
            self.__connect_to_postgres()
            self.__create_database()
            self.__connect_to_indexing_api_database()
            self.__create_table()
        finally:
            if self.connection:
                self.connection.close()
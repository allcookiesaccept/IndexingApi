import psycopg2
from config.datamanager import DataManager
from config.logger import logger, log_func_calls
import datetime

class Postgres:
    def __init__(self):
        logger.info("Postgres Class Initialization")
        data_manager: DataManager = DataManager.get_instance()
        self.host = data_manager._DataManager__postgres_info.host
        self.port = data_manager._DataManager__postgres_info.port
        self.database = data_manager._DataManager__postgres_info.database
        self.user = data_manager._DataManager__postgres_info.user
        self.password = data_manager._DataManager__postgres_info.password

    def __call__(self):
        logger.info("Postgres Class Start")
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(f"Connected to the {self.database} database")
            self.create_indexing_table()
        except Exception as e:
            logger.error("Error connecting to the PostgreSQL database:", str(e))

    def write_request_result_to_db(self, url, response):

        try:
            cursor = self.connection.cursor()
            current_datetime = datetime.datetime.now()
            query = "INSERT INTO page_data (url, status, date) VALUES (%s, %s, %s)"
            values = (url, response, current_datetime)
            cursor.execute(query, values)
            logger.info("Data written to database")
        except Exception as e:
            logger.error(f"Error writing data to database: {e}")


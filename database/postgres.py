import psycopg2
from config.data_manager import DataManager
from config.logger import logger


class Postgres:
    def __init__(self):
        data_manager: DataManager = DataManager.get_instance()
        self.host = data_manager.instance.postgres.host
        self.port = data_manager.instance.postgres.port
        self.database = data_manager.instance.postgres.database
        self.user = data_manager.instance.postgres.user
        self.password = data_manager.instance.postgres.password

    def __call__(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(f'connection established')
        except Exception as e:
            logger.error(f"Error during connection\n:{e}")

    def execute_query(self, query, values=None):
        send_line = f"{query}|{values}"
        logger.info(f'executing send_line: {send_line})')
        try:
            cursor = self.connection.cursor()
            result = cursor.execute(query, values)
            self.connection.commit()
            cursor.close()
            return result
        except Exception as e:
            logger.error(f"Error during execution query\n:{e}")

import psycopg2
from config.datamanager import DataManager
from config.logger import logger
from database.migration import MigrationManager
class Postgres:
    def __init__(self):
        data_manager: DataManager = DataManager.get_instance()
        self.host = data_manager.postgres.host
        self.port = data_manager.postgres.port
        self.database = data_manager.postgres.database
        self.user = data_manager.postgres.user
        self.password = data_manager.postgres.password
        logger.info(f"{type(self)} Initializated")

    def __call__(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(f"Connected to the {self.database} database")

        except Exception as e:
            logger.error("Error connecting to the PostgreSQL database:\n", str(e))

    def do_migration(self):
        migration = MigrationManager(self)
        migration.create_indexing_table()

    def execute_query(self, query, values=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"{e}")

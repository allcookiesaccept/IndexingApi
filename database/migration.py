from config.logger import logger
from database.postgres import Postgres

class MigrationManager:

    def __init__(self, database):
        self.database = database

    def create_indexing_table(self):
        try:
            cursor = self.database.connection.cursor()
            cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'api_call_results')")
            exists = cursor.fetchone()[0]
            if exists:
                logger.info(f"'api_call_results' already exists in database!")
                cursor.close()
            else:
                schema = "id SERIAL PRIMARY KEY, url VARCHAR(255), status TEXT, datetime VARCHAR(255)"
                sql_query = f"CREATE TABLE api_call_results ({schema});"
                cursor.execute(sql_query)
                self.database.connection.commit()
                logger.info(f"'api_call_results' successfully created!")
        except Exception as e:
            logger.error(f"Error occurs during creating 'api_call_results': {str(e)}")


if __name__ == '__main__':
    database = Postgres()
    database.__call__()
    migration = MigrationManager(database)
    migration.create_indexing_table()
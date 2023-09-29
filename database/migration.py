from config.logger import logger

class MigrationManager:
    def __init__(self, database):
        self.database = database
    def create_indexing_table(self):
        try:
            cursor = self.database.connection.cursor()
            cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'indexing_table')")
            exists = cursor.fetchone()[0]
            if exists:
                logger.info(f"'indexing_table' already exists in database!")
                cursor.close()
            else:
                schema = "url VARCHAR(255), status TEXT, datetime VARCHAR(255)"
                sql_query = f"CREATE TABLE indexing_table ({schema});"
                cursor.execute(sql_query)
                logger.info(f"'indexing_table' successfully created!")
        except Exception as e:
            logger.error(f"Error occurs during creating 'indexing_table': {str(e)}")

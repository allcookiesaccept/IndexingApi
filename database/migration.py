import datetime
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
                print(f"Таблица 'indexing_table' уже существует!")
                cursor.close()
            else:
                schema = "url VARCHAR(255), status TEXT, datetime VARCHAR(255)"
                sql_query = f"CREATE TABLE indexing_table ({schema});"
                cursor.execute(sql_query)
                logger.info(f"Таблица 'indexing_table' с полями 'url', 'status', 'datetime' создана успешно.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {e}")

    def backup_data(self):
        try:
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            backup_file = f"backup_{now}.sql"
            query = f"COPY indexing_table TO '{backup_file}'"
            self.database.execute_query(query)
            print(f"Данные успешно сохранены в файл '{backup_file}'")
        except Exception as e:
            print(f"{e}")


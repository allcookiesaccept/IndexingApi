import datetime
from .postgres import Postgres
class MigrationManager:
    def __init__(self, database_manager):
        self.database_manager = database_manager

    def create_indexing_table(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS indexing_data (
                url VARCHAR(255),
                status TEXT,
                date VARCHAR(255)
            );
            """
            self.database_manager.execute_query(query)
            print("Таблица 'indexing_data' создана успешно!")
        except Exception as e:
            print(f"{e}")

    def backup_data(self):
        try:
            backup_file = f"backup_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.sql"
            query = f"COPY indexing_data TO '{backup_file}'"
            self.database_manager.execute_query(query)
            print(f"Данные успешно сохранены в файл '{backup_file}'")
        except Exception as e:
            print(f"{e}")

    def create_indexing_table(self):

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'page_data')")
            exists = cursor.fetchone()[0]
            if exists:
                print(f"Таблица 'page_data' уже существует!")
                cursor.close()
            schema = "url VARCHAR(255), status TEXT, date VARCHAR(255)"
            sql_query = f"CREATE TABLE indexing_data ({schema});"
            cursor.execute(sql_query)
            logger.info(f"Таблица 'page_data' с полями 'url', 'status', 'date' создана успешно.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {e}")
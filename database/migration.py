from config.logger import logger
from config.data_manager import DataManager
import psycopg2


class Migration:
    def __init__(self):
        data_manager: DataManager = DataManager.get_instance()
        self.host = data_manager.instance.postgres.host
        self.port = data_manager.instance.postgres.port
        self.database = data_manager.instance.postgres.database
        self.user = data_manager.instance.postgres.user
        self.password = data_manager.instance.postgres.password

    def __connect_to_postgres(self):
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database="postgres",
            ) as self.connection:
                self.connection.autocommit = True
        except Exception as ex:
            logger.error(f"Error occurs during __connect_to_postgres: {str(ex)}")

    def __create_database(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE {self.database}")
            self.connection.commit()
            print(f"Database '{self.database}' created successfully!")
        except Exception as ex:
            logger.error(f"Error occurs during __create_database: {str(ex)}")

    def __connect_to_indexing_api_database(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except Exception as ex:
            logger.error(
                f"Error occurs during __connect_to_indexing_api_database: {str(ex)}"
            )

    def __create_table(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'api_call_results')"
            )
            exists = cursor.fetchone()[0]
            if exists:
                logger.info(f"'api_call_results' already exists in database!")
                cursor.close()
            else:
                schema = "id SERIAL PRIMARY KEY, url VARCHAR(255), status TEXT, datetime VARCHAR(255)"
                sql_query = f"CREATE TABLE api_call_results ({schema});"
                cursor.execute(sql_query)
                self.connection.commit()
                logger.info(f"'api_call_results' successfully created!")
        except Exception as ex:
            logger.error(
                f"Error occurs during __connect_to_indexing_api_database: {str(ex)}"
            )

        print("Table created successfully!")

    def run_migration(self):
        self.__connect_to_postgres()
        self.__create_database()
        self.connection.close()

        self.__connect_to_indexing_api_database()
        self.__create_table()
        self.connection.close()


if __name__ == "__main__":
    migration = Migration()
    migration.run_migration()
    print("All tasks done")

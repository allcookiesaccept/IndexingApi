from .models import Postgres
from dotenv import load_dotenv
import os


class DataManager:

    instance = None

    @staticmethod
    def get_instance():
        if DataManager.instance is None:
            DataManager()
        return DataManager.instance

    def __init__(self):
        if DataManager.instance is not None:
            raise Exception("DataManger is a singleton class")
        else:
            load_dotenv()
            self.postgres = self.load_postgres_data()
            DataManager.instance = self

    def load_postgres_data(self) -> Postgres:
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DB")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        return Postgres(host, port, database, user, password)
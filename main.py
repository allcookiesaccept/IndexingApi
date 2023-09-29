from manager import IndexingManager
from database.postgres import Postgres
from config.logger import logger

database = Postgres()


class IndexingAPI:
    KEYS_FOLDER = "json_keys"

    def __init__(self, db=database):
        self.database = db
        self.database()

        logger.info(f"{type(self)} Initializated")

    def __call__(self, urls):
        manager = IndexingManager(IndexingAPI.KEYS_FOLDER, database)
        manager.index_urls(urls)


if __name__ == "__main__":
    with open("urls.csv", "r") as file:
        urls = file.read().splitlines()

    api = IndexingAPI()
    api(urls[:7])

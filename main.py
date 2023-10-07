from workers.manager import IndexingManager
from database.postgres import Postgres
from config.logger import logger



class IndexingAPI:
    KEYS_FOLDER = "json_keys"

    def __init__(self):
        self.database = Postgres()
        self.database()
        logger.info(f"{type(self)} Initializated")

    def __call__(self, urls):
        self.manager = IndexingManager(IndexingAPI.KEYS_FOLDER, self.database)
        self.manager()
        self.manager.index_urls(urls)

if __name__ == "__main__":
    with open("urls.csv", "r") as file:
        urls = file.read().splitlines()
    api = IndexingAPI()
    api(urls[:1])

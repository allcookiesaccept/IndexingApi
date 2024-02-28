from workers.manager import IndexingManager
from database.postgres import Postgres
from config.logger import logger



class IndexingAPI:
    KEYS_FOLDER = "json_keys"

    def __init__(self):
        self.database = Postgres()
        self.database()
        logger.info(f"{type(self)} Initializated")

    def __load_urls(self, filename='urls.csv'):

        with open(filename, "r") as file:
            self.urls = file.read().splitlines()
            file.close()

    def __save_unprocessed_urls(self, ):

        unprocessed_urls = self.__collect_unprocessed_urls()
        with open(f"unprocessed_urls_{self.manager.get_time().strftime('%Y-%m-%dT%H-%M-%S')}.csv", "w") as file:
            for line in unprocessed_urls:
                file.write(f"{line}\n")
            file.close()

    def __collect_unprocessed_urls(self):

        unprocessed_urls = []

        for url in self.urls:
            url_updated = any(item['url'] == url and item["status"]== 'URL_UPDATED' for item in self.manager.pushed_urls)
            if not url_updated:
                    unprocessed_urls.append(url)

        return unprocessed_urls

    def __call__(self):
        self.__load_urls()
        self.manager = IndexingManager(IndexingAPI.KEYS_FOLDER, self.database)
        self.manager()
        self.manager.index_urls(self.urls)

if __name__ == "__main__":
    api = IndexingAPI()
    api()

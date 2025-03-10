from workers.manager import IndexingManager
from database.postgres import Postgres
from config.logger import logger
import os
import sys

class IndexingAPI:
    KEYS_FOLDER = os.path.abspath("json_keys")

    def __init__(self):
        self.database = Postgres()
        self.manager = None
        self.urls = []

    def load_urls(self, filename='urls.csv'):
        try:
            with open(filename, 'r') as f:
                self.urls = [line.strip() for line in f]
        except FileNotFoundError:
            logger.error(f"URL file '{filename}' not found")
            sys.exit(1)

    def save_unprocessed(self):
        if not self.urls:
            return

        unprocessed = []
        for url in self.urls:
            if not any(
                item[0] == url and item[1] == 'URL_UPDATED'
                for item in self.manager.pushed_urls
            ):
                unprocessed.append(url)

        if unprocessed:
            filename = f"unprocessed_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.csv"
            with open(filename, 'w') as f:
                f.write("\n".join(unprocessed))
            logger.info(f"Unprocessed URLs saved to {filename}")

    def run(self):
        self.load_urls()
        self.manager = IndexingManager(self.KEYS_FOLDER, self.database)
        self.manager.start_agents()
        self.manager.index_urls(self.urls)
        self.save_unprocessed()

if __name__ == "__main__":
    api = IndexingAPI()
    api.run()
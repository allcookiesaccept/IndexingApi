from config.logger import logger
import os
from queue import Queue
from agent import IndexingAgent
# from database.postgres import Postgres
import pandas as pd

class IndexingManager:
    def __init__(self, urls, json_keys_folder):
        logger.info("Indexing Manager Initialization")
        self.urls = urls
        self.json_keys_folder = json_keys_folder
        self.agents = []
        # self.database = Postgres()
        # self.database()
        self.__start_agents()
        self.pushed_urls = []

    def __start_agents(self):
        for file in os.listdir(self.json_keys_folder):
            json_key = os.path.join(self.json_keys_folder, file)
            self.agents.append(IndexingAgent(json_key))

    def index_urls(self):

        queue = Queue()

        for agent in self.agents:
            agent.queue = queue
            agent.start()

        for url in self.urls:
            queue.put(url)

        queue.join()

        self.create_final_df()

    def create_final_df(self):
        columns = ['url', 'status']
        for agent in self.agents:
            self.pushed_urls += agent.pushed_urls

        df = pd.DataFrame(self.pushed_urls, columns=columns)
        df.to_excel('index_push_result.xlsx')
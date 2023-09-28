from config.logger import logger
import os
from queue import Queue
from agent import IndexingAgent
import pandas as pd
import datetime

class IndexingManager:
    def __init__(self, json_keys_folder, database):
        self.db = database
        self.json_keys_folder = json_keys_folder
        self.agents = []
        self.__start_agents()
        self.pushed_urls = []
        logger.info("Indexing Manager Initializated")


    def __start_agents(self):
        for file in os.listdir(self.json_keys_folder):
            json_key = os.path.join(self.json_keys_folder, file)
            self.agents.append(IndexingAgent(json_key, self))

    def add_pushed_url(self, url, response):
        self.pushed_urls.append([url, response])

    def index_urls(self, urls):

        queue = Queue()

        for agent in self.agents:
            agent.queue = queue
            agent.start()

        for url in urls:
            queue.put(url)

        queue.join()

        self.create_final_df()
        logger.info("File created")

    def create_final_df(self):

        filename = self.__generate_filename()
        columns = ['url', 'status']
        df = pd.DataFrame(self.pushed_urls, columns=columns)
        df.to_excel(filename)

    def __generate_filename(self) -> str:

        now = datetime.datetime.now()
        date_string = now.strftime("%Y-%m-%d")
        time_string = now.strftime("%H-%M-%S")
        return f'result/index_api_result_{date_string}_{time_string}.xlsx'

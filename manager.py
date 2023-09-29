from config.logger import logger
import os
from queue import Queue
from threading import Lock
from agent import IndexingAgent
import pandas as pd
import datetime

class IndexingManager:
    def __init__(self, json_keys_folder, database):
        self.db = database
        self.json_keys_folder = json_keys_folder
        self.__start_agents()
        self.num_of_agents = len(self.agents)
        self.pushed_urls = []
        self.done_agents = 0
        self.lock = Lock()
        logger.info("Indexing Manager Initializated")

    def __start_agents(self):
        self.agents = []
        for file in os.listdir(self.json_keys_folder):
            json_key = os.path.join(self.json_keys_folder, file)
            self.agents.append(IndexingAgent(json_key, self))


    def add_pushed_url(self, url, response, time):
        self.pushed_urls.append([url, response, time])

    def agent_done(self):
        with self.lock:
            self.done_agents += 1
            if self.done_agents == len(self.agents):
                self.all_agents_done()  # Вызываем функцию, когда все агенты закончили работу

    def all_agents_done(self):
        self.create_final_df()
        logger.info("File created")
        self.send_results_to_database()
        logger.info("Database updated")

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
        self.send_results_to_database()
        logger.info("Database updated")
    def send_results_to_database(self):
        for item in self.pushed_urls:
            url = item[0]
            status = item[1]
            datetime = item[2]
            query = "INSERT INTO indexing_table (url, status, datetime) VALUES (%s, %s, %s)"
            values = (url, status, datetime)
            self.db.execute_query(query, values)

    def create_final_df(self):

        filename = self.__generate_filename()
        columns = ['url', 'status', 'time']
        df = pd.DataFrame(self.pushed_urls, columns=columns)
        df.to_excel(filename)

    def __generate_filename(self) -> str:
        now = datetime.datetime.now()
        formatted_time = now.strftime("%Y-%m-%dT%H-%M-%S")
        return f'result/index_api_result_{formatted_time}.xlsx'

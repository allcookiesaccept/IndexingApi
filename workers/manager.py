from config.logger import logger
import os
from queue import Queue
from threading import Lock
from workers.agent import IndexingAgent
import pandas as pd
import datetime


class IndexingManager:
    SCOPES = ["https://www.googleapis.com/auth/indexing"]
    ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

    def __init__(self, json_keys_folder, database):
        self.database: database.postgres.Postgres = database
        self.json_keys_folder = json_keys_folder
        self.pushed_urls = []
        self.done_agents = 0
        self.lock = Lock()
        logger.info(f"{type(self)} Initializated")

    def __call__(self):
        self.__start_agents()
        self.num_of_agents = len(self.agents)

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
            logger.info(f"class IndexingAgent stopped")
            if self.done_agents == len(self.agents):
                self.all_agents_done()

    def all_agents_done(self):
        self.create_final_df()
        self.send_results_to_database()

    def index_urls(self, urls):
        queue = Queue()
        for agent in self.agents:
            agent.queue = queue
            agent.start()
        for url in urls:
            queue.put(url)
        queue.join()
        self.create_final_df()
        # self.send_results_to_database()

    def send_url_result_to_database(self, url, push_status, time):
        try:
            query = "INSERT INTO api_call_results (url, push_status, time) VALUES (%s, %s, %s)"
            values = (url, push_status, time)
            self.database.execute_query(query, values)
            logger.info(f"{url} updated in database")
        except Exception as e:
            logger.error(f"Error happened: {e}")

    def send_results_to_database(self):
        for item in self.pushed_urls:
            url, status, datetime = item
            if status != "URL_UPDATED":
                continue
            else:
                try:
                    query = "INSERT INTO api_call_results (url, status, datetime) VALUES (%s, %s, %s)"
                    values = (url, status, datetime)
                    self.database.execute_query(query, values)
                except Exception as e:
                    logger.error(f"Error happened: {e}")

        logger.info("Database updated")

    def create_final_df(self):
        filename = self.__generate_filename()
        columns = ["url", "status", "datetime"]
        df = pd.DataFrame(self.pushed_urls, columns=columns)
        df.to_excel(filename)
        logger.info(f"Created {filename}")

    def get_time(self):
        now = datetime.datetime.now()
        return now

    def __generate_filename(self) -> str:
        formatted_time = self.get_time().strftime("%Y-%m-%dT%H-%M-%S")
        return f"result/index_api_result_{formatted_time}.xlsx"

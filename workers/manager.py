from config.logger import logger
import os
from queue import Queue
from threading import Lock, Event
import pandas as pd
import datetime

class IndexingManager:
    SCOPES = ["https://www.googleapis.com/auth/indexing"]
    ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

    def __init__(self, json_keys_folder, database):
        self.database = database
        self.json_keys_folder = json_keys_folder
        self.agents = []
        self.pushed_urls = []
        self.lock = Lock()
        self.shutdown_event = Event()

    def start_agents(self):
        for file in os.listdir(self.json_keys_folder):
            json_key = os.path.join(self.json_keys_folder, file)
            agent = IndexingAgent(json_key, self)
            agent.queue = Queue()
            agent.start()
            self.agents.append(agent)

    def add_pushed_url(self, url, status, timestamp):
        with self.lock:
            self.pushed_urls.append((url, status, timestamp))

    def index_urls(self, urls):

        for i, url in enumerate(urls):
            agent = self.agents[i % len(self.agents)]
            agent.queue.put(url)

        for agent in self.agents:
            agent.queue.put(None)

        for agent in self.agents:
            agent.join()

        self.save_results()

    def save_results(self):

        with self.lock:
            df = pd.DataFrame(
                self.pushed_urls,
                columns=['url', 'status', 'timestamp']
            )
            filename = self.generate_filename()
            df.to_excel(filename, index=False)
            logger.info(f"Report saved to {filename}")

            with self.database as db:
                for item in self.pushed_urls:
                    db.execute_query(
                        "INSERT INTO api_call_results (url, status, datetime) "
                        "VALUES (%s, %s, %s)",
                        (item[0], item[1], item[2])
                    )
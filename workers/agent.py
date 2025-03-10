from config.logger import logger
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import json
from threading import Thread
import datetime

class IndexingAgent(Thread):
    def __init__(self, json_key, manager):
        super().__init__()
        self.manager = manager
        self.http = self.get_credentials(json_key)
        self.queue = None
        self.max_retries = 3

    def get_credentials(self, json_key):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_key, scopes=self.manager.SCOPES
        )
        return credentials.authorize(httplib2.Http())

    def validate_url(self, url):
        try:
            with self.manager.database.connection.cursor() as cursor:
                # Безопасный параметризованный запрос
                cursor.execute(
                    "SELECT status, datetime FROM api_call_results "
                    "WHERE url = %s ORDER BY datetime DESC LIMIT 1",
                    (url,)
                )
                result = cursor.fetchone()
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            return 'send to index'

        if not result:
            return 'send to index'

        status, last_update = result
        if status != 'URL_UPDATED':
            return 'send to index'

        delta = datetime.datetime.now() - last_update
        if delta.days >= 14:
            return 'send to index'
        return f"Already indexed {delta.days} days ago"

    def run(self):
        while True:
            url = self.queue.get()
            if url is None:
                break

            validation = self.validate_url(url)
            if validation == 'send to index':
                try:
                    response = self.index_single_url(url)
                    self.manager.add_pushed_url(url, response, datetime.datetime.now())
                    if "error" in response:
                        logger.warning(f"Error response: {response}")
                        self.queue.put(url)  # Повторная попытка
                    else:
                        self.queue.task_done()
                except Exception as e:
                    logger.error(f"Indexing failed: {e}")
                    self.queue.task_done()
            else:
                self.manager.add_pushed_url(url, validation, datetime.datetime.now())
                self.queue.task_done()

    def index_single_url(self, url):
        payload = json.dumps({"url": url.strip(), "type": "URL_UPDATED"})
        response, content = self.http.request(
            self.manager.ENDPOINT,
            method="POST",
            body=payload
        )
        return json.loads(content.decode())
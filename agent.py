from config.logger import logger
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import json
from threading import Thread


class IndexingAgent(Thread):
    SCOPES = ["https://www.googleapis.com/auth/indexing"]
    ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

    def __debug__message(self, response):

        if "error" in response:
            code = response["error"]["code"]
            status = response["error"]["status"]
            message = response["error"]["message"]
            logger.error(f"{code}\n{status}\n{message}")
        else:
            url = response["urlNotificationMetadata"]["latestUpdate"]["url"]
            request_result = response["urlNotificationMetadata"]["latestUpdate"]["type"]
            logger.info(f"{url}:\t{request_result}")

    def __init__(self, json_key):
        logger.info("Indexing Agent Initialization")
        super().__init__()
        # self.db = database
        self.http = self.get_credentials(json_key)
        self.queue = None
        self.pushed_urls = []
        self.get_limit = False

    def get_credentials(self, json_key):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_key, scopes=IndexingAgent.SCOPES
        )
        return credentials.authorize(httplib2.Http())

    def index_single_url(self, http, url):
        content = {"url": url.strip(), "type": "URL_UPDATED"}
        payload = json.dumps(content)
        response, content = http.request(
            IndexingAgent.ENDPOINT, method="POST", body=payload
        )
        result = json.loads(content.decode())
        self.__debug__message(result)
        return result

    def run(self):

        logger.info("Indexing Agent Run")
        while True:
            url = self.queue.get()
            response = self.index_single_url(self.http, url)

            if "error" in response:
                self.queue.put(url)
                break
            else:
                self.pushed_urls.append([url, response])

            self.queue.task_done()
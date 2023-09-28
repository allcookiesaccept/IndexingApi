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
            print(f"{request_result}:\t{url}")

    def __init__(self, json_key, manager):
        super().__init__()
        self.http = self.get_credentials(json_key)
        self.queue = None
        self.manager = manager
        print("Agent Initializated")

    def get_credentials(self, json_key):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_key, scopes=IndexingAgent.SCOPES
        )
        return credentials.authorize(httplib2.Http())

    def index_single_url(self, url):
        content = {"url": url.strip(), "type": "URL_UPDATED"}
        payload = json.dumps(content)
        response, content = self.http.request(
            IndexingAgent.ENDPOINT, method="POST", body=payload
        )
        result = json.loads(content.decode())
        self.__debug__message(result)
        return result

    def run(self):

        print("Agent Run")
        for url in iter(self.queue.get, None):

            response = self.index_single_url(url)

            if "error" in response:
                self.queue.put(url)
                break
            else:
                self.manager.add_pushed_url(url, response)
            self.queue.task_done()


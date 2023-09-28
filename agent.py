from config.logger import logger
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import json
from threading import Thread
import datetime

class IndexingAgent(Thread):
    SCOPES = ["https://www.googleapis.com/auth/indexing"]
    ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"


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
        return json.loads(content.decode())

    def run(self):

        print("Agent Run")
        for url in iter(self.queue.get, None):

            response = self.index_single_url(url)
            push_result = self.__parse_response(response)
            now = datetime.datetime.now()
            self.manager.add_pushed_url(url, push_result, now)
            if "error" in response:
                break

            self.queue.task_done()

    def __parse_response(self, response):
        try:
            if "error" in response:
                code = response["error"]["code"]
                status = response["error"]["status"]
                message = response["error"]["message"]
                logger.error(f"{code}:\t{status}:\t{message}")
                return f"{code}:\t{status}"
            else:
                url = response["urlNotificationMetadata"]["latestUpdate"]["url"]
                request_result = response["urlNotificationMetadata"]["latestUpdate"]["type"]
                print(f"{request_result}:\t{url}")
                return request_result
        except Exception as e:
            logger.error(e)
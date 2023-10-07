import workers.manager
from config.logger import logger
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import json
from threading import Thread
import datetime

class IndexingAgent(Thread):

    def __init__(self, json_key, manager):
        super().__init__()
        self.manager: workers.manager.IndexingManager = manager

        self.http = self.get_credentials(json_key)
        self.queue = None
        logger.info(f"{type(self)} Initializated")

    def get_credentials(self, json_key):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            json_key, scopes=self.manager.SCOPES
        )
        return credentials.authorize(httplib2.Http())

    def index_single_url(self, url):
        content = {"url": url.strip(), "type": "URL_UPDATED"}
        payload = json.dumps(content)
        response, content = self.http.request(
            self.manager.ENDPOINT, method="POST", body=payload
        )
        return json.loads(content.decode())


    def get_url_info_from_database(self, url):

        try:
            query = f"SELECT status, datetime FROM api_call_results WHERE url = '{url}' ORDER BY id DESC"
            cursor = self.manager.database.connection.cursor()
            cursor.execute(query)
            query_result = cursor.fetchall()
            if len(query_result) > 0:
                url_status = query_result[0][0]
                url_last_update = query_result[0][1]
                print(f"{url}: {url_status}: {url_last_update}")
                return url_status, url_last_update
            else:
                return None, None
        except Exception as e:
            logger.error(f"Error occurs during request for url info: {e}")


    def validate_url(self, url):

        url_status, url_last_update = self.get_url_info_from_database(url)
        if url_status == 'URL_UPDATED':
            url_last_update = url_last_update.split(' ')[0]
            date1 = datetime.datetime.strptime(self.manager.get_time().strftime("%Y-%m-%d"), "%Y-%m-%d")
            date2 = datetime.datetime.strptime(url_last_update, "%Y-%m-%d")
            url_days_passed = (date1 - date2).days
            if url_days_passed <= 14:
                return f'The address was successfully sent less than two weeks ago'
        else:
            return 'send to index'

    def run(self):
        logger.info(f"{type(self)} running")
        for url in iter(self.queue.get, None):
            validation_result = self.validate_url(url)
            print(validation_result)
            if validation_result == 'send to index':
                try:
                    response = self.index_single_url(url)
                    push_result = self.__parse_response(response, url)
                    self.manager.add_pushed_url(url, push_result, self.manager.get_time())
                    if "error" in response:
                        self.queue.put(url)
                        break
                    else:
                        self.queue.task_done()
                except Exception as e:
                    logger.error(e)
            else:
                self.manager.add_pushed_url(url, validation_result, self.manager.get_time())
                self.queue.task_done()
        self.manager.agent_done()

    def __parse_response(self, response, url):

        try:
            if "error" in response:
                code = response["error"]["code"]
                status = response["error"]["status"]
                logger.info(f"{code}:\t{status}:\t{url}")
                request_result = f"{code}:\t{status}"
                return request_result
            else:
                url = response["urlNotificationMetadata"]["latestUpdate"]["url"]
                request_result = response["urlNotificationMetadata"]["latestUpdate"][
                    "type"
                ]
                logger.info(f"{request_result}:\t{url}")
                return request_result
        except Exception as e:
            logger.error(f"During response parsing happened error: {e}")

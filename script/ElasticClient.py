import time
from elasticsearch import Elasticsearch, AuthenticationException, AuthorizationException
from elasticsearch.helpers import bulk, BulkIndexError


class ElasticClient:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.index = self.config.ELASTIC_INDEX
        self.pipeline = self.config.ELASTIC_PIPELINE or None

        self.client = Elasticsearch(
            self.config.ELASTIC_URL,
            api_key=self.config.ELASTIC_API_KEY,
            retry_on_timeout=True,
            max_retries=3,
        )
        self.logger.debug("Elastic client initialised for index: %s", self.index)

    def send(self, data):
        actions = []
        for message in data:
            message = message.rstrip("\n")
            if not message:
                continue

            timestamp_ms = self._extract_timestamp_ms(message)
            doc = {
                "_op_type": "create",
                "_index": self.index,
                "_source": {"message": message},
            }
            if timestamp_ms:
                doc["_source"]["@timestamp"] = self._ms_to_iso(timestamp_ms)
            if self.pipeline:
                doc["pipeline"] = self.pipeline

            actions.append(doc)

        if not actions:
            self.logger.warning("No documents to send to Elastic.")
            return True

        try:
            success, failed = bulk(
                self.client,
                actions,
                raise_on_error=False,
                raise_on_exception=False,
            )
            if failed:
                self.logger.error(
                    "Elastic bulk: %d succeeded, %d failed. First failure: %s",
                    success, len(failed), failed[0],
                )
                return False
            self.logger.info("Elastic bulk indexed %d documents.", success)
            return True
        except BulkIndexError as e:
            self.logger.error("Elastic BulkIndexError: %s", e.errors[0])
            return False
        except (AuthenticationException, AuthorizationException) as e:
            self.logger.error("Elastic auth error: %s", e)
            return False
        except Exception as e:
            self.logger.exception("Unexpected Elastic error: %s", e)
            return False

    def _extract_timestamp_ms(self, message):
        try:
            if message.startswith("CEF"):
                return int(message.split("start=")[1].split(" ")[0])
            elif message.startswith("LEEF"):
                return int(message.split("start=")[1].split("\t")[0])
        except (IndexError, ValueError):
            pass
        return None

    @staticmethod
    def _ms_to_iso(epoch_ms):
        return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(epoch_ms / 1000))

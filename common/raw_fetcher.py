from common.base_fetcher import BaseFetcher
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

class RawDataFetcher(BaseFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)

    def fetch_data(self):
        raise NotImplementedError("Subclasses must implement `fetch_data` method. 🥸")

    def save_data(self, data):
        if data:
            self.data_manager.save_data(data)
        else:
            logger.error("No data to save. 😢❌")

    def execute(self):
        data = self.fetch_data()
        if data:
            self.save_data()
            logger.info("Raw data fetch and save process completed. ")
        else:
            logger.error("No data to fetch. 😢❌")
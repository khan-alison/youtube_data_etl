from common.base_fetcher import BaseFetcher


class RawDataFetcher(BaseFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)
        self.test = 1

    def fetch_data(self):
        response = self.youtube.execute()
        return response

    def save_data(self):
        self.data_manager.save_data(response)

    def execute(self):
        data = self.fetch_data()
        self.save_data()
        print("Raw data fetch and save process completed.")

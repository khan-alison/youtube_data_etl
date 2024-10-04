from common.base_fetcher import BaseFetcher
from helper.logger import LoggerSimple


logger = LoggerSimple.get_logger(__name__)


class YoutubeFetcher(BaseFetcher):
    def __init__(self, youtube, data_manager, endpoint, params, formatter):
        """
        General class to fetch data from YouTube API.
        :param youtube: YouTube API client instance.
        :param data_manager: Object for handling data storage.
        :param endpoint: API endpoint (e.g., videos().list, channels().list).
        :param params: Parameters for the API request.
        :param formatter: Function for formatting the data.
        """
        super().__init__(youtube, data_manager)
        self.endpoint = endpoint
        self.params = params
        self.formatter = formatter

    def fetch_data(self):
        """
        Calls the YouTube API to fetch data based on the endpoint and params.
        """
        logger.info("🌜Start fetching data from YouTube....")
        try:
            request = self.endpoint.list(**self.params)
            response = request.execute()
            return response
        except Exception as e:
            logger.error(f'Error fetching data: {str(e)}')
            return None

    def format_data(self, data):
        """
        Formats the data returned by the YouTube API into a DataFrame.
        """
        if not data or 'items' not in data:
            logger.error("No data to format. ❌")
            return pd.DataFrame()
        return self.formatter(data)

    def save_data(self, data):
        """
        Save the formatted data using the data manager.
        """
        formatted_data = self.format_data(data)
        print(f"formatted_data {formatted_data}")
        if formatted_data.empty:
            logger.error("No formatted data to save. ❌")
        else:
            self.data_manager.save_data(formatted_data)

    def execute(self):
        """
        Fetches, formats, and saves the data.
        """
        data = self.fetch_data()
        if data:
            self.save_data(data)
        else:
            logger.error("Failed to fetch any data. ❌")

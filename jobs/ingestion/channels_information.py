from common.raw_fetcher import RawDataFetcher
from common.base_manager import BaseCSVManager
import os
import googleapiclient.discovery
from dotenv import load_dotenv
import pandas as pd
import json
from minio import Minio


class ChannelsInformation(RawDataFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)

    def fetch_data(self):
        pass

    def format_data(self, data):
        pass

    def save_data(self):
        pass

    def execute(self):
        pass


if __name__ == "__main__":
    load_dotenv()
    configurations = {
        "file_name":"channels_on_trending"
    }
    data_manager = BaseCSVManager()
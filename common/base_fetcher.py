from abc import ABC, abstractmethod

class BaseFetcher(ABC):
    def __init__(self, youtube, data_manager):
        self.youtube = youtube
        self.data_manager = data_manager
    
    @abstractmethod
    def fetch_data(self):
        raise NotImplementedError
    
    @abstractmethod
    def save_data(self):
        raise NotImplementedError
    
    @abstractmethod
    def execute(self):
        raise NotImplementedError
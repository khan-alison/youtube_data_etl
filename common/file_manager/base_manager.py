from abc import ABC, abstractmethod


class BaseManager(ABC):
    def __init__(self, spark, source_system, database, table, run_date, batch_run_id, bucket_name):
        self.spark = spark
        self.source_system = source_system
        self.database = database
        self.table = table
        self.run_date = run_date
        self.batch_run_id = batch_run_id
        self.bucket_name = bucket_name

    @abstractmethod
    def save_data(self):
        raise NotImplementedError

    @abstractmethod
    def load_data(self):
        raise NotImplementedError
